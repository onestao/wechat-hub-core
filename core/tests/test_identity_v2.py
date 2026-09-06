"""Identity v2 anti-cross-contamination regression tests (taskbook B10).

Covers the six mandatory scenarios plus the HTTP gate/status/confirm-switch
contract from docs/IDENTITY_V2_CONTRACT.md:

1. X logs in wxid_A -> sync -> every message owned by identity A
2. X observes wxid_B -> mismatch: sync blocked, send blocked
3. confirm-switch -> new messages owned by B, A history untouched
4. a second instance logging in wxid_A resolves the SAME identity uuid
5. legacy unresolved backfill never guesses a wechat_user_id
6. migration re-runs are idempotent
"""

from __future__ import annotations

import base64
import json
import os
import shutil
import sqlite3
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from unittest.mock import patch

CORE_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CORE_ROOT))

_TEST_GUI_LEASE_DIR = tempfile.TemporaryDirectory()
os.environ.setdefault("WECHAT_GUI_LEASE_DIR", _TEST_GUI_LEASE_DIR.name)

from core import identity as identity_v2  # noqa: E402
from core.account_worker import AccountWorker  # noqa: E402
from core.app import CoreService, create_server  # noqa: E402
from core.identity import IdentityError  # noqa: E402
from core.normalize import import_account  # noqa: E402
from core.registry import AccountRegistry, parse_account  # noqa: E402
from core.sender import AccountSender  # noqa: E402
from core.store import CoreStore  # noqa: E402
from memory.memory_ingest import ingest_chat, init_memory_db  # noqa: E402


WXID_A = "wxid_alpha_1111"
WXID_B = "wxid_bravo_2222"
TEST_AI = "wxid_charlie_3333"


def http_json(base_url: str, method: str, path: str, payload: dict | None = None) -> tuple[int, dict]:
    request = urllib.request.Request(
        f"{base_url}{path}",
        method=method,
        data=json.dumps(payload or {}).encode("utf-8") if method == "POST" else None,
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(request, timeout=10) as response:
            return response.status, json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        return exc.code, json.loads(exc.read().decode("utf-8"))


class IdentityTestBase(unittest.TestCase):
    def setUp(self):
        self.temp_root = CORE_ROOT / ".tmp" / f"identity-{uuid.uuid4().hex}"
        self.temp_root.mkdir(parents=True)
        self.store = CoreStore(self.temp_root / "core.sqlite")

    def tearDown(self):
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def seed_account(self, account_id: str, *, runtime: dict | None = None) -> None:
        self.store.upsert_account(account_id, account_id.title(), state="online", runtime=runtime or {})

    def seed_chat(self, account_id: str, chat_id: str = "chat-1") -> None:
        self.store.upsert_chat(
            {"account_id": account_id, "chat_id": chat_id, "type": "private", "display_name": chat_id.title()}
        )

    def sync_message(self, account_id: str, message_id: str, chat_id: str = "chat-1", created_at: str = "") -> None:
        self.store.upsert_message(
            {
                "account_id": account_id,
                "message_id": message_id,
                "chat_id": chat_id,
                "type": "text",
                "direction": "incoming",
                "created_at": created_at or "2026-09-06T00:00:00Z",
                "author": {"member_id": "peer", "display_name": "Peer", "is_self": False},
                "text": f"body-{message_id}",
            }
        )

    def identity_uuids(self) -> dict[str, dict]:
        with sqlite3.connect(self.temp_root / "core.sqlite") as conn:
            conn.row_factory = sqlite3.Row
            return {
                row["wechat_identity_uuid"]: dict(row)
                for row in conn.execute("SELECT * FROM wechat_identities").fetchall()
            }


class LegacyPassthroughTest(IdentityTestBase):
    def test_unengaged_slot_keeps_legacy_writes(self):
        """A slot that never engaged the identity system keeps working (compat)."""
        self.seed_account("alpha")
        self.seed_chat("alpha")
        self.sync_message("alpha", "m1")
        with sqlite3.connect(self.temp_root / "core.sqlite") as conn:
            conn.row_factory = sqlite3.Row
            chat = dict(conn.execute("SELECT * FROM chats WHERE chat_id='chat-1'").fetchone())
            message = dict(conn.execute("SELECT * FROM messages WHERE message_id='m1'").fetchone())
        self.assertTrue(chat["instance_uuid"])
        self.assertEqual(chat["wechat_identity_uuid"], "")
        self.assertEqual(message["wechat_identity_uuid"], "")


class BindingStateMachineTest(IdentityTestBase):
    """Mandatory scenarios 1-4 and mismatch persistence."""

    def setUp(self):
        super().setUp()
        self.seed_account("alpha")
        self.seed_chat("alpha")

    def test_scenario_1_first_login_binds_and_owns_sync(self):
        result = self.store.observe_login("alpha", WXID_A, verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        self.assertEqual(result["state"], "bound")
        self.assertTrue(result["binding_created"])
        for index in range(10):
            self.sync_message("alpha", f"m{index}", created_at=f"2026-09-06T00:0{index}:00Z")
        identity_uuid = result["wechat_identity_uuid"]
        with sqlite3.connect(self.temp_root / "core.sqlite") as conn:
            owners = {
                row[0] for row in conn.execute("SELECT wechat_identity_uuid FROM messages").fetchall()
            }
        self.assertEqual(owners, {identity_uuid})

    def test_scenario_2_mismatch_blocks_sync_and_send(self):
        self.store.observe_login("alpha", WXID_A, verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        self.sync_message("alpha", "m-a")
        mismatch = self.store.observe_login("alpha", WXID_B, verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        self.assertEqual(mismatch["state"], "mismatch")
        self.assertFalse(mismatch["binding_created"])
        with self.assertRaises(IdentityError) as sync_ctx:
            self.sync_message("alpha", "m-blocked")
        self.assertEqual(sync_ctx.exception.code, "identity_sync_blocked")
        self.assertEqual(sync_ctx.exception.status, 409)
        with self.assertRaises(IdentityError) as send_ctx:
            self.store.identity_send_gate("alpha")
        self.assertEqual(send_ctx.exception.code, "identity_binding_changed")
        self.assertEqual(send_ctx.exception.status, 409)
        self.assertEqual(send_ctx.exception.details["expected_identity"], WXID_A)
        self.assertEqual(send_ctx.exception.details["observed_identity"], WXID_B)
        # nothing was written while blocked
        with sqlite3.connect(self.temp_root / "core.sqlite") as conn:
            count = conn.execute("SELECT COUNT(*) FROM messages WHERE message_id='m-blocked'").fetchone()[0]
        self.assertEqual(count, 0)

    def test_scenario_3_confirm_switch_isolates_history(self):
        bound_a = self.store.observe_login("alpha", WXID_A, verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        for index in range(3):
            self.sync_message("alpha", f"a{index}", created_at=f"2026-09-06T00:0{index}:00Z")
        self.store.observe_login("alpha", WXID_B, verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        switch = self.store.confirm_switch("alpha")
        self.assertTrue(switch["switched"])
        self.assertEqual(switch["bound_wechat_user_id"], WXID_B)
        self.assertNotEqual(switch["wechat_identity_uuid"], bound_a["wechat_identity_uuid"])
        for index in range(2):
            self.sync_message("alpha", f"b{index}", created_at=f"2026-09-06T01:0{index}:00Z")
        with sqlite3.connect(self.temp_root / "core.sqlite") as conn:
            rows = dict(
                conn.execute("SELECT message_id, wechat_identity_uuid FROM messages").fetchall()
            )
            bindings = conn.execute(
                "SELECT wechat_identity_uuid, active FROM instance_identity_bindings ORDER BY bound_at"
            ).fetchall()
        for index in range(3):
            self.assertEqual(rows[f"a{index}"], bound_a["wechat_identity_uuid"])
        for index in range(2):
            self.assertEqual(rows[f"b{index}"], switch["wechat_identity_uuid"])
        self.assertEqual([tuple(row) for row in bindings], [
            (bound_a["wechat_identity_uuid"], 0),
            (switch["wechat_identity_uuid"], 1),
        ])
        state = self.store.binding_state("alpha")
        self.assertEqual(state["state"], "bound")

    def test_scenario_4_second_instance_resolves_same_identity(self):
        self.seed_account("beta")
        first = self.store.observe_login("alpha", WXID_A, verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        second = self.store.observe_login("beta", WXID_A, verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        self.assertEqual(first["wechat_identity_uuid"], second["wechat_identity_uuid"])
        self.assertNotEqual(first["instance_uuid"], second["instance_uuid"])
        self.seed_chat("beta")
        self.sync_message("beta", "from-beta")
        with sqlite3.connect(self.temp_root / "core.sqlite") as conn:
            owners = {
                row[0] for row in conn.execute("SELECT wechat_identity_uuid FROM messages").fetchall()
            }
        self.assertEqual(owners, {first["wechat_identity_uuid"]})

    def test_mismatch_persists_and_recovers_on_matching_relogin(self):
        self.store.observe_login("alpha", WXID_A, verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        self.store.observe_login("alpha", WXID_B, verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        repeated = self.store.observe_login("alpha", WXID_B, verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        self.assertEqual(repeated["state"], "mismatch")
        self.assertFalse(repeated["changed"])
        recovered = self.store.observe_login("alpha", WXID_A, verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        self.assertEqual(recovered["state"], "bound")
        self.assertTrue(recovered["changed"])
        self.sync_message("alpha", "m-after-recovery")
        with sqlite3.connect(self.temp_root / "core.sqlite") as conn:
            owner = conn.execute(
                "SELECT wechat_identity_uuid FROM messages WHERE message_id='m-after-recovery'"
            ).fetchone()[0]
        state = self.store.binding_state("alpha")
        self.assertEqual(owner, state["identity"]["wechat_identity_uuid"])

    def test_confirm_switch_requires_mismatch(self):
        self.store.observe_login("alpha", WXID_A, verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        with self.assertRaises(IdentityError) as ctx:
            self.store.confirm_switch("alpha")
        self.assertEqual(ctx.exception.code, "identity_not_mismatch")

    def test_invalid_wxid_is_rejected(self):
        with self.assertRaises(IdentityError):
            self.store.observe_login("alpha", "Not A wxid!", verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        state = self.store.binding_state("alpha")
        self.assertEqual(state["state"], "unbound")


class LegacyMigrationTest(IdentityTestBase):
    """Mandatory scenarios 5-6 plus evidence priorities."""

    def migrate(self, accounts: list[dict], identity_map: dict | None = None) -> dict:
        return self.store.migrate_identity_v2(accounts, identity_map=identity_map)

    def test_provable_backfill_via_persisted_logged_in_user(self):
        self.seed_account("alpha", runtime={"logged_in_user": WXID_A})
        self.seed_chat("alpha")
        self.sync_message("alpha", "legacy-1")
        report = self.migrate([{"account_id": "alpha", "display_name": "Alpha", "runtime_provider": "agent_wechat"}])
        entry = report["accounts"][0]
        self.assertEqual(entry["status"], "backfilled")
        self.assertEqual(entry["evidence"], "persisted_runtime_logged_in_user")
        identity = self.identity_uuids()[entry["wechat_identity_uuid"]]
        self.assertEqual(identity["wechat_user_id"], WXID_A)
        with sqlite3.connect(self.temp_root / "core.sqlite") as conn:
            owner = conn.execute(
                "SELECT wechat_identity_uuid FROM messages WHERE message_id='legacy-1'"
            ).fetchone()[0]
            active = conn.execute(
                "SELECT COUNT(*) FROM instance_identity_bindings WHERE instance_uuid=? AND active=1",
                (entry["instance_uuid"],),
            ).fetchone()[0]
        self.assertEqual(owner, entry["wechat_identity_uuid"])
        self.assertEqual(active, 1)
        state = self.store.binding_state("alpha")
        self.assertEqual(state["state"], "bound")
        self.sync_message("alpha", "post-migration")
        with sqlite3.connect(self.temp_root / "core.sqlite") as conn:
            owner = conn.execute(
                "SELECT wechat_identity_uuid FROM messages WHERE message_id='post-migration'"
            ).fetchone()[0]
        self.assertEqual(owner, entry["wechat_identity_uuid"])

    def test_provable_backfill_via_wechat_data_dir(self):
        self.seed_account("alpha")
        report = self.migrate(
            [
                {
                    "account_id": "alpha",
                    "display_name": "Alpha",
                    "runtime_provider": "legacy",
                    "data_dir_names": [WXID_A],
                }
            ]
        )
        entry = report["accounts"][0]
        self.assertEqual(entry["status"], "backfilled")
        self.assertEqual(entry["evidence"], "wechat_data_dir")
        self.assertEqual(self.identity_uuids()[entry["wechat_identity_uuid"]]["wechat_user_id"], WXID_A)

    def test_scenario_5_unresolved_backfill_never_guesses(self):
        self.seed_account("alpha")
        self.seed_chat("alpha")
        self.sync_message("alpha", "legacy-orphan")
        report = self.migrate([{"account_id": "alpha", "display_name": "Alpha", "runtime_provider": "legacy"}])
        entry = report["accounts"][0]
        self.assertEqual(entry["status"], "unresolved")
        identity = self.identity_uuids()[entry["wechat_identity_uuid"]]
        self.assertIsNone(identity["wechat_user_id"])
        self.assertEqual(identity["nickname"], identity_v2.UNRESOLVED_NICKNAME)
        with sqlite3.connect(self.temp_root / "core.sqlite") as conn:
            owner = conn.execute(
                "SELECT wechat_identity_uuid FROM messages WHERE message_id='legacy-orphan'"
            ).fetchone()[0]
            bindings = conn.execute("SELECT COUNT(*) FROM instance_identity_bindings").fetchone()[0]
        self.assertEqual(owner, entry["wechat_identity_uuid"])
        self.assertEqual(bindings, 0)
        state = self.store.binding_state("alpha")
        self.assertEqual(state["state"], "unresolved")
        with self.assertRaises(IdentityError) as ctx:
            self.sync_message("alpha", "blocked-unresolved")
        self.assertEqual(ctx.exception.code, "identity_sync_blocked")
        # a display-name lookalike is never accepted as evidence
        report = self.migrate(
            [{"account_id": "alpha", "display_name": "Alpha", "runtime_provider": "legacy", "data_dir_names": ["Alpha"]}],
            identity_map={"alpha": "Alpha Display Name"},
        )
        with sqlite3.connect(self.temp_root / "core.sqlite") as conn:
            backfills = conn.execute("SELECT evidence FROM identity_backfills WHERE account_id='alpha'").fetchall()
        self.assertEqual(len(backfills), 1)
        self.assertEqual(backfills[0][0], "unresolved_no_verified_evidence")

    def test_no_auto_merge_of_two_legacy_accounts(self):
        self.seed_account("alpha")
        self.seed_account("beta")
        self.seed_chat("alpha")
        self.seed_chat("beta")
        report = self.migrate(
            [
                {"account_id": "alpha", "display_name": "Alpha", "runtime_provider": "legacy"},
                {"account_id": "beta", "display_name": "Alpha", "runtime_provider": "legacy"},
            ]
        )
        uuids = {entry["wechat_identity_uuid"] for entry in report["accounts"]}
        self.assertEqual(len(uuids), 2)
        self.assertTrue(all(entry["status"] == "unresolved" for entry in report["accounts"]))
        # a slot without any business history is left unengaged instead of
        # being pre-emptively marked unresolved
        self.seed_account("gamma")
        fresh = self.migrate([{"account_id": "gamma", "display_name": "Gamma", "runtime_provider": "legacy"}])
        self.assertEqual(fresh["accounts"][0]["status"], "skipped_no_history")
        # the same proven wxid on two accounts, however, shares one identity
        self.seed_account("delta")
        shared = self.migrate(
            [
                {"account_id": "alpha", "display_name": "Alpha", "runtime_provider": "legacy", "data_dir_names": [WXID_A]},
                {"account_id": "delta", "display_name": "Delta", "runtime_provider": "legacy", "data_dir_names": [WXID_A]},
            ]
        )
        # alpha was already backfilled unresolved; only delta gets the proven identity
        delta_entry = next(entry for entry in shared["accounts"] if entry["account_id"] == "delta")
        self.assertEqual(delta_entry["status"], "backfilled")
        alpha_entry = next(entry for entry in shared["accounts"] if entry["account_id"] == "alpha")
        self.assertEqual(alpha_entry["status"], "already_backfilled")
        self.assertNotEqual(alpha_entry["wechat_identity_uuid"], delta_entry["wechat_identity_uuid"])

    def test_operator_identity_map_evidence(self):
        self.seed_account("alpha")
        report = self.migrate(
            [{"account_id": "alpha", "display_name": "Alpha", "runtime_provider": "legacy"}],
            identity_map={"alpha": WXID_A},
        )
        entry = report["accounts"][0]
        self.assertEqual(entry["evidence"], "operator_credential_map")
        self.assertEqual(entry["wechat_user_id"], WXID_A)

    def test_scenario_6_migration_is_idempotent(self):
        self.seed_account("alpha", runtime={"logged_in_user": WXID_A})
        self.seed_chat("alpha")
        self.sync_message("alpha", "legacy-1")
        first = self.migrate([{"account_id": "alpha", "display_name": "Alpha", "runtime_provider": "agent_wechat"}])
        snapshot = self.identity_uuids()
        with sqlite3.connect(self.temp_root / "core.sqlite") as conn:
            counts_before = {
                table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in ("wechat_identities", "instance_identity_bindings", "identity_backfills")
            }
            owner_before = conn.execute(
                "SELECT wechat_identity_uuid FROM messages WHERE message_id='legacy-1'"
            ).fetchone()[0]
        second = self.migrate([{"account_id": "alpha", "display_name": "Alpha", "runtime_provider": "agent_wechat"}])
        self.assertEqual(second["accounts"][0]["status"], "already_backfilled")
        self.assertEqual(self.identity_uuids(), snapshot)
        with sqlite3.connect(self.temp_root / "core.sqlite") as conn:
            counts_after = {
                table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                for table in ("wechat_identities", "instance_identity_bindings", "identity_backfills")
            }
            owner_after = conn.execute(
                "SELECT wechat_identity_uuid FROM messages WHERE message_id='legacy-1'"
            ).fetchone()[0]
        self.assertEqual(counts_before, counts_after)
        self.assertEqual(owner_before, owner_after)
        self.assertEqual(first["accounts"][0]["wechat_identity_uuid"], second["accounts"][0]["wechat_identity_uuid"])

    def test_wechat_data_dir_name_helper(self):
        self.assertEqual(identity_v2.wechat_data_dir_name(f"/config/xwechat_files/{WXID_A}/db_storage"), WXID_A)
        self.assertEqual(identity_v2.wechat_data_dir_name(f"C:\\data\\xwechat_files\\{WXID_B}\\db_storage"), WXID_B)
        self.assertEqual(identity_v2.wechat_data_dir_name("/config/xwechat_files/__runtime_unresolved__/db_storage"), "")
        self.assertEqual(identity_v2.wechat_data_dir_name(""), "")
        self.assertEqual(identity_v2.wechat_data_dir_name("/opt/something-else/db_storage"), "")


class IdentityApiTest(unittest.TestCase):
    """HTTP contract: §5.1 account shape, §5.2 conflict, §5.3 confirm-switch, B8 queries."""

    def setUp(self):
        self.temp_root = CORE_ROOT / ".tmp" / f"identity-api-{uuid.uuid4().hex}"
        self.temp_root.mkdir(parents=True)
        self.registry = AccountRegistry(
            [
                parse_account({"account_id": "alpha", "display_name": "Alpha", "runtime_dir": "runtime/accounts/alpha"}, root=self.temp_root),
            ],
            self.temp_root / "accounts.json",
        )
        self.store = CoreStore(self.temp_root / "core.sqlite")
        self.service = CoreService(root=self.temp_root, registry=self.registry, store=self.store)
        self.server = create_server("127.0.0.1", 0, self.service)
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def apply_runtime_status(self, logged_in_user: str) -> None:
        self.service._apply_runtime_status(
            {
                "account_id": "alpha",
                "running": True,
                "container_running": True,
                "agent_server_healthy": True,
                "runtime_provider": "agent_wechat",
                "wechat_login_status": "logged_in",
                "logged_in_user": logged_in_user,
            }
        )

    def bind(self) -> str:
        self.apply_runtime_status(WXID_A)
        state = self.store.binding_state("alpha")
        self.assertEqual(state["state"], "bound")
        return str(state["identity"]["wechat_identity_uuid"])

    def seed_bound_chat(self, chat_id: str = "chat-1") -> None:
        self.store.upsert_chat(
            {"account_id": "alpha", "chat_id": chat_id, "type": "private", "display_name": chat_id.title()}
        )

    def sync_message(self, account_id: str, message_id: str, chat_id: str = "chat-1", created_at: str = "") -> None:
        self.store.upsert_message(
            {
                "account_id": account_id,
                "message_id": message_id,
                "chat_id": chat_id,
                "type": "text",
                "direction": "incoming",
                "created_at": created_at or "2026-09-06T00:00:00Z",
                "author": {"member_id": "peer", "display_name": "Peer", "is_self": False},
                "text": f"body-{message_id}",
            }
        )

    def send_text(self, text: str, *, expected: str = "") -> tuple[int, dict]:
        payload = {"account_id": "alpha", "chat_id": "chat-1", "text": text}
        if expected:
            payload["expected_wechat_identity_uuid"] = expected
        return http_json(self.base_url, "POST", "/v1/send/text", payload)

    def test_account_status_shape_contract(self):
        identity_uuid = self.bind()
        status, body = http_json(self.base_url, "GET", "/v1/accounts/alpha")
        self.assertEqual(status, 200)
        for key in (
            "instance_uuid",
            "account_id",
            "runtime_alias",
            "display_name",
            "resource_key",
            "runtime_provider",
            "logged_in_user",
            "wechat_identity_uuid",
            "identity_binding_state",
            "wechat_profile",
        ):
            self.assertIn(key, body)
        self.assertEqual(body["account_id"], "alpha")
        self.assertEqual(body["runtime_alias"], "alpha")
        self.assertEqual(body["wechat_identity_uuid"], identity_uuid)
        self.assertEqual(body["identity_binding_state"], "bound")
        self.assertEqual(body["logged_in_user"], WXID_A)
        self.assertEqual(body["wechat_profile"]["wechat_user_id"], WXID_A)
        self.assertTrue(body["instance_uuid"])
        # the list endpoint carries the same shape
        status, listing = http_json(self.base_url, "GET", "/v1/accounts")
        self.assertEqual(status, 200)
        self.assertEqual(listing["accounts"][0]["wechat_identity_uuid"], identity_uuid)
        self.assertEqual(listing["accounts"][0]["identity_binding_state"], "bound")

    def test_send_gate_409_on_mismatch_with_contract_shape(self):
        self.bind()
        self.seed_bound_chat()
        self.apply_runtime_status(WXID_B)
        status, body = self.send_text("hello")
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "identity_binding_changed")
        details = body["error"]["details"]
        self.assertEqual(details["expected_identity"], WXID_A)
        self.assertEqual(details["observed_identity"], WXID_B)
        self.assertEqual(details["state"], "mismatch")
        self.assertTrue(details["action_required"])
        self.assertTrue(details["instance_uuid"])
        # mismatch also blocks the chat creation path that sync would use
        with self.assertRaises(IdentityError):
            self.seed_bound_chat("chat-new")

    def test_send_gate_409_on_expected_identity_conflict(self):
        identity_uuid = self.bind()
        self.seed_bound_chat()
        status, body = self.send_text("hello", expected="00000000-0000-4000-8000-000000000000")
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "identity_binding_changed")
        self.assertIn("00000000", body["error"]["details"]["expected_identity"])
        # the correct identity intent is accepted
        status, body = self.send_text("hello", expected=identity_uuid)
        self.assertEqual(status, 202)
        self.assertEqual(body["status"], "accepted")

    def test_send_queues_with_identity_stamp_while_bound(self):
        identity_uuid = self.bind()
        self.seed_bound_chat()
        status, body = self.send_text("hello")
        self.assertEqual(status, 202)
        with sqlite3.connect(self.temp_root / "core.sqlite") as conn:
            conn.row_factory = sqlite3.Row
            row = dict(conn.execute("SELECT * FROM outbox").fetchone())
        self.assertEqual(row["wechat_identity_uuid"], identity_uuid)
        self.assertTrue(row["instance_uuid"])

    def test_confirm_switch_endpoint(self):
        self.bind()
        self.seed_bound_chat()
        self.apply_runtime_status(WXID_B)
        status, body = http_json(self.base_url, "POST", "/v1/runtime/accounts/alpha/confirm-switch", {})
        self.assertEqual(status, 200)
        self.assertTrue(body["switched"])
        self.assertEqual(body["bound_wechat_user_id"], WXID_B)
        status, body = http_json(self.base_url, "GET", "/v1/accounts/alpha")
        self.assertEqual(body["identity_binding_state"], "bound")
        self.assertEqual(body["wechat_profile"]["wechat_user_id"], WXID_B)
        # switching without a mismatch is refused
        status, body = http_json(self.base_url, "POST", "/v1/runtime/accounts/alpha/confirm-switch", {})
        self.assertEqual(status, 409)
        self.assertEqual(body["error"]["code"], "identity_not_mismatch")

    def test_identity_query_endpoints(self):
        identity_uuid = self.bind()
        self.seed_bound_chat()
        self.sync_message("alpha", "m1")
        self.store.upsert_contact("alpha", {"member_id": "peer", "display_name": "Peer", "alias": "peer-id"})
        self.store.upsert_member("alpha", "chat-1", {"member_id": "peer", "display_name": "Peer"})
        self.store.upsert_media(
            {
                "account_id": "alpha",
                "media_id": "media-1",
                "filename": "a.bin",
                "mime_type": "application/octet-stream",
                "local_path": str(self.temp_root / "media-1.bin"),
                "status": "ready",
            }
        )
        headers = {"instance_uuid": "", "wechat_identity_uuid": ""}
        with sqlite3.connect(self.temp_root / "core.sqlite") as conn:
            for table, column, key in (
                ("chats", "chat_id", "chat-1"),
                ("messages", "message_id", "m1"),
                ("contacts", "member_id", "peer"),
            ):
                row = conn.execute(
                    f"SELECT instance_uuid, wechat_identity_uuid FROM {table} WHERE {column}=?",
                    (key,),
                ).fetchone()
                headers["instance_uuid"] = row[0]
                self.assertEqual(row[1], identity_uuid)
        status, chats = http_json(self.base_url, "GET", f"/v1/identities/{identity_uuid}/chats")
        self.assertEqual(status, 200)
        self.assertEqual([chat["chat_id"] for chat in chats["chats"]], ["chat-1"])
        self.assertEqual(chats["identity"]["wechat_user_id"], WXID_A)
        status, messages = http_json(self.base_url, "GET", f"/v1/identities/{identity_uuid}/messages?chat_id=chat-1")
        self.assertEqual(status, 200)
        self.assertEqual([message["message_id"] for message in messages["messages"]], ["m1"])
        self.assertEqual(messages["messages"][0]["wechat_identity_uuid"], identity_uuid)
        status, contacts = http_json(self.base_url, "GET", f"/v1/identities/{identity_uuid}/contacts")
        self.assertEqual(status, 200)
        self.assertEqual([contact["member_id"] for contact in contacts["contacts"]], ["peer"])
        status, members = http_json(self.base_url, "GET", f"/v1/identities/{identity_uuid}/chats/chat-1/members")
        self.assertEqual(status, 200)
        self.assertEqual([member["member_id"] for member in members["members"]], ["peer"])
        status, media = http_json(self.base_url, "GET", f"/v1/identities/{identity_uuid}/media")
        self.assertEqual(status, 200)
        self.assertEqual([item["media_id"] for item in media["media"]], ["media-1"])
        status, _ = http_json(self.base_url, "GET", "/v1/identities/not-a-uuid/chats")
        self.assertEqual(status, 404)

    def test_account_messages_scoped_by_current_binding(self):
        identity_a = self.bind()
        self.seed_bound_chat()
        for index in range(2):
            self.sync_message("alpha", f"a{index}", created_at=f"2026-09-06T00:0{index}:00Z")
        status, body = http_json(self.base_url, "GET", "/v1/accounts/alpha/chats/chat-1/messages")
        self.assertEqual(status, 200)
        self.assertEqual([message["message_id"] for message in body["messages"]], ["a0", "a1"])
        self.assertEqual(body["messages"][0]["wechat_identity_uuid"], identity_a)
        self.apply_runtime_status(WXID_B)
        switch = http_json(self.base_url, "POST", "/v1/runtime/accounts/alpha/confirm-switch", {})[1]
        self.sync_message("alpha", "b0", created_at="2026-09-06T02:00:00Z")
        status, body = http_json(self.base_url, "GET", "/v1/accounts/alpha/chats/chat-1/messages")
        self.assertEqual(status, 200)
        # A's history stays owned by A and is no longer served under B's view
        self.assertEqual([message["message_id"] for message in body["messages"]], ["b0"])
        self.assertEqual(body["messages"][0]["wechat_identity_uuid"], switch["wechat_identity_uuid"])
        status, body = http_json(
            self.base_url, "GET", f"/v1/accounts/alpha/chats/chat-1/messages?wechat_identity_uuid={identity_a}"
        )
        self.assertEqual(status, 200)
        self.assertEqual([message["message_id"] for message in body["messages"]], ["a0", "a1"])


class SenderDispatchGateTest(IdentityTestBase):
    """A queued send must fail closed when the binding changed before dispatch."""

    def setUp(self):
        super().setUp()
        self.registry = AccountRegistry(
            [
                parse_account(
                    {
                        "account_id": "alpha",
                        "display_name": "Alpha",
                        "runtime_dir": str(self.temp_root / "runtime" / "alpha"),
                        "runtime": {"sender_enabled": True, "sender_driver": "agent_wechat"},
                    },
                    root=self.temp_root,
                )
            ],
            self.temp_root / "accounts.json",
        )
        self.sender = AccountSender(self.registry, self.store, root=self.temp_root)

        class FakeDriver:
            def __init__(self):
                self.calls: list[tuple[str, str, str]] = []

            def send(self, kind: str, account_id: str, chat_id: str, request: dict):
                self.calls.append((kind, account_id, chat_id))
                return {"driver": "fake", "confirmed": False}

        self.fake_driver = FakeDriver()
        self.sender._agent_wechat_driver = self.fake_driver

    def test_dispatch_fails_row_after_identity_change(self):
        self.seed_account("alpha")
        self.seed_chat("alpha")
        self.store.observe_login("alpha", WXID_A, verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        receipt = self.store.queue_send("text", {"account_id": "alpha", "chat_id": "chat-1", "text": "hi"})
        self.assertEqual(receipt["status"], "accepted")
        # operator logs a different wxid before the outbox loop dispatches
        self.store.observe_login("alpha", WXID_B, verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        result = self.sender.process_pending()
        self.assertEqual(result["failed"], 1)
        self.assertEqual(self.fake_driver.calls, [])
        with sqlite3.connect(self.temp_root / "core.sqlite") as conn:
            conn.row_factory = sqlite3.Row
            row = dict(conn.execute("SELECT * FROM outbox").fetchone())
        self.assertEqual(row["status"], "failed")
        self.assertEqual(json.loads(row["details_json"])["identity"]["code"], "identity_binding_changed")

    def test_dispatch_proceeds_while_bound(self):
        self.seed_account("alpha")
        self.seed_chat("alpha")
        self.store.observe_login("alpha", WXID_A, verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        self.store.queue_send("text", {"account_id": "alpha", "chat_id": "chat-1", "text": "hi"})
        result = self.sender.process_pending()
        self.assertEqual(result["submitted"], 1)
        self.assertEqual(self.fake_driver.calls, [("text", "alpha", "chat-1")])

    def test_dispatch_fails_row_when_expected_identity_no_longer_bound(self):
        self.seed_account("alpha")
        self.seed_chat("alpha")
        bound = self.store.observe_login("alpha", WXID_A, verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        self.store.queue_send(
            "text",
            {
                "account_id": "alpha",
                "chat_id": "chat-1",
                "text": "hi",
                "expected_wechat_identity_uuid": bound["wechat_identity_uuid"],
            },
        )
        self.store.observe_login("alpha", WXID_B, verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        self.store.confirm_switch("alpha")
        result = self.sender.process_pending()
        self.assertEqual(result["failed"], 1)
        self.assertEqual(self.fake_driver.calls, [])


class SyncImportGateTest(IdentityTestBase):
    """The real normalize/import pipeline honours the Sync Gate (B5)."""

    def setUp(self):
        super().setUp()
        self.account = parse_account(
            {
                "account_id": "alpha",
                "display_name": "Alpha",
                "runtime_dir": str(self.temp_root / "runtime" / "alpha"),
            },
            root=self.temp_root,
        )
        self.raw_db = self.temp_root / "raw-message.db"
        with sqlite3.connect(self.raw_db) as conn:
            conn.executescript(
                """
                CREATE TABLE Msg_stable (
                    local_id INTEGER, server_id INTEGER, local_type INTEGER, sort_seq INTEGER,
                    real_sender_id INTEGER, create_time INTEGER, status INTEGER,
                    upload_status INTEGER, download_status INTEGER, server_seq INTEGER,
                    origin_source INTEGER, source TEXT, message_content TEXT,
                    compress_content TEXT, packed_info_data BLOB, WCDB_CT_message_content INTEGER
                );
                """
            )
            conn.execute(
                "INSERT INTO Msg_stable VALUES (100, 0, 1, 10, 0, 1725091200, 1, 0, 0, 0, 1, '', 'stable hello', '', NULL, 0)"
            )
        self.account.memory_db.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.account.memory_db) as conn:
            init_memory_db(conn)
            ingest_chat(conn, "stable-chat", "Msg_stable", self.raw_db, {}, {})

    def test_import_stamps_bound_identity(self):
        self.store.upsert_account("alpha", "Alpha", state="online")
        self.store.observe_login("alpha", WXID_A, verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        summary = import_account(self.account, self.store)
        self.assertEqual(summary["messages"], 1)
        with sqlite3.connect(self.temp_root / "core.sqlite") as conn:
            conn.row_factory = sqlite3.Row
            message = dict(conn.execute("SELECT * FROM messages").fetchone())
            chat = dict(conn.execute("SELECT * FROM chats").fetchone())
        state = self.store.binding_state("alpha")
        expected = state["identity"]["wechat_identity_uuid"]
        self.assertEqual(message["wechat_identity_uuid"], expected)
        self.assertEqual(chat["wechat_identity_uuid"], expected)
        self.assertTrue(message["instance_uuid"])
        # author normalization preserved (B9)
        author = json.loads(message["author_json"])
        self.assertEqual(author["member_id"], "self")
        self.assertTrue(author["is_self"])

    def test_import_blocked_on_mismatch(self):
        self.store.upsert_account("alpha", "Alpha", state="online")
        self.store.observe_login("alpha", WXID_A, verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        self.store.observe_login("alpha", WXID_B, verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        with self.assertRaises(IdentityError) as ctx:
            import_account(self.account, self.store)
        self.assertEqual(ctx.exception.code, "identity_sync_blocked")
        with sqlite3.connect(self.temp_root / "core.sqlite") as conn:
            count = conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        self.assertEqual(count, 0)

    def test_worker_reports_sync_blocked_as_observable_event(self):
        worker = AccountWorker(AccountRegistry([], self.temp_root / "accounts.json"), self.store)
        account = parse_account(
            {
                "account_id": "alpha",
                "display_name": "Alpha",
                "runtime_dir": str(self.temp_root / "runtime" / "alpha"),
                "runtime": {"logged_in_user": WXID_B},
            },
            root=self.temp_root,
        )
        self.store.upsert_account("alpha", "Alpha", state="online")
        self.store.observe_login("alpha", WXID_A, verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        self.seed_chat_for("alpha")

        def _blocked_import(imported_account, store):
            # the real Sync Gate refuses the write with full mismatch details
            store.upsert_message(
                {
                    "account_id": "alpha",
                    "message_id": "blocked",
                    "chat_id": "chat-1",
                    "type": "text",
                    "direction": "incoming",
                    "created_at": "2026-09-06T00:00:00Z",
                    "author": {"member_id": "peer", "display_name": "Peer", "is_self": False},
                    "text": "blocked",
                }
            )

        with patch("memory.decrypt_sync.refresh_decrypted", return_value={"failed": 0}), patch(
            "memory.media_sync.sync_media", return_value={}
        ), patch("memory.memory_ingest.ingest_memory", return_value={}), patch(
            "memory.sync_repair.repair_memory_indexes", return_value={"ok": True}
        ), patch("core.account_worker.import_account", side_effect=_blocked_import):
            status = worker.run_account(account)
        self.assertEqual(status["identity_binding"]["code"], "identity_sync_blocked")
        self.assertEqual(status["identity_binding"]["expected_identity"], WXID_A)
        self.assertEqual(status["identity_binding"]["observed_identity"], WXID_B)
        page = self.store.poll_events(after="0", limit=100)
        blocked = [event for event in page["events"] if event["event_type"] == "identity.sync_blocked"]
        binding_events = [event for event in page["events"] if event["event_type"] == "identity.binding_changed"]
        self.assertEqual(len(blocked), 1)
        self.assertGreaterEqual(len(binding_events), 1)

    def seed_chat_for(self, account_id: str, chat_id: str = "chat-1") -> None:
        self.store.upsert_chat(
            {"account_id": account_id, "chat_id": chat_id, "type": "private", "display_name": chat_id.title()}
        )


class WorkerObservationTest(IdentityTestBase):
    def test_worker_observes_login_before_import(self):
        worker = AccountWorker(AccountRegistry([], self.temp_root / "accounts.json"), self.store)
        account = parse_account(
            {
                "account_id": "alpha",
                "display_name": "Alpha",
                "runtime_dir": str(self.temp_root / "runtime" / "alpha"),
                "runtime": {"logged_in_user": WXID_A},
            },
            root=self.temp_root,
        )
        self.store.upsert_account("alpha", "Alpha", state="online")
        status = worker.run_account(account)
        # missing staging db fails the cycle, but the login observation must
        # have been recorded before any pipeline work
        state = self.store.binding_state("alpha")
        self.assertEqual(state["state"], "bound")
        self.assertEqual(state["identity"]["wechat_user_id"], WXID_A)
        self.assertIn("error", status)


class IdentityV2CrossModuleIntegrationTests(unittest.TestCase):
    """Step 4 Cross-module fixture ingestion & Foundation Gates F1-F10 verification."""

    def setUp(self):
        self.temp_root = Path(tempfile.mkdtemp(prefix="core-identity-v2-int-"))
        (self.temp_root / "runtime").mkdir(parents=True, exist_ok=True)
        self.registry_file = self.temp_root / "accounts.json"
        self.store = CoreStore(self.temp_root / "core.sqlite")

    def tearDown(self):
        if hasattr(self, "server"):
            self.server.shutdown()
            self.server.server_close()
            self.thread.join(timeout=5)
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def _start_service(self, accounts_fixture: list[dict]):
        self.registry_file.write_text(json.dumps({"version": 2, "accounts": accounts_fixture}), encoding="utf-8")
        from core.registry import load_registry
        self.registry = load_registry(self.registry_file, root=self.temp_root)
        self.service = CoreService(root=self.temp_root, registry=self.registry, store=self.store)
        self.server = create_server("127.0.0.1", 0, self.service)
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def test_step4_runtime_v2_fixture_ingestion_preserves_canonical_keys(self):
        """P0-I1, P0-I3, Gate F1/F2: Runtime v2 fixture ingested into Core preserves instance_uuid and resource_key."""
        fixture = [
            {
                "id": "work",
                "account_id": "work",
                "runtime_alias": "work",
                "instance_uuid": "11111111-2222-4333-8444-555555555555",
                "resource_key": "legacy-work-resource",
                "display_name": "工作微信",
                "runtime_provider": "agent_wechat",
                "logged_in_user": "wxid_work_a",
            }
        ]
        self._start_service(fixture)

        # 1. Verify single account endpoint GET /v1/accounts/work
        status, body = http_json(self.base_url, "GET", "/v1/accounts/work")
        self.assertEqual(status, 200)
        self.assertEqual(body["instance_uuid"], "11111111-2222-4333-8444-555555555555")
        self.assertEqual(body["resource_key"], "legacy-work-resource")
        self.assertEqual(body["runtime_alias"], "work")
        self.assertEqual(body["display_name"], "工作微信")
        self.assertEqual(body["account_id"], "work")
        self.assertEqual(body["logged_in_user"], "wxid_work_a")
        self.assertEqual(body["identity_binding_state"], "bound")
        self.assertEqual(body["wechat_profile"]["wechat_user_id"], "wxid_work_a")

        # 2. Verify account list GET /v1/accounts
        status, listing = http_json(self.base_url, "GET", "/v1/accounts")
        self.assertEqual(status, 200)
        self.assertEqual(len(listing["accounts"]), 1)
        acc = listing["accounts"][0]
        self.assertEqual(acc["instance_uuid"], "11111111-2222-4333-8444-555555555555")
        self.assertEqual(acc["resource_key"], "legacy-work-resource")

        # 3. Verify SQLite runtime_instances has exactly one row with exact keys (NO shadow UUID)
        with sqlite3.connect(self.temp_root / "core.sqlite") as conn:
            rows = conn.execute("SELECT instance_uuid, runtime_alias, resource_key, display_name FROM runtime_instances").fetchall()
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0][0], "11111111-2222-4333-8444-555555555555")
            self.assertEqual(rows[0][1], "work")
            self.assertEqual(rows[0][2], "legacy-work-resource")
            self.assertEqual(rows[0][3], "工作微信")

        # 4. Verify runtime status enrichment does not overwrite Runtime's instance_uuid
        with patch.object(self.service, "_runtime_request", return_value={"accounts": [dict(fixture[0])]}):
            status, rt_accounts = http_json(self.base_url, "GET", "/v1/runtime/accounts")
            self.assertEqual(status, 200)
            rt_acc = rt_accounts["accounts"][0]
            self.assertEqual(rt_acc["instance_uuid"], "11111111-2222-4333-8444-555555555555")
            self.assertEqual(rt_acc["resource_key"], "legacy-work-resource")
            self.assertEqual(rt_acc["identity_binding_state"], "bound")

    def test_gate_f8_alias_rename_fail_closed_and_display_name_mutable(self):
        """Gate F8 (P0-I2): alias rename attempt is fail-closed; display_name update is allowed and safe."""
        fixture = [
            {
                "id": "work",
                "account_id": "work",
                "runtime_alias": "work",
                "instance_uuid": "11111111-2222-4333-8444-555555555555",
                "resource_key": "legacy-work-resource",
                "display_name": "工作微信",
                "runtime_provider": "agent_wechat",
                "logged_in_user": "wxid_work_a",
            }
        ]
        self._start_service(fixture)

        # Attempt to rename runtime_alias must raise alias_rename_deferred (HTTP 400)
        with self.assertRaises(IdentityError) as ctx:
            self.store.ensure_instance(
                "work",
                instance_uuid="11111111-2222-4333-8444-555555555555",
                runtime_alias="company",
            )
        self.assertEqual(ctx.exception.code, "alias_rename_deferred")
        self.assertEqual(ctx.exception.status, 400)
        self.assertIn("is deferred in this release", str(ctx.exception))

        # Updating display_name succeeds freely without altering instance_uuid or resource_key
        updated = self.store.ensure_instance(
            "work",
            instance_uuid="11111111-2222-4333-8444-555555555555",
            display_name="新工作微信",
        )
        self.assertEqual(updated["instance_uuid"], "11111111-2222-4333-8444-555555555555")
        self.assertEqual(updated["resource_key"], "legacy-work-resource")
        self.assertEqual(updated["display_name"], "新工作微信")
        self.assertEqual(updated["runtime_alias"], "work")

    def test_gate_f2_instance_uuid_conflict_fails_closed(self):
        """Gate F2: Registering a conflicting instance_uuid for an existing alias is fail-closed."""
        fixture = [
            {
                "id": "work",
                "account_id": "work",
                "runtime_alias": "work",
                "instance_uuid": "11111111-2222-4333-8444-555555555555",
                "resource_key": "legacy-work-resource",
                "display_name": "工作微信",
                "runtime_provider": "agent_wechat",
                "logged_in_user": "wxid_work_a",
            }
        ]
        self._start_service(fixture)

        with self.assertRaises(IdentityError) as ctx:
            self.store.ensure_instance(
                "work",
                instance_uuid="99999999-8888-4777-a666-555555555555",
                runtime_alias="work",
            )
        self.assertEqual(ctx.exception.code, "instance_uuid_conflict")
        self.assertEqual(ctx.exception.status, 409)

    def test_e2e_real_runtime_registry_to_core_alignment(self):
        """Full end-to-end verification: real Runtime Registry -> Core load_registry -> status alignment."""
        runtime_scripts = Path(__file__).resolve().parents[6] / "work" / "runtime" / "root" / "scripts" / "wechat"
        if str(runtime_scripts) not in sys.path:
            sys.path.insert(0, str(runtime_scripts))
        import wechat_runtime
        import wechat_runtime_control

        paths = wechat_runtime.RuntimePaths(
            registry_file=self.registry_file,
            account_home_root=self.temp_root / "accounts",
            runtime_dir=self.temp_root / "run",
        )
        reg = wechat_runtime.Registry(paths)
        with patch.object(wechat_runtime, "require_root"):
            rt_account = wechat_runtime.register_account(
                reg,
                "work",
                ":1",
                True,
                label="工作微信",
                provider="agent_wechat",
                instance_uuid="11111111-2222-4333-8444-555555555555",
                runtime_alias="work",
                resource_key="legacy-work-resource",
            )

        self.assertEqual(rt_account["instance_uuid"], "11111111-2222-4333-8444-555555555555")
        self.assertEqual(rt_account["resource_key"], "legacy-work-resource")

        # Load into Core
        from core.registry import load_registry
        core_reg = load_registry(self.registry_file, root=self.temp_root)
        cfg = core_reg.require("work")
        self.assertEqual(cfg.instance_uuid, "11111111-2222-4333-8444-555555555555")
        self.assertEqual(cfg.resource_key, "legacy-work-resource")
        self.assertEqual(cfg.runtime_alias, "work")

        service = CoreService(root=self.temp_root, registry=core_reg, store=self.store)
        server = create_server("127.0.0.1", 0, service)
        base = f"http://127.0.0.1:{server.server_port}"
        th = threading.Thread(target=server.serve_forever, daemon=True)
        th.start()
        try:
            status, acc = http_json(base, "GET", "/v1/accounts/work")
            self.assertEqual(status, 200)
            self.assertEqual(acc["instance_uuid"], rt_account["instance_uuid"])
            self.assertEqual(acc["resource_key"], rt_account["resource_key"])
            self.assertEqual(acc["runtime_alias"], rt_account["runtime_alias"])
        finally:
            server.shutdown()
            server.server_close()
            th.join(timeout=5)


if __name__ == "__main__":
    unittest.main()
