"""RC.14 Governed Consumer Bootstrap & Re-Bootstrap tests.

Governing documents:
  - docs/RC14_NEW_CONSUMER_BOOTSTRAP_POLICY.md
  - docs/RC14_OPTIONAL_EFB_PRODUCTION_LIVE_QUALIFICATION_RETRY1_RESULT.md

Covers:
  1. Unregistered consumer rejected (fail-closed)
  2. Unbootstrapped poll rejected for new optional consumer
  3. Poll below server-assigned initial_cursor rejected
  4. at_head bootstrap assigns current stream head cursor
  5. bootstrap is atomic and idempotent (never moves cursor forward on repeat)
  6. bounded_window bootstrap with events count or since timestamp
  7. bounded_window requires operator_token
  8. explicit_historical_replay rejected without maintenance window
  9. Checkpoint before bootstrap rejected
 10. First checkpoint below initial_cursor rejected
 11. Legacy consumers (wechat-console, wechat-agent) remain backward compatible
 12. Re-bootstrap remediation for failed EFB qualification checkpoint 13181
 13. Re-bootstrap rejected for currently required consumers (wechat-console)
 14. Full audit trail recorded permanently in consumer_bootstrap_audit
 15. HTTP API endpoints for bootstrap, rebootstrap, and provenance
"""

from __future__ import annotations

import json
import shutil
import tempfile
import threading
import urllib.error
import urllib.request
import uuid
from pathlib import Path
import unittest

from core.app import CoreService, create_server
from core.registry import AccountRegistry, parse_account
from core.store import CoreStore, StoreError

EFB_ID = "efb-linux-wechat:wechat.linux"
CONSOLE_ID = "wechat-console"
AGENT_ID = "wechat-agent"


class TestGovernedBootstrapStore(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "test_core.sqlite"
        self.store = CoreStore(self.db_path)
        self.store.upsert_account("acc-1", "Test Account", state="online")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _seed_messages(self, count: int) -> list[dict]:
        events = []
        for i in range(count):
            ev = self.store.record_identity_event(
                "acc-1",
                "message.created",
                {"message": {"message_id": f"msg-{i}", "chat_id": "c-1", "text": f"hello-{i}"}},
            )
            events.append(ev)
        return events

    def test_unregistered_consumer_rejected_on_bootstrap(self) -> None:
        with self.assertRaises(StoreError) as ctx:
            self.store.bootstrap_consumer("random-rogue-consumer", mode="at_head")
        self.assertEqual(ctx.exception.code, "consumer_not_registered")
        self.assertEqual(ctx.exception.status, 400)

    def test_unregistered_consumer_rejected_on_poll(self) -> None:
        with self.assertRaises(StoreError) as ctx:
            self.store.poll_events(after="0", limit=10, consumer_id="random-rogue-consumer")
        self.assertEqual(ctx.exception.code, "consumer_not_registered")
        self.assertEqual(ctx.exception.status, 400)

    def test_unbootstrapped_registered_consumer_rejected_on_poll(self) -> None:
        self._seed_messages(5)
        with self.assertRaises(StoreError) as ctx:
            self.store.poll_events(after="0", limit=10, consumer_id=EFB_ID)
        self.assertEqual(ctx.exception.code, "missing_bootstrap_provenance")
        self.assertEqual(ctx.exception.status, 400)

    def test_at_head_bootstrap(self) -> None:
        self._seed_messages(10)
        # stream_head is 11 (1 account.status + 10 message.created)
        res = self.store.bootstrap_consumer(EFB_ID, mode="at_head")
        self.assertTrue(res["ok"])
        self.assertEqual(res["consumer_id"], EFB_ID)
        self.assertEqual(res["initial_cursor"], 11)
        self.assertEqual(res["stream_head_cursor"], 11)
        self.assertEqual(res["mode"], "at_head")
        self.assertFalse(res["idempotent"])

        # Checkpoint row reflects initial cursor
        cp = self.store.get_checkpoint(EFB_ID)
        self.assertIsNotNone(cp)
        self.assertEqual(cp["processed_through_cursor"], 11)

        # Provenance reflects mode and audit entry
        prov = self.store.get_bootstrap_provenance(EFB_ID)
        self.assertIsNotNone(prov)
        self.assertEqual(prov["bootstrap_mode"], "at_head")
        self.assertEqual(prov["initial_cursor"], 11)
        self.assertEqual(len(prov["audit_history"]), 1)
        self.assertEqual(prov["audit_history"][0]["action"], "bootstrap")

    def test_bootstrap_idempotency(self) -> None:
        self._seed_messages(10)
        res1 = self.store.bootstrap_consumer(EFB_ID, mode="at_head")
        self.assertEqual(res1["initial_cursor"], 11)
        self.assertFalse(res1["idempotent"])

        # Stream advances
        self._seed_messages(5)
        # Repeat bootstrap call MUST return recorded provenance and NOT advance initial_cursor
        res2 = self.store.bootstrap_consumer(EFB_ID, mode="at_head")
        self.assertTrue(res2["idempotent"])
        self.assertEqual(res2["initial_cursor"], 11)
        self.assertEqual(res2["processed_through_cursor"], 11)

        # Audit history still has only 1 row
        prov = self.store.get_bootstrap_provenance(EFB_ID)
        self.assertEqual(len(prov["audit_history"]), 1)

    def test_bounded_window_bootstrap(self) -> None:
        self._seed_messages(30)
        # head = 31
        res = self.store.bootstrap_consumer(
            EFB_ID,
            mode="bounded_window",
            window={"events": 10},
            operator_token="QUAL-TOKEN-1",
        )
        self.assertTrue(res["ok"])
        self.assertEqual(res["initial_cursor"], 21)
        self.assertEqual(res["mode"], "bounded_window")

        prov = self.store.get_bootstrap_provenance(EFB_ID)
        self.assertEqual(prov["initial_cursor"], 21)
        self.assertEqual(prov["audit_history"][0]["operator_token"], "QUAL-TOKEN-1")

    def test_bounded_window_requires_operator_token(self) -> None:
        self._seed_messages(10)
        with self.assertRaises(StoreError) as ctx:
            self.store.bootstrap_consumer(EFB_ID, mode="bounded_window", window={"events": 5})
        self.assertEqual(ctx.exception.code, "mode_requires_authorization")

    def test_explicit_historical_replay_refused(self) -> None:
        with self.assertRaises(StoreError) as ctx:
            self.store.bootstrap_consumer(EFB_ID, mode="explicit_historical_replay")
        self.assertEqual(ctx.exception.code, "mode_requires_maintenance_window")

    def test_poll_below_initial_cursor_rejected(self) -> None:
        self._seed_messages(20)
        self.store.bootstrap_consumer(EFB_ID, mode="at_head")
        self._seed_messages(5)

        # Polling below initial cursor (21) is rejected
        with self.assertRaises(StoreError) as ctx:
            self.store.poll_events(after="10", limit=10, consumer_id=EFB_ID)
        self.assertEqual(ctx.exception.code, "poll_below_initial_cursor")

        # Polling at or above initial cursor succeeds
        page = self.store.poll_events(after="21", limit=10, consumer_id=EFB_ID)
        self.assertEqual(len(page["events"]), 5)

    def test_checkpoint_before_bootstrap_rejected(self) -> None:
        self._seed_messages(5)
        with self.assertRaises(StoreError) as ctx:
            self.store.checkpoint_consumer(EFB_ID, 5)
        self.assertEqual(ctx.exception.code, "checkpoint_before_bootstrap")

    def test_checkpoint_below_initial_cursor_rejected(self) -> None:
        self._seed_messages(20)
        self.store.bootstrap_consumer(EFB_ID, mode="at_head")  # initial_cursor = 21

        with self.assertRaises(StoreError) as ctx:
            self.store.checkpoint_consumer(EFB_ID, 15)
        self.assertEqual(ctx.exception.code, "cursor_below_initial_cursor")

        # Checkpoint >= initial_cursor succeeds
        self._seed_messages(5)
        res = self.store.checkpoint_consumer(EFB_ID, 26)
        self.assertTrue(res["ok"])
        self.assertEqual(res["processed_through_cursor"], 26)

    def test_rebootstrap_failed_efb_qualification_checkpoint_13181(self) -> None:
        """Represents remediation for defect R14-EFB-D2:
        EFB has preserved production checkpoint 13181, which cannot advance without
        re-bootstrap or unsafe cursor-0 catchup.
        """
        self._seed_messages(50)
        head = 51

        # Simulate migrated production state with failed qualification checkpoint 13181
        with self.store.connection() as conn:
            conn.execute(
                """
                INSERT INTO consumer_checkpoints (
                    consumer_id, processed_through_cursor, last_event_id, subscription_account_id,
                    updated_at, bootstrap_mode, bootstrap_source, bootstrap_at, initial_cursor, window_spec_json
                ) VALUES (?, 13181, 'ev-13181', '', '2026-09-14T08:00:00Z', 'legacy_unbounded', 'qualification_retry1', '2026-09-14T08:00:00Z', 0, '{}')
                """,
                (EFB_ID,),
            )
            conn.execute(
                """
                INSERT INTO consumer_bootstrap_audit (
                    audit_id, consumer_id, action, previous_cursor, previous_last_event_id,
                    previous_bootstrap_mode, previous_bootstrap_at, new_initial_cursor,
                    new_bootstrap_mode, new_bootstrap_at, window_spec_json, operator_token, reason, quiescence_evidence, created_at
                ) VALUES ('audit-init', ?, 'bootstrap', NULL, '', '', '', 0, 'legacy_unbounded', '2026-09-14T08:00:00Z', '{}', '', 'qualification_retry1', '', '2026-09-14T08:00:00Z')
                """,
                (EFB_ID,),
            )

        # Normal bootstrap must fail because it is already bootstrapped
        with self.assertRaises(StoreError) as ctx:
            self.store.bootstrap_consumer(EFB_ID, mode="at_head")
        self.assertEqual(ctx.exception.code, "already_bootstrapped")

        # Rebootstrap without operator token must fail
        with self.assertRaises(StoreError) as ctx:
            self.store.rebootstrap_consumer(
                EFB_ID,
                mode="bounded_window",
                window={"events": 10},
                operator_token="",
                quiescence_evidence="container stopped",
            )
        self.assertEqual(ctx.exception.code, "rebootstrap_requires_authorization")

        # Rebootstrap without quiescence evidence must fail
        with self.assertRaises(StoreError) as ctx:
            self.store.rebootstrap_consumer(
                EFB_ID,
                mode="bounded_window",
                window={"events": 10},
                operator_token="OP-TOKEN",
                quiescence_evidence="",
            )
        self.assertEqual(ctx.exception.code, "quiescence_evidence_required")

        # Successful governed rebootstrap
        re_res = self.store.rebootstrap_consumer(
            EFB_ID,
            mode="bounded_window",
            window={"events": 10},
            operator_token="RC14-RETRY2-TOKEN",
            quiescence_evidence="EFB container stopped, preserved local cursor 13214, zero in-flight sends",
        )
        self.assertTrue(re_res["ok"])
        self.assertEqual(re_res["consumer_id"], EFB_ID)
        self.assertEqual(re_res["previous_checkpoint"], 13181)
        self.assertEqual(re_res["initial_cursor"], head - 10)  # 41
        self.assertEqual(re_res["processed_through_cursor"], head - 10)

        # Audit history shows both the failed qualification baseline AND the re-bootstrap
        prov = self.store.get_bootstrap_provenance(EFB_ID)
        self.assertIsNotNone(prov)
        self.assertEqual(prov["initial_cursor"], 41)
        self.assertEqual(len(prov["audit_history"]), 2)
        re_audit = prov["audit_history"][1]
        self.assertEqual(re_audit["action"], "rebootstrap")
        self.assertEqual(re_audit["previous_checkpoint"], 13181)
        self.assertEqual(re_audit["operator_token"], "RC14-RETRY2-TOKEN")
        self.assertIn("preserved local cursor 13214", re_audit["quiescence_evidence"])

    def test_rebootstrap_required_consumer_rejected(self) -> None:
        """A currently required consumer (wechat-console) cannot be re-bootstrapped."""
        with self.assertRaises(StoreError) as ctx:
            self.store.rebootstrap_consumer(
                CONSOLE_ID,
                mode="at_head",
                operator_token="TOKEN",
                quiescence_evidence="console stopped",
            )
        self.assertEqual(ctx.exception.code, "cannot_rebootstrap_required_consumer")

    def test_legacy_consumer_backward_compatibility(self) -> None:
        """wechat-console and wechat-agent continue to work without explicit bootstrap."""
        self._seed_messages(5)
        # Checkpointing directly without bootstrap call succeeds
        res = self.store.checkpoint_consumer(CONSOLE_ID, 5)
        self.assertTrue(res["ok"])
        # Polling without prior bootstrap call succeeds
        page = self.store.poll_events(after="0", limit=10, consumer_id=CONSOLE_ID)
        self.assertEqual(len(page["events"]), 6)


class TestGovernedBootstrapHttp(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_root = Path(tempfile.mkdtemp(prefix="core-boot-http-"))
        self.registry = AccountRegistry(
            [parse_account({"account_id": "acc-1", "display_name": "Acc1", "runtime_dir": "runtime/acc-1"}, root=self.temp_root)],
            self.temp_root / "accounts.json",
        )
        self.store = CoreStore(self.temp_root / "core.sqlite")
        self.service = CoreService(root=self.temp_root, registry=self.registry, store=self.store)
        self.server = create_server("127.0.0.1", 0, self.service)
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

        # Seed events
        self.store.upsert_account("acc-1", "Acc1", state="online")
        for i in range(10):
            self.store.record_identity_event("acc-1", "message.created", {"message": {"text": f"m{i}"}})

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def request(self, path: str, method: str = "GET", payload: dict | None = None) -> tuple[int, dict]:
        body = None
        headers = {}
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(self.base_url + path, data=body, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=3) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                return resp.status, data
        except urllib.error.HTTPError as err:
            data = json.loads(err.read().decode("utf-8"))
            return err.code, data

    def test_http_bootstrap_flow(self) -> None:
        # 1. Unbootstrapped poll returns 400
        status, data = self.request(f"/v1/events/poll?consumer_id={EFB_ID}")
        self.assertEqual(status, 400)
        self.assertEqual(data["error"]["code"], "missing_bootstrap_provenance")

        # 2. Bootstrap via POST /v1/consumers/bootstrap
        status, data = self.request(
            "/v1/consumers/bootstrap",
            method="POST",
            payload={"consumer_id": EFB_ID, "mode": "at_head"},
        )
        self.assertEqual(status, 200)
        self.assertTrue(data["ok"])
        head = data["stream_head_cursor"]
        self.assertEqual(data["initial_cursor"], head)

        # 3. GET /v1/consumers/<consumer_id>/bootstrap
        status, prov = self.request(f"/v1/consumers/{EFB_ID}/bootstrap")
        self.assertEqual(status, 200)
        self.assertEqual(prov["initial_cursor"], head)
        self.assertEqual(len(prov["audit_history"]), 1)

        # 4. Checkpoint at initial cursor succeeds
        status, cp = self.request(
            "/v1/events/checkpoint",
            method="POST",
            payload={"consumer_id": EFB_ID, "processed_through_cursor": head},
        )
        self.assertEqual(status, 200)
        self.assertEqual(cp["processed_through_cursor"], head)

        # 5. Rebootstrap via POST /v1/consumers/rebootstrap
        status, re_data = self.request(
            "/v1/consumers/rebootstrap",
            method="POST",
            payload={
                "consumer_id": EFB_ID,
                "mode": "bounded_window",
                "window": {"events": 5},
                "operator_token": "HTTP-RETRY-TOKEN",
                "quiescence_evidence": "HTTP test container stopped",
            },
        )
        self.assertEqual(status, 200)
        self.assertTrue(re_data["ok"])
        self.assertEqual(re_data["previous_checkpoint"], head)
        self.assertEqual(re_data["initial_cursor"], head - 5)


if __name__ == "__main__":
    unittest.main()
