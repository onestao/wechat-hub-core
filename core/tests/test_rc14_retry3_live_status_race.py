"""RC.14 Retry3 live-derived status-race and telemetry-gap regressions.

The frozen Retry2 base was proven to fail the first two desired assertions
before the Retry3 production edits:

* two simultaneous same-target writers emitted 2 account.status events;
* a healthy ``logged_in -> unknown/empty -> logged_in`` gap emitted 2 events.

This suite keeps those contracts and adds the full Retry3 engineering gates.
No real send path is exercised.
"""

from __future__ import annotations

import json
import os
import random
import shutil
import tempfile
import threading
import time
import unittest
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import patch

from core import identity as identity_v2
from core.account_worker import AccountWorker
from core.app import CoreService
from core.identity import IdentityError, VERIFIED_SOURCE_AGENT_AUTH
from core.registry import AccountRegistry, parse_runtime_account
from core.runtime_bridge import discover_agent_wechat_source_db, resolve_runtime_account
from core.source_provenance import SourceIdentityError
from core.store import CoreStore, account_status_event_semantic, parse_json


WXID_A = "wxid_7ugft7xlkf5a22_4117"
WXID_B = "wxid_rpfflqttdz4a22_7fcd"
WXID_HISTORICAL = "wxid_yx40oh06ya1322_7b57"
WXID_CONFLICTING = "wxid_conflicting_9999"
RANDOM_SEED = 20260912


class Retry3StressStore(CoreStore):
    """Keep Retry3 concurrency semantics while avoiding durability-bound tests.

    The 20,000-operation engineering gate is about CoreStore's locking,
    transaction ordering, semantic dedup, and identity-state behavior, not
    physical fsync latency.  Every connection still uses the production
    CoreStore code path and WAL database, but disables synchronous disk flushes
    for this disposable local fixture so the stress gate remains practical on
    Windows developer storage.  Production defaults are unchanged.
    """

    def connect(self):
        conn = super().connect()
        conn.execute("PRAGMA synchronous=OFF")
        return conn


class Retry3LiveDerivedRegression(unittest.TestCase):
    def setUp(self):
        # Long randomized runs can outlive the coding-tools process sandbox's
        # redirected system TEMP directory.  Use the workstation's stable
        # LocalAppData temp root rather than the sandbox TEMP value.  This also
        # keeps the SQLite stress fixture on the local system drive instead of
        # the project volume, avoiding hours of unrelated filesystem latency
        # without changing SQLite/CoreStore semantics.
        configured_temp = str(os.environ.get("RC14_RETRY3_TEST_TEMP_ROOT") or "").strip()
        if configured_temp:
            temp_base = Path(configured_temp)
        else:
            # Fall back to the normal interpreter temp directory for ordinary
            # developer/CI runs.  The long local qualification runner sets the
            # explicit stable root above so it is not tied to coding-tools'
            # ephemeral TEMP sandbox.
            temp_base = Path(tempfile.gettempdir()) / "wechat-hub-rc14-retry3-tests"
        temp_base.mkdir(parents=True, exist_ok=True)
        self.root = Path(tempfile.mkdtemp(prefix="rc14-retry3-test-", dir=temp_base))
        self.store = Retry3StressStore(self.root / "core.sqlite")

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def _status_event_count(self, account_id: str | None = None) -> int:
        with self.store.connection() as conn:
            if account_id is None:
                row = conn.execute("SELECT COUNT(*) FROM events WHERE event_type='account.status'").fetchone()
            else:
                row = conn.execute(
                    "SELECT COUNT(*) FROM events WHERE account_id=? AND event_type='account.status'",
                    (account_id,),
                ).fetchone()
        return int(row[0])

    def _status_events(self) -> list[dict]:
        with self.store.connection() as conn:
            rows = conn.execute(
                "SELECT cursor, account_id, payload_json FROM events "
                "WHERE event_type='account.status' ORDER BY cursor"
            ).fetchall()
        return [
            {
                "cursor": int(row["cursor"]),
                "account_id": str(row["account_id"]),
                "payload": parse_json(row["payload_json"], {}),
            }
            for row in rows
        ]

    @staticmethod
    def _logged_in_status(account_id: str, wxid: str, **updates):
        status = {
            "account_id": account_id,
            "running": True,
            "container_running": True,
            "agent_server_healthy": True,
            "runtime_health": "healthy",
            "health_error": "",
            "wechat_login_status": "logged_in",
            "logged_in_user": wxid,
            "username": f"agent_{account_id}",
            "pids": [1234],
            "windows": ["0x1234"],
        }
        status.update(updates)
        return status

    def _make_agent_fixture(self, account_id: str, wxid: str):
        base = self.root / account_id
        home = base / "home"
        current_db = home / "Documents" / "xwechat_files" / wxid / "db_storage"
        historical_db = home / "Documents" / "xwechat_files" / WXID_HISTORICAL / "db_storage"
        current_db.mkdir(parents=True)
        historical_db.mkdir(parents=True)
        now = time.time()
        os.utime(current_db, (now - 1000, now - 1000))
        os.utime(historical_db, (now + 1000, now + 1000))

        status_file = base / "agent-status.json"
        registry_item = {
            "id": account_id,
            "display_name": account_id,
            "runtime_provider": "agent_wechat",
            "home": str(home),
            "resource_key": f"{account_id}-resource",
            "agent_wechat": {"container_name": f"wechat-agent-{account_id}"},
            "username": f"agent_{account_id}",
        }
        registry_path = base / "accounts.json"
        registry_path.parent.mkdir(parents=True, exist_ok=True)
        registry_path.write_text(json.dumps({"accounts": [registry_item]}), encoding="utf-8")
        account = parse_runtime_account(registry_item, root=self.root, registry_path=registry_path)
        account.runtime["runtime_status_file"] = str(status_file)
        registry = AccountRegistry([account], registry_path)
        service = CoreService(root=self.root, registry=registry, store=self.store)

        self.store.ensure_instance(
            account_id,
            instance_uuid=account.instance_uuid,
            runtime_alias=account.runtime_alias,
            resource_key=account.resource_key,
            display_name=account.display_name,
            runtime_provider="agent_wechat",
        )
        self.store.observe_login(account_id, wxid, verified_source=VERIFIED_SOURCE_AGENT_AUTH)
        status = self._logged_in_status(account_id, wxid)
        status_file.write_text(json.dumps(status), encoding="utf-8")
        service._apply_runtime_status(status)
        existing = self.store.account(account_id)
        assert existing is not None
        self.store.upsert_account(
            account_id,
            account.display_name,
            state="online",
            runtime=existing["runtime"],
            sync={"ok": True, "stale": False},
        )
        return {
            "account": account,
            "service": service,
            "status_file": status_file,
            "current_db": current_db,
            "historical_db": historical_db,
            "wxid": wxid,
        }

    def _worker_projection(self, fixture: dict, status: dict) -> None:
        """Exercise Worker resolution/final projection without decrypt/ingest."""
        fixture["status_file"].write_text(json.dumps(status), encoding="utf-8")
        account = fixture["account"]
        existing = self.store.account(account.account_id)
        assert existing is not None
        binding = self.store.binding_state(account.account_id)
        bound_wxid = str((binding.get("identity") or {}).get("wechat_user_id") or "")
        resolved = resolve_runtime_account(
            account,
            bound_wxid=bound_wxid,
            previous_runtime=existing["runtime"],
            binding_state=str(binding.get("state") or ""),
        )
        self.assertEqual(resolved.source_db_dir, fixture["current_db"])
        runtime = resolved.public_runtime()
        runtime["registered"] = True
        state = str(existing.get("state") or "online")
        if not runtime.get("running", True):
            state = "stopped"
        elif runtime.get("agent_server_healthy") is False:
            state = "degraded"
        elif str(runtime.get("wechat_login_status") or "") == "logged_out":
            state = "login_required"
        self.store.upsert_account(
            account.account_id,
            account.display_name,
            state=state,
            runtime=runtime,
            sync=existing["sync"],
        )

    def _assert_no_consecutive_duplicate_semantics(self) -> None:
        last: dict[str, dict] = {}
        duplicates: list[tuple[int, str]] = []
        for event in self._status_events():
            payload = event["payload"] if isinstance(event["payload"], dict) else {}
            account = payload.get("account") if isinstance(payload.get("account"), dict) else {}
            semantic = account_status_event_semantic(account)
            account_id = event["account_id"]
            if account_id in last and last[account_id] == semantic:
                duplicates.append((event["cursor"], account_id))
            last[account_id] = semantic
        self.assertEqual(duplicates, [], f"strict duplicate semantic events: {duplicates[:10]}")

    # R3-T1 -----------------------------------------------------------------
    def test_r3_t1_1000_live_gap_cycles_path_a_and_path_b(self):
        fixtures = [self._make_agent_fixture("f-live-a", WXID_A), self._make_agent_fixture("testB", WXID_B)]
        baseline = self._status_event_count()
        for i in range(2000):
            # The taskbook requires >=1,000 cycles for both the A-like and
            # B-like fixtures.  Alternating 2,000 logical cycles gives each
            # live-derived identity exactly 1,000 gap/recovery cycles while
            # exercising both writer paths on every cycle.
            fixture = fixtures[i % len(fixtures)]
            account_id = fixture["account"].account_id
            wxid = fixture["wxid"]
            verified = self._logged_in_status(account_id, wxid, pids=[2000 + (i % 31)])
            gap = dict(verified)
            gap.update({"wechat_login_status": "unknown", "logged_in_user": ""})

            fixture["service"]._apply_runtime_status(gap)
            during_a = self.store.account(account_id)
            assert during_a is not None
            self.assertEqual(during_a["runtime"].get("wechat_login_status"), "logged_in")
            self.assertEqual(during_a["runtime"].get("logged_in_user"), wxid)
            self.assertTrue(during_a["sync"].get("ok"))

            self._worker_projection(fixture, gap)
            during_b = self.store.account(account_id)
            assert during_b is not None
            self.assertEqual(during_b["runtime"].get("wechat_login_status"), "logged_in")
            self.assertEqual(during_b["runtime"].get("logged_in_user"), wxid)
            self.assertTrue(during_b["sync"].get("ok"))

            fixture["service"]._apply_runtime_status(verified)

        self.assertEqual(self._status_event_count() - baseline, 0)
        for fixture in fixtures:
            final = self.store.account(fixture["account"].account_id)
            assert final is not None
            self.assertEqual(final["runtime"].get("logged_in_user"), fixture["wxid"])
            self.assertTrue(final["sync"].get("ok"))
        self._assert_no_consecutive_duplicate_semantics()

    # R3-T2 -----------------------------------------------------------------
    def test_r3_t2_1000_forced_same_account_races_emit_one_event_each(self):
        account_id = "race-account"
        runtime = {"running": True, "logged_in_user": WXID_B, "wechat_login_status": "logged_in"}
        sync = {"ok": True, "stale": False}
        self.store.upsert_account(account_id, "Race", state="starting", runtime=runtime, sync=sync)
        baseline = self._status_event_count(account_id)

        errors: list[BaseException] = []
        with ThreadPoolExecutor(max_workers=2) as pool:
            for i in range(1000):
                target_state = "online" if i % 2 == 0 else "degraded"
                start = threading.Barrier(3)

                def writer(state=target_state):
                    try:
                        start.wait(timeout=10)
                        self.store.upsert_account(
                            account_id,
                            "Race",
                            state=state,
                            runtime=runtime,
                            sync=sync,
                        )
                    except BaseException as exc:  # pragma: no cover - asserted below
                        errors.append(exc)

                futures = [pool.submit(writer), pool.submit(writer)]
                start.wait(timeout=10)
                for future in futures:
                    future.result(timeout=20)
                self.store.upsert_account(account_id, "Race", state=target_state, runtime=runtime, sync=sync)

        self.assertEqual(errors, [])
        self.assertEqual(self._status_event_count(account_id) - baseline, 1000)
        self._assert_no_consecutive_duplicate_semantics()

    # R3-T3 -----------------------------------------------------------------
    def test_r3_t3_different_accounts_use_distinct_locks_and_do_not_cross_contaminate(self):
        self.assertIsNot(self.store._account_status_lock("account-a"), self.store._account_status_lock("account-b"))
        runtime_a = {"running": True, "logged_in_user": WXID_A, "wechat_login_status": "logged_in"}
        runtime_b = {"running": True, "logged_in_user": WXID_B, "wechat_login_status": "logged_in"}
        sync = {"ok": True}
        self.store.upsert_account("account-a", "A", state="starting", runtime=runtime_a, sync=sync)
        self.store.upsert_account("account-b", "B", state="starting", runtime=runtime_b, sync=sync)

        # Holding A's process-local status guard must not block B at the Python
        # serialization layer.  This is deterministic evidence that Retry3 did
        # not replace the race with an unnecessary global status mutex.
        with self.store.account_status_guard("account-a"):
            with ThreadPoolExecutor(max_workers=1) as probe_pool:
                probe = probe_pool.submit(
                    self.store.upsert_account,
                    "account-b",
                    "B",
                    state="online",
                    runtime=runtime_b,
                    sync=sync,
                )
                probe.result(timeout=2)

        with ThreadPoolExecutor(max_workers=2) as pool:
            for i in range(500):
                state = "online" if i % 2 == 0 else "degraded"
                start = threading.Barrier(3)

                def write_a():
                    start.wait(timeout=10)
                    self.store.upsert_account("account-a", "A", state=state, runtime=runtime_a, sync=sync)

                def write_b():
                    start.wait(timeout=10)
                    self.store.upsert_account("account-b", "B", state=state, runtime=runtime_b, sync=sync)

                futures = [pool.submit(write_a), pool.submit(write_b)]
                start.wait(timeout=10)
                for future in futures:
                    future.result(timeout=20)

        account_a = self.store.account("account-a")
        account_b = self.store.account("account-b")
        assert account_a is not None and account_b is not None
        self.assertEqual(account_a["runtime"].get("logged_in_user"), WXID_A)
        self.assertEqual(account_b["runtime"].get("logged_in_user"), WXID_B)
        self._assert_no_consecutive_duplicate_semantics()

    def test_r3_stale_worker_gap_cannot_mask_explicit_logout(self):
        """Path B must refresh before persistence after a concurrent logout."""
        fixture = self._make_agent_fixture("logout-race", WXID_B)
        service = fixture["service"]
        gap = self._logged_in_status(
            "logout-race",
            WXID_B,
            wechat_login_status="unknown",
            logged_in_user="",
        )
        logged_out = self._logged_in_status(
            "logout-race",
            WXID_B,
            wechat_login_status="logged_out",
            logged_in_user="",
        )

        # Build the stale Worker-side projection first, exactly as a long sync
        # cycle can do before a later Runtime/API logout arrives.
        fixture["status_file"].write_text(json.dumps(gap), encoding="utf-8")
        existing = self.store.account("logout-race")
        assert existing is not None
        binding = self.store.binding_state("logout-race")
        bound_wxid = str((binding.get("identity") or {}).get("wechat_user_id") or "")
        stale_worker_account = resolve_runtime_account(
            fixture["account"],
            bound_wxid=bound_wxid,
            previous_runtime=existing["runtime"],
            binding_state=str(binding.get("state") or ""),
        )
        self.assertEqual(stale_worker_account.runtime.get("wechat_login_status"), "logged_in")

        # An authoritative logout wins, followed by an unknown/empty sample.
        fixture["status_file"].write_text(json.dumps(logged_out), encoding="utf-8")
        service._apply_runtime_status(logged_out)
        after_logout = self.store.account("logout-race")
        assert after_logout is not None
        self.assertEqual(after_logout["state"], "login_required")
        fixture["status_file"].write_text(json.dumps(gap), encoding="utf-8")

        worker = AccountWorker(service.registry, self.store)
        worker._persist_final_account_status(
            stale_worker_account,
            state="online",
            sync_status={"ok": True, "stale": False},
        )
        final = self.store.account("logout-race")
        assert final is not None
        self.assertEqual(final["state"], "login_required")
        self.assertNotEqual(final["runtime"].get("wechat_login_status"), "logged_in")
        self.assertEqual(final["runtime"].get("logged_in_user"), "")

    def test_r3_stale_worker_partial_status_cannot_mask_explicit_health_failure(self):
        """A partial final status read must retain the latest explicit health failure."""
        fixture = self._make_agent_fixture("health-race", WXID_B)
        service = fixture["service"]

        # Capture the Worker's stale healthy projection before a Runtime/API
        # writer observes a real health failure.
        existing = self.store.account("health-race")
        assert existing is not None
        binding = self.store.binding_state("health-race")
        bound_wxid = str((binding.get("identity") or {}).get("wechat_user_id") or "")
        fixture["status_file"].write_text(
            json.dumps(self._logged_in_status("health-race", WXID_B)),
            encoding="utf-8",
        )
        stale_worker_account = resolve_runtime_account(
            fixture["account"],
            bound_wxid=bound_wxid,
            previous_runtime=existing["runtime"],
            binding_state=str(binding.get("state") or ""),
        )
        self.assertTrue(stale_worker_account.runtime.get("agent_server_healthy"))

        unhealthy = self._logged_in_status(
            "health-race",
            WXID_B,
            agent_server_healthy=False,
            runtime_health="unhealthy",
            health_error="probe failed",
        )
        fixture["status_file"].write_text(json.dumps(unhealthy), encoding="utf-8")
        service._apply_runtime_status(unhealthy)
        after_failure = self.store.account("health-race")
        assert after_failure is not None
        self.assertEqual(after_failure["state"], "degraded")
        self.assertIs(after_failure["runtime"].get("agent_server_healthy"), False)

        # Runtime then yields a partial healthy-gap sample that omits health
        # fields entirely.  The stale Worker snapshot must not use its old
        # ``agent_server_healthy=True`` value to overwrite the newer degraded
        # projection.
        partial_gap = {
            "account_id": "health-race",
            "running": True,
            "container_running": True,
            "wechat_login_status": "unknown",
            "logged_in_user": "",
        }
        fixture["status_file"].write_text(json.dumps(partial_gap), encoding="utf-8")
        worker = AccountWorker(service.registry, self.store)
        worker._persist_final_account_status(
            stale_worker_account,
            state="online",
            sync_status={"ok": True, "stale": False},
        )
        final = self.store.account("health-race")
        assert final is not None
        self.assertEqual(final["state"], "degraded")
        self.assertIs(final["runtime"].get("agent_server_healthy"), False)
        self.assertEqual(final["runtime"].get("runtime_health"), "unhealthy")

    # R3-T4 -----------------------------------------------------------------
    def test_r3_t4_explicit_logout_is_not_masked(self):
        fixture = self._make_agent_fixture("logout", WXID_B)
        baseline = self._status_event_count("logout")
        logged_out = self._logged_in_status(
            "logout",
            WXID_B,
            wechat_login_status="logged_out",
            logged_in_user="",
        )
        fixture["service"]._apply_runtime_status(logged_out)
        account = self.store.account("logout")
        assert account is not None
        self.assertEqual(account["state"], "login_required")
        self.assertEqual(account["runtime"].get("wechat_login_status"), "logged_out")
        self.assertEqual(account["runtime"].get("logged_in_user"), "")
        self.assertEqual(self._status_event_count("logout") - baseline, 1)
        fixture["service"]._apply_runtime_status(logged_out)
        self.assertEqual(self._status_event_count("logout") - baseline, 1)

    # R3-T5 -----------------------------------------------------------------
    def test_r3_t5_fresh_conflicting_identity_fails_closed(self):
        fixture = self._make_agent_fixture("conflict", WXID_B)
        existing = self.store.account("conflict")
        assert existing is not None
        conflict = self._logged_in_status("conflict", WXID_CONFLICTING)
        fixture["status_file"].write_text(json.dumps(conflict), encoding="utf-8")
        with self.assertRaises(SourceIdentityError):
            resolve_runtime_account(
                fixture["account"],
                bound_wxid=WXID_B,
                previous_runtime=existing["runtime"],
                binding_state=str(self.store.binding_state("conflict").get("state") or ""),
            )

        # Exercise the real Runtime/API writer as well: it must surface the
        # fresh conflicting identity to Identity-v2 rather than replacing it
        # with the remembered bound user.
        fixture["service"]._apply_runtime_status(conflict)
        projected = self.store.account("conflict")
        assert projected is not None
        self.assertEqual(projected["runtime"].get("logged_in_user"), WXID_CONFLICTING)
        mismatch = self.store.binding_state("conflict")
        self.assertEqual(mismatch["state"], identity_v2.STATE_MISMATCH)
        with self.assertRaises(IdentityError):
            self.store.identity_send_gate("conflict")

    # R3-T6 -----------------------------------------------------------------
    def test_r3_t6_explicit_health_failure_and_recovery_remain_meaningful(self):
        fixture = self._make_agent_fixture("health", WXID_B)
        baseline = self._status_event_count("health")
        unhealthy = self._logged_in_status(
            "health",
            WXID_B,
            agent_server_healthy=False,
            runtime_health="unhealthy",
            health_error="probe failed",
            wechat_login_status="unknown",
            logged_in_user="",
        )
        fixture["service"]._apply_runtime_status(unhealthy)
        account = self.store.account("health")
        assert account is not None
        self.assertEqual(account["state"], "degraded")
        self.assertEqual(account["runtime"].get("logged_in_user"), "")
        self.assertEqual(self._status_event_count("health") - baseline, 1)
        fixture["service"]._apply_runtime_status(unhealthy)
        self.assertEqual(self._status_event_count("health") - baseline, 1)

        recovered = self._logged_in_status("health", WXID_B)
        fixture["service"]._apply_runtime_status(recovered)
        account = self.store.account("health")
        assert account is not None
        self.assertEqual(account["state"], "online")
        self.assertEqual(account["runtime"].get("logged_in_user"), WXID_B)
        self.assertEqual(self._status_event_count("health") - baseline, 2)

        stopped = self._logged_in_status(
            "health",
            WXID_B,
            running=False,
            container_running=False,
            wechat_login_status="unknown",
            logged_in_user="",
        )
        fixture["service"]._apply_runtime_status(stopped)
        account = self.store.account("health")
        assert account is not None
        self.assertEqual(account["state"], "stopped")
        self.assertEqual(account["runtime"].get("logged_in_user"), "")
        self.assertEqual(self._status_event_count("health") - baseline, 3)

    def test_r3_nested_store_transaction_reuses_same_account_lock_safely(self):
        """The per-account RLock must remain safe inside CoreStore.transaction()."""
        runtime = {"running": True, "wechat_login_status": "logged_in", "logged_in_user": WXID_B}
        sync = {"ok": True, "stale": False}
        with self.store.transaction():
            self.store.upsert_account("nested", "Nested", state="starting", runtime=runtime, sync=sync)
            self.store.upsert_account("nested", "Nested", state="online", runtime=runtime, sync=sync)
            self.store.upsert_account("nested", "Nested", state="online", runtime=runtime, sync=sync)
        self.assertEqual(self._status_event_count("nested"), 2)
        self._assert_no_consecutive_duplicate_semantics()

    def test_r3_invalid_user_healthy_gap_preserves_bound_verified_identity(self):
        fixture = self._make_agent_fixture("invalid-gap", WXID_B)
        baseline = self._status_event_count("invalid-gap")
        gap = self._logged_in_status(
            "invalid-gap",
            WXID_B,
            wechat_login_status="unknown",
            logged_in_user="??",
        )
        fixture["service"]._apply_runtime_status(gap)
        account = self.store.account("invalid-gap")
        assert account is not None
        self.assertEqual(account["runtime"].get("wechat_login_status"), "logged_in")
        self.assertEqual(account["runtime"].get("logged_in_user"), WXID_B)
        self.assertEqual(self._status_event_count("invalid-gap") - baseline, 0)

    def test_r3_logged_in_without_valid_user_is_non_authoritative_gap(self):
        """A split/non-atomic Runtime sample must not erase verified identity."""
        fixture = self._make_agent_fixture("partial-positive", WXID_B)
        baseline = self._status_event_count("partial-positive")
        partial = self._logged_in_status(
            "partial-positive",
            WXID_B,
            wechat_login_status="logged_in",
            logged_in_user="",
        )
        fixture["service"]._apply_runtime_status(partial)
        account = self.store.account("partial-positive")
        assert account is not None
        self.assertEqual(account["state"], "online")
        self.assertEqual(account["runtime"].get("wechat_login_status"), "logged_in")
        self.assertEqual(account["runtime"].get("logged_in_user"), WXID_B)
        self.assertEqual(self._status_event_count("partial-positive") - baseline, 0)

    def test_r3_incomplete_logged_in_cannot_resurrect_explicit_logout(self):
        """The word logged_in alone is not a positive identity observation."""
        fixture = self._make_agent_fixture("partial-after-logout", WXID_B)
        service = fixture["service"]
        logged_out = self._logged_in_status(
            "partial-after-logout",
            WXID_B,
            wechat_login_status="logged_out",
            logged_in_user="",
        )
        service._apply_runtime_status(logged_out)
        after_logout = self.store.account("partial-after-logout")
        assert after_logout is not None
        self.assertEqual(after_logout["state"], "login_required")

        incomplete_positive = self._logged_in_status(
            "partial-after-logout",
            WXID_B,
            wechat_login_status="logged_in",
            logged_in_user="",
        )
        service._apply_runtime_status(incomplete_positive)
        final = self.store.account("partial-after-logout")
        assert final is not None
        self.assertEqual(final["state"], "login_required")
        self.assertNotEqual(final["runtime"].get("wechat_login_status"), "logged_in")
        self.assertEqual(final["runtime"].get("logged_in_user"), "")

    def test_r3_mismatch_state_cannot_be_cleared_by_remembered_gap_identity(self):
        """Only a currently bound slot may reuse its previous verified login."""
        fixture = self._make_agent_fixture("mismatch-gap", WXID_B)
        mismatch = self.store.observe_login(
            "mismatch-gap",
            WXID_CONFLICTING,
            verified_source=VERIFIED_SOURCE_AGENT_AUTH,
        )
        self.assertEqual(mismatch["state"], identity_v2.STATE_MISMATCH)
        existing = self.store.account("mismatch-gap")
        assert existing is not None
        binding = self.store.binding_state("mismatch-gap")
        self.assertEqual(binding["state"], identity_v2.STATE_MISMATCH)

        gap = self._logged_in_status(
            "mismatch-gap",
            WXID_B,
            wechat_login_status="unknown",
            logged_in_user="",
        )
        fixture["status_file"].write_text(json.dumps(gap), encoding="utf-8")
        resolved = resolve_runtime_account(
            fixture["account"],
            bound_wxid=WXID_B,
            previous_runtime=existing["runtime"],
            binding_state=str(binding.get("state") or ""),
        )
        self.assertEqual(resolved.source_db_dir, fixture["current_db"])
        self.assertNotEqual(resolved.runtime.get("wechat_login_status"), "logged_in")
        self.assertEqual(resolved.runtime.get("logged_in_user"), "")

        # The Runtime/API writer sees the same non-authoritative gap and must
        # leave the mismatch in place; remembered bound identity is not fresh
        # evidence that the conflicting login has disappeared.
        fixture["service"]._apply_runtime_status(gap)
        after = self.store.binding_state("mismatch-gap")
        self.assertEqual(after["state"], identity_v2.STATE_MISMATCH)

    def test_r3_runtime_gap_projection_cannot_overwrite_concurrent_logout(self):
        """Path A read/merge/write is ordered by the same per-account guard."""
        fixture = self._make_agent_fixture("runtime-logout-race", WXID_B)
        service = fixture["service"]
        gap = self._logged_in_status(
            "runtime-logout-race",
            WXID_B,
            wechat_login_status="unknown",
            logged_in_user="",
            # Force this otherwise-semantic-noop gap down the persistence path
            # so the test can exercise ordering against a concurrent logout.
            # The display-name change is meaningful, but the stale remembered
            # login must still never overwrite the later explicit logout.
            display_name="renamed-during-gap",
        )
        logged_out = self._logged_in_status(
            "runtime-logout-race",
            WXID_B,
            wechat_login_status="logged_out",
            logged_in_user="",
        )

        original_upsert = self.store.upsert_account
        gap_reached_upsert = threading.Event()
        release_gap = threading.Event()

        def ordered_upsert(*args, **kwargs):
            if threading.current_thread().name == "retry3-gap-writer":
                gap_reached_upsert.set()
                if not release_gap.wait(timeout=10):
                    raise TimeoutError("gap writer was not released")
            return original_upsert(*args, **kwargs)

        errors: list[BaseException] = []

        def apply(status):
            try:
                service._apply_runtime_status(status)
            except BaseException as exc:  # pragma: no cover - asserted below
                errors.append(exc)

        with patch.object(self.store, "upsert_account", side_effect=ordered_upsert):
            gap_thread = threading.Thread(target=apply, args=(gap,), name="retry3-gap-writer")
            logout_thread = threading.Thread(target=apply, args=(logged_out,), name="retry3-logout-writer")
            gap_thread.start()
            self.assertTrue(gap_reached_upsert.wait(timeout=10))
            logout_thread.start()
            # With the read/merge/write guard, the logout writer cannot pass
            # the gap writer and then be overwritten by its stale projection.
            time.sleep(0.05)
            self.assertTrue(logout_thread.is_alive())
            release_gap.set()
            gap_thread.join(timeout=10)
            logout_thread.join(timeout=10)

        self.assertFalse(gap_thread.is_alive())
        self.assertFalse(logout_thread.is_alive())
        self.assertEqual(errors, [])
        final = self.store.account("runtime-logout-race")
        assert final is not None
        self.assertEqual(final["state"], "login_required")
        self.assertEqual(final["runtime"].get("wechat_login_status"), "logged_out")
        self.assertEqual(final["runtime"].get("logged_in_user"), "")

    # R3-T7 -----------------------------------------------------------------
    def test_r3_t7_20000_seeded_randomized_interleave(self):
        rng = random.Random(RANDOM_SEED)
        fixtures = {
            "f-live-a": self._make_agent_fixture("f-live-a-random", WXID_A),
            "testB": self._make_agent_fixture("testB-random", WXID_B),
        }
        baseline_status_events = self._status_event_count()
        expected_meaningful_events = 0
        historical_selections = 0
        false_sync_flaps = 0
        wrong_identity_ingest = 0
        explicit_transitions_checked = 0
        source_checks = 0

        with ThreadPoolExecutor(max_workers=4) as pool:
            for i in range(20000):
                fixture = fixtures["f-live-a" if rng.randrange(2) == 0 else "testB"]
                account_id = fixture["account"].account_id
                wxid = fixture["wxid"]
                op = rng.randrange(9)

                if op in {0, 1}:
                    verified = self._logged_in_status(account_id, wxid, pids=[1000 + (i % 97)])
                    fixture["service"]._apply_runtime_status(verified)
                    gap = dict(verified)
                    gap.update({"wechat_login_status": "unknown", "logged_in_user": ""})
                    if op == 0:
                        fixture["service"]._apply_runtime_status(gap)
                    else:
                        self._worker_projection(fixture, gap)
                    current = self.store.account(account_id)
                    assert current is not None
                    if current["sync"].get("ok") is not True:
                        false_sync_flaps += 1
                    if current["runtime"].get("logged_in_user") != wxid:
                        false_sync_flaps += 1
                    source_home = Path(str(fixture["account"].runtime.get("source_home") or ""))
                    discovered = discover_agent_wechat_source_db(
                        source_home,
                        "",
                        account_id=account_id,
                        bound_wxid=wxid,
                    )
                    source_checks += 1
                    if discovered is None or discovered[0] != fixture["current_db"]:
                        historical_selections += 1
                elif op == 2:
                    fixture["service"]._apply_runtime_status(
                        self._logged_in_status(
                            account_id,
                            wxid,
                            pids=[2000 + (i % 101), 3000 + (i % 53)],
                            windows=[f"0x{4000 + i:x}"],
                        )
                    )
                elif op == 3:
                    fixture["service"]._apply_runtime_status(
                        self._logged_in_status(
                            account_id,
                            wxid,
                            wechat_login_status="logged_out",
                            logged_in_user="",
                        )
                    )
                    mid = self.store.account(account_id)
                    assert mid is not None
                    if mid["state"] != "login_required":
                        self.fail(f"seed={RANDOM_SEED} i={i}: explicit logout suppressed")
                    fixture["service"]._apply_runtime_status(self._logged_in_status(account_id, wxid))
                    recovered = self.store.account(account_id)
                    assert recovered is not None
                    self.assertEqual(recovered["state"], "online", f"seed={RANDOM_SEED} i={i}: logout recovery")
                    expected_meaningful_events += 2
                    explicit_transitions_checked += 1
                elif op == 4:
                    fixture["service"]._apply_runtime_status(
                        self._logged_in_status(
                            account_id,
                            wxid,
                            agent_server_healthy=False,
                            runtime_health="unhealthy",
                            wechat_login_status="unknown",
                            logged_in_user="",
                        )
                    )
                    mid = self.store.account(account_id)
                    assert mid is not None
                    if mid["state"] != "degraded":
                        self.fail(f"seed={RANDOM_SEED} i={i}: health failure suppressed")
                    fixture["service"]._apply_runtime_status(self._logged_in_status(account_id, wxid))
                    recovered = self.store.account(account_id)
                    assert recovered is not None
                    self.assertEqual(recovered["state"], "online", f"seed={RANDOM_SEED} i={i}: health recovery")
                    expected_meaningful_events += 2
                    explicit_transitions_checked += 1
                elif op == 5:
                    current = self.store.account(account_id)
                    assert current is not None
                    start = threading.Barrier(3)

                    def same_writer():
                        start.wait(timeout=10)
                        self.store.upsert_account(
                            account_id,
                            current["display_name"],
                            state=current["state"],
                            runtime=current["runtime"],
                            sync=current["sync"],
                        )

                    futures = [pool.submit(same_writer), pool.submit(same_writer)]
                    start.wait(timeout=10)
                    for future in futures:
                        future.result(timeout=20)
                elif op == 6:
                    values = []
                    for item in fixtures.values():
                        current = self.store.account(item["account"].account_id)
                        assert current is not None
                        values.append(current)
                    start = threading.Barrier(3)

                    def write_current(current):
                        start.wait(timeout=10)
                        self.store.upsert_account(
                            current["account_id"],
                            current["display_name"],
                            state=current["state"],
                            runtime=current["runtime"],
                            sync=current["sync"],
                        )

                    futures = [pool.submit(write_current, current) for current in values]
                    start.wait(timeout=10)
                    for future in futures:
                        future.result(timeout=20)
                elif op == 7:
                    self._worker_projection(fixture, self._logged_in_status(account_id, wxid))
                else:
                    existing = self.store.account(account_id)
                    assert existing is not None
                    fixture["status_file"].write_text(
                        json.dumps(self._logged_in_status(account_id, WXID_CONFLICTING)),
                        encoding="utf-8",
                    )
                    try:
                        resolve_runtime_account(
                            fixture["account"],
                            bound_wxid=wxid,
                            previous_runtime=existing["runtime"],
                            binding_state=str(self.store.binding_state(account_id).get("state") or ""),
                        )
                    except SourceIdentityError:
                        pass
                    else:  # pragma: no cover - asserted gate
                        wrong_identity_ingest += 1

        # Classify the retained event stream once instead of issuing thousands
        # of extra SQLite COUNT queries inside the randomized loop.  A healthy
        # unknown/empty identity event is telemetry-gap churn by definition.
        telemetry_gap_churn = 0
        for event in self._status_events():
            payload = event["payload"] if isinstance(event["payload"], dict) else {}
            account = payload.get("account") if isinstance(payload.get("account"), dict) else {}
            runtime = account.get("runtime") if isinstance(account.get("runtime"), dict) else {}
            if (
                account.get("state") == "online"
                and runtime.get("running") is True
                and runtime.get("agent_server_healthy") is not False
                and str(runtime.get("runtime_health") or "").lower() not in {"degraded", "failed", "unhealthy", "error", "offline", "stopped"}
                and str(runtime.get("wechat_login_status") or "") in {"", "unknown"}
                and not str(runtime.get("logged_in_user") or "").strip()
            ):
                telemetry_gap_churn += 1

        self.assertGreater(source_checks, 0, f"seed={RANDOM_SEED}: no source checks executed")
        self.assertEqual(telemetry_gap_churn, 0, f"seed={RANDOM_SEED}")
        self.assertEqual(historical_selections, 0, f"seed={RANDOM_SEED}")
        self.assertEqual(false_sync_flaps, 0, f"seed={RANDOM_SEED}")
        self.assertEqual(wrong_identity_ingest, 0, f"seed={RANDOM_SEED}")
        self.assertGreater(explicit_transitions_checked, 0)
        self.assertEqual(
            self._status_event_count() - baseline_status_events,
            expected_meaningful_events,
            f"seed={RANDOM_SEED}: meaningful transition event accounting",
        )
        self._assert_no_consecutive_duplicate_semantics()


if __name__ == "__main__":
    unittest.main()
