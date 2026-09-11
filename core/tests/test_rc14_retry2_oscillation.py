"""RC.14 Retry2 Deterministic Oscillation & Multi-WXID Regression Test Suite.

Automated gates for FLASH_RC14_CORE_RUNTIME_STATUS_OSCILLATION_REPAIR_RETRY2_ENGINEERING:
- Reproduction of writer divergence (Path A vs Path B)
- Reproduction of multi-wxid mtime fallback
- Gate R2-T1: 1,000 alternating-writer cycles -> 0 additional status events
- Gate R2-T2: 1,000 telemetry-only cycles -> 0 additional status events
- Gate R2-T3: Meaningful transitions emit exactly 1 status event; conflicting identity fails closed
- Gate R2-T4: Multi-wxid transient observation selects bound wxid, never historical wxid by mtime
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

CORE_ROOT = Path(__file__).resolve().parents[2]
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from core.account_worker import AccountWorker
from core.app import CoreService
from core import identity as identity_v2
from core.identity import IdentityError, VERIFIED_SOURCE_AGENT_AUTH
from core.registry import AccountConfig, AccountRegistry, parse_account, parse_runtime_account
from core.runtime_bridge import (
    canonical_runtime_projection,
    discover_agent_wechat_source_db,
    discover_source_db,
    resolve_runtime_account,
)
from core.source_provenance import SourceIdentityError, valid_wxid
from core.store import CoreStore, account_status_event_semantic

WXID_CURRENT = "wxid_rpfflqttdz4a22_7fcd"
WXID_HISTORICAL = "wxid_yx40oh06ya1322_7b57"
WXID_CONFLICTING = "wxid_conflicting_9999"


class TestRC14Retry2Oscillation(unittest.TestCase):
    def setUp(self):
        self.test_dir = Path(tempfile.mkdtemp(prefix="rc14-retry2-test-"))
        self.accounts_dir = self.test_dir / "accounts"
        self.accounts_dir.mkdir(parents=True)
        self.home = self.test_dir / "home"
        self.home.mkdir(parents=True)
        self.status_file = self.test_dir / "agent-status.json"

        # Current and historical db_storage directories
        self.current_db = self.home / "Documents" / "xwechat_files" / WXID_CURRENT / "db_storage"
        self.historical_db = self.home / "Documents" / "xwechat_files" / WXID_HISTORICAL / "db_storage"
        self.current_db.mkdir(parents=True)
        self.historical_db.mkdir(parents=True)

        now = time.time()
        # Historical directory has NEWER mtime to verify no unsafe mtime fallback
        os.utime(self.current_db, (now - 500, now - 500))
        os.utime(self.historical_db, (now + 500, now + 500))

        self.db_path = self.test_dir / "core.sqlite"
        self.store = CoreStore(self.db_path)

        self._write_status(
            running=True,
            logged_in_user=WXID_CURRENT,
            username="agent_testb",
            pids=[1234],
            windows=["win-1"],
        )

        self.registry_item = {
            "id": "testb",
            "display_name": "TestB",
            "runtime_provider": "agent_wechat",
            "home": str(self.home),
            "agent_wechat": {
                "container_name": "wechat-agent-testb",
            },
            "username": "agent_testb",
        }
        self.registry_path = self.test_dir / "accounts.json"
        self.registry_path.write_text(json.dumps({"accounts": [self.registry_item]}), encoding="utf-8")

        self.account_config = parse_runtime_account(self.registry_item, root=self.test_dir, registry_path=self.registry_path)
        # Point keys file and status file
        self.account_config.runtime["runtime_status_file"] = str(self.status_file)
        self.account_config.keys_file.parent.mkdir(parents=True, exist_ok=True)
        self.account_config.keys_file.write_text("dummy-keys", encoding="utf-8")

        self.registry = AccountRegistry([self.account_config], self.registry_path)
        self.service = CoreService(root=self.test_dir, registry=self.registry, store=self.store)
        self.worker = AccountWorker(self.registry, self.store)

    def tearDown(self):
        shutil.rmtree(self.test_dir, ignore_errors=True)

    def _write_status(self, **kwargs):
        data = {
            "account_id": "testb",
            "running": True,
            "container_running": True,
            "agent_server_healthy": True,
            "runtime_health": "healthy",
            "health_error": "",
            "wechat_login_status": "logged_in",
            "logged_in_user": WXID_CURRENT,
            "username": "agent_testb",
            "uid": 1000,
            "pids": [1234],
            "windows": ["win-1"],
            "window_error": None,
            "autostart": True,
        }
        data.update(kwargs)
        self.status_file.write_text(json.dumps(data), encoding="utf-8")
        return data

    def _count_status_events(self) -> int:
        with self.store.connection() as conn:
            row = conn.execute("SELECT COUNT(*) FROM events WHERE event_type='account.status'").fetchone()
            return int(row[0]) if row else 0

    def _mock_worker_sync_patches(self):
        return (
            patch("memory.decrypt_sync.refresh_decrypted", return_value={"failed": [], "missing_key": [], "updated": [], "skipped": []}),
            patch("memory.media_sync.sync_media", return_value={}),
            patch("memory.memory_ingest.ingest_memory", return_value={"chats": 0, "messages": 0, "changed_rows": 0}),
            patch("memory.sync_repair.repair_memory_indexes", return_value={"ok": True}),
            patch("core.account_worker.import_account", return_value={"chats": 0, "messages": 0, "message_changes": 0}),
            patch("core.key_extract.extract_account_keys", return_value={"returncode": 0}),
        )

    # --------------------------------------------------------------------------
    # Reproduction & Baseline Tests
    # --------------------------------------------------------------------------

    def test_reproduce_multi_wxid_legacy_mtime_fallback(self):
        """Demonstrate that unconstrained legacy discovery picks historical wxid by mtime when logged_in_user is empty."""
        legacy_pick = discover_source_db(self.home)
        self.assertIsNotNone(legacy_pick)
        assert legacy_pick is not None
        # Proves that legacy discovery chooses the historical directory because of newer mtime
        self.assertEqual(legacy_pick[0], self.historical_db)

    def test_reproduce_writer_ping_pong_resolved(self):
        """Verify that canonical_runtime_projection eliminates writer ping-pong."""
        status = self._write_status()
        self.service._apply_runtime_status(status)
        acct_after_apply = self.store.account("testb")
        self.assertIsNotNone(acct_after_apply)
        semantic_apply = account_status_event_semantic(acct_after_apply)

        # Run worker cycle
        patches = self._mock_worker_sync_patches()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            self.worker.run_account(self.account_config)

        acct_after_worker = self.store.account("testb")
        self.assertIsNotNone(acct_after_worker)
        semantic_worker = account_status_event_semantic(acct_after_worker)

        # Both must have identical username and semantic fields
        self.assertEqual(semantic_apply["runtime"].get("username"), "agent_testb")
        self.assertEqual(semantic_worker["runtime"].get("username"), "agent_testb")
        self.assertEqual(semantic_apply["runtime"], semantic_worker["runtime"])

    # --------------------------------------------------------------------------
    # Gate R2-T1: 1,000 Alternating-Writer Cycles -> 0 Additional Status Events
    # --------------------------------------------------------------------------

    def test_r2_t1_alternating_writer_cycles(self):
        """Verify 1,000 alternating cycles between Path A and Path B produce exactly 0 redundant events."""
        status = self._write_status()
        # Seed initial state
        self.service._apply_runtime_status(status)
        patches = self._mock_worker_sync_patches()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            self.worker.run_account(self.account_config)

        # Record baseline event count (typically 1 for initial creation)
        baseline_events = self._count_status_events()
        self.assertGreaterEqual(baseline_events, 1)

        # 1,000 alternating cycles
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            for _ in range(1000):
                self.service._apply_runtime_status(status)
                self.worker.run_account(self.account_config)

        final_events = self._count_status_events()
        additional_events = final_events - baseline_events
        self.assertEqual(
            additional_events,
            0,
            f"Expected 0 redundant status events during 1000 alternating cycles, got {additional_events}",
        )

    # --------------------------------------------------------------------------
    # Gate R2-T2: 1,000 Telemetry-Only Cycles -> 0 Additional Status Events
    # --------------------------------------------------------------------------

    def test_r2_t2_telemetry_only_cycles(self):
        """Verify 1,000 cycles with fluctuating PIDs/windows/timestamps emit 0 additional status events."""
        status = self._write_status()
        self.service._apply_runtime_status(status)
        patches = self._mock_worker_sync_patches()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            self.worker.run_account(self.account_config)

        baseline_events = self._count_status_events()

        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            for i in range(1000):
                # Telemetry changes: pids, windows, window_error fluctuation
                status = self._write_status(
                    pids=[2000 + (i % 50), 3000 + (i % 20)],
                    windows=[f"0x{10000 + i:05x}"],
                    window_error=None if i % 2 == 0 else "transient notice",
                )
                self.service._apply_runtime_status(status)
                self.worker.run_account(self.account_config)

        final_events = self._count_status_events()
        additional_events = final_events - baseline_events
        self.assertEqual(
            additional_events,
            0,
            f"Expected 0 status events for telemetry-only fluctuations over 1000 cycles, got {additional_events}",
        )

    # --------------------------------------------------------------------------
    # Gate R2-T3: Meaningful Semantic Transitions Emit Exactly 1 Event
    # --------------------------------------------------------------------------

    def test_r2_t3_meaningful_transitions(self):
        """Verify meaningful transitions emit exactly 1 event each, and mismatch fails closed."""
        status = self._write_status()
        self.service._apply_runtime_status(status)
        patches = self._mock_worker_sync_patches()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            self.worker.run_account(self.account_config)

        events_before = self._count_status_events()

        # 1. logged_in -> logged_out
        status = self._write_status(wechat_login_status="logged_out", logged_in_user="")
        self.service._apply_runtime_status(status)
        acct = self.store.account("testb")
        self.assertIsNotNone(acct)
        assert acct is not None
        self.assertEqual(acct["state"], "login_required")
        self.assertEqual(self._count_status_events(), events_before + 1)
        events_before = self._count_status_events()

        # Repeat without change -> 0 events
        self.service._apply_runtime_status(status)
        self.assertEqual(self._count_status_events(), events_before)

        # 2. logged_out -> logged_in
        status = self._write_status(wechat_login_status="logged_in", logged_in_user=WXID_CURRENT)
        self.service._apply_runtime_status(status)
        acct = self.store.account("testb")
        self.assertIsNotNone(acct)
        assert acct is not None
        self.assertEqual(acct["state"], "online")
        self.assertEqual(self._count_status_events(), events_before + 1)
        events_before = self._count_status_events()

        # 3. agent_server_healthy: True -> False (online -> degraded)
        status = self._write_status(agent_server_healthy=False, health_error="agent server timeout")
        self.service._apply_runtime_status(status)
        acct = self.store.account("testb")
        self.assertIsNotNone(acct)
        assert acct is not None
        self.assertEqual(acct["state"], "degraded")
        self.assertEqual(self._count_status_events(), events_before + 1)
        events_before = self._count_status_events()

        # 4. agent_server_healthy: False -> True (degraded -> online)
        status = self._write_status(agent_server_healthy=True, health_error="")
        self.service._apply_runtime_status(status)
        acct = self.store.account("testb")
        self.assertIsNotNone(acct)
        assert acct is not None
        self.assertEqual(acct["state"], "online")
        self.assertEqual(self._count_status_events(), events_before + 1)
        events_before = self._count_status_events()

        # 5. running: True -> False (online -> stopped)
        status = self._write_status(running=False, container_running=False)
        self.service._apply_runtime_status(status)
        acct = self.store.account("testb")
        self.assertIsNotNone(acct)
        assert acct is not None
        self.assertEqual(acct["state"], "stopped")
        self.assertEqual(self._count_status_events(), events_before + 1)
        events_before = self._count_status_events()

        # 6. Conflicting identity fails closed
        self.store.observe_login("testb", WXID_CURRENT, verified_source=VERIFIED_SOURCE_AGENT_AUTH)
        mismatch = self.store.observe_login("testb", WXID_CONFLICTING, verified_source=VERIFIED_SOURCE_AGENT_AUTH)
        self.assertEqual(mismatch["state"], "mismatch")
        self.assertFalse(mismatch["binding_created"])
        with self.assertRaises(IdentityError) as send_ctx:
            self.store.identity_send_gate("testb")
        self.assertEqual(send_ctx.exception.code, "identity_binding_changed")

    # --------------------------------------------------------------------------
    # Gate R2-T4: Multi-WXID Transient Observation Discovery & Worker Stability
    # --------------------------------------------------------------------------

    def test_r2_t4_multi_wxid_transient_observation(self):
        """Verify discover_agent_wechat_source_db prefers bound wxid on transient drop and never uses historical."""
        # 1. Normal: logged_in_user matches current
        found = discover_agent_wechat_source_db(
            self.home,
            logged_in_user=WXID_CURRENT,
            account_id="testb",
            bound_wxid=WXID_CURRENT,
        )
        self.assertIsNotNone(found)
        assert found is not None
        self.assertEqual(found[0], self.current_db)

        # 2. Transient missing: logged_in_user is empty, bound_wxid is WXID_CURRENT
        # Must select current_db and NEVER historical_db despite newer mtime
        found_transient = discover_agent_wechat_source_db(
            self.home,
            logged_in_user="",
            account_id="testb",
            bound_wxid=WXID_CURRENT,
        )
        self.assertIsNotNone(found_transient)
        assert found_transient is not None
        self.assertEqual(found_transient[0], self.current_db)

        # 3. Conflicting: logged_in_user conflicts with bound_wxid
        # Must fail closed with SourceIdentityError
        with self.assertRaises(SourceIdentityError) as ctx:
            discover_agent_wechat_source_db(
                self.home,
                logged_in_user=WXID_CONFLICTING,
                account_id="testb",
                bound_wxid=WXID_CURRENT,
            )
        self.assertEqual(ctx.exception.code, "source_identity_mismatch")

        # 4. Full worker cycle with transient missing logged_in_user:
        # Establish binding first
        self.store.observe_login("testb", WXID_CURRENT, verified_source=VERIFIED_SOURCE_AGENT_AUTH)
        # Status file has transient empty logged_in_user
        self._write_status(logged_in_user="")
        patches = self._mock_worker_sync_patches()
        with patches[0], patches[1], patches[2], patches[3], patches[4], patches[5]:
            sync_result = self.worker.run_account(self.account_config)

        # Must succeed and stay on current db
        self.assertTrue(sync_result.get("ok"), f"Sync failed: {sync_result}")
        self.assertEqual(sync_result.get("source_db_dir"), str(self.current_db))
        acct = self.store.account("testb")
        self.assertIsNotNone(acct)
        assert acct is not None
        self.assertEqual(acct["state"], "online")


if __name__ == "__main__":
    unittest.main()
