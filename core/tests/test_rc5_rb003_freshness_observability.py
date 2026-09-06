"""Regression tests for RC.5 RB-003 freshness completeness & observability.

Verifies:
1. missing_key on active shards produces degraded/incomplete state, not ok=true.
2. Complete healthy cycle produces ok=true, completeness=complete, state=online.
3. Freshness telemetry records source, decrypt, staging, Core timestamps/deltas
   without leaking any encryption keys or auth tokens.
4. Partial/failed refresh preserves last known-good decrypted DB without destructive deletion.
5. Freshness SLO marks stale finished_at as degraded even if HTTP server is healthy.
6. CoreService._apply_runtime_status does not mask degraded sync with healthy online state.
7. CoreService.accounts() downgrades stale accounts to degraded.
8. CoreService.sync_health() surfaces worker liveness, SLO, and stale/degraded accounts.
9. AccountSyncLoop error isolation and failure telemetry.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import tempfile
import time
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

CORE_ROOT = Path(__file__).resolve().parents[2]
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from core.account_worker import (
    DEFAULT_FRESHNESS_SLO_MULTIPLIER,
    DEFAULT_MIN_FRESHNESS_SLO_SECONDS,
    AccountSyncLoop,
    AccountWorker,
    evaluate_account_freshness,
)
from core.app import CoreService
from core.registry import AccountRegistry, parse_account
from core.store import CoreStore, utc_now
import memory.memory_ingest as memory_ingest_module


class FreshnessObservabilityTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.root = Path(self.temp.name)

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def _create_test_account(self, account_id: str = "test-account") -> tuple[Any, CoreStore]:
        source = self.root / "home" / "xwechat_files" / f"wxid_{account_id}" / "db_storage"
        (source / "message").mkdir(parents=True, exist_ok=True)
        (source / "message" / "message_0.db").write_bytes(b"dummy-encrypted-content")

        keys_file = self.root / "runtime" / account_id / "keys.json"
        keys_file.parent.mkdir(parents=True, exist_ok=True)
        test_hex_key = "a" * 64
        keys_file.write_text(json.dumps({"_db_dir": str(source), "message/message_0.db": {"enc_key": test_hex_key}}), encoding="utf-8")

        account = parse_account(
            {
                "account_id": account_id,
                "display_name": f"Account {account_id}",
                "source_db_dir": str(source),
                "runtime_dir": str(self.root / "runtime" / account_id),
            },
            root=self.root,
        )
        account.keys_file.parent.mkdir(parents=True, exist_ok=True)
        account.keys_file.write_text(keys_file.read_text(encoding="utf-8"), encoding="utf-8")

        store = CoreStore(self.root / f"{account_id}_core.sqlite")
        store.upsert_account(account_id, f"Account {account_id}", state="online")
        return account, store

    def test_missing_key_sets_ok_false_and_degraded_state(self) -> None:
        account, store = self._create_test_account("acc-missing-key")
        try:
            registry = AccountRegistry([account], self.root / "accounts.json")
            worker = AccountWorker(registry, store)

            mock_refresh = {
                "updated": [],
                "skipped": [],
                "missing_key": ["message/message_1.db"],
                "failed": [],
            }
            with patch("memory.decrypt_sync.refresh_decrypted", return_value=mock_refresh), \
                 patch.object(memory_ingest_module, "ingest_memory", return_value={"chats": 1, "messages": 10, "changed_rows": 0}), \
                 patch("memory.media_sync.sync_media", return_value={}), \
                 patch("core.account_worker.import_account", return_value={"chats": 1, "messages": 10, "message_changes": 0}):
                status = worker.run_account(account)

            self.assertFalse(status["ok"], "missing_key must not result in ok=True")
            self.assertEqual(status["completeness"], "incomplete")
            self.assertEqual(status["missing_key_shards"], ["message/message_1.db"])
            self.assertIn("message/message_1.db", status["degraded_reason"])
            self.assertEqual(status["freshness"]["status"], "degraded")
            self.assertEqual(status["freshness"]["completeness"], "incomplete")

            stored = store.account(account.account_id)
            self.assertIsNotNone(stored)
            assert stored is not None
            self.assertEqual(stored["state"], "degraded", "Account with missing key must be stored as degraded")
            self.assertFalse(stored["sync"]["ok"])
            self.assertEqual(stored["sync"]["completeness"], "incomplete")
        finally:
            store.close()

    def test_healthy_refresh_sets_ok_true_and_online_state(self) -> None:
        account, store = self._create_test_account("acc-healthy")
        try:
            registry = AccountRegistry([account], self.root / "accounts.json")
            worker = AccountWorker(registry, store)

            mock_refresh = {
                "updated": [{"db": "message/message_0.db", "patched_wal_pages": 1}],
                "skipped": [],
                "missing_key": [],
                "failed": [],
            }
            with patch("memory.decrypt_sync.refresh_decrypted", return_value=mock_refresh), \
                 patch.object(memory_ingest_module, "ingest_memory", return_value={"chats": 2, "messages": 20, "changed_rows": 5}), \
                 patch("memory.media_sync.sync_media", return_value={}), \
                 patch("core.account_worker.import_account", return_value={"chats": 2, "messages": 20, "message_changes": 5}):
                status = worker.run_account(account)

            self.assertTrue(status["ok"])
            self.assertEqual(status["completeness"], "complete")
            self.assertEqual(status["freshness"]["status"], "healthy")
            self.assertEqual(status["freshness"]["completeness"], "complete")

            stored = store.account(account.account_id)
            self.assertIsNotNone(stored)
            assert stored is not None
            self.assertEqual(stored["state"], "online")
            self.assertTrue(stored["sync"]["ok"])
        finally:
            store.close()

    def test_freshness_telemetry_contains_all_layers_and_no_key_leaks(self) -> None:
        account, store = self._create_test_account("acc-telemetry")
        try:
            registry = AccountRegistry([account], self.root / "accounts.json")
            worker = AccountWorker(registry, store)

            mock_refresh = {
                "updated": [{"db": "message/message_0.db", "patched_wal_pages": 2}],
                "skipped": ["contact/contact.db"],
                "missing_key": [],
                "failed": [],
            }
            with patch("memory.decrypt_sync.refresh_decrypted", return_value=mock_refresh), \
                 patch.object(memory_ingest_module, "ingest_memory", return_value={"chats": 3, "messages": 45, "changed_rows": 2}), \
                 patch("memory.media_sync.sync_media", return_value={}), \
                 patch("core.account_worker.import_account", return_value={"chats": 3, "messages": 45, "message_changes": 2}):
                status = worker.run_account(account)

            freshness = status["freshness"]
            self.assertIn("source", freshness)
            self.assertIn("decrypt", freshness)
            self.assertIn("staging", freshness)
            self.assertIn("core", freshness)

            self.assertEqual(freshness["source"]["total_shards"], 2)
            self.assertEqual(freshness["decrypt"]["updated_count"], 1)
            self.assertEqual(freshness["decrypt"]["skipped_count"], 1)
            self.assertEqual(freshness["staging"]["messages"], 45)
            self.assertEqual(freshness["staging"]["changed_rows"], 2)
            self.assertEqual(freshness["core"]["messages"], 45)
            self.assertEqual(freshness["core"]["message_changes"], 2)

            # Security audit: assert no hex encryption keys leaked in status JSON
            status_json_str = json.dumps(status)
            self.assertNotIn("a" * 64, status_json_str, "Raw hex encryption key leaked into sync status!")
            self.assertNotIn("enc_key", status_json_str)
        finally:
            store.close()

    def test_partial_refresh_leaves_decrypted_db_without_destructive_deletion(self) -> None:
        account, store = self._create_test_account("acc-non-destructive")
        try:
            # Pre-populate an existing decrypted DB file
            decrypted_db = account.decrypted_dir / "message" / "message_0.db"
            decrypted_db.parent.mkdir(parents=True, exist_ok=True)
            decrypted_db.write_bytes(b"existing-good-decrypted-sqlite-data")

            registry = AccountRegistry([account], self.root / "accounts.json")
            worker = AccountWorker(registry, store)

            mock_refresh = {
                "updated": [],
                "skipped": [],
                "missing_key": ["message/message_0.db"],
                "failed": [],
            }
            with patch("memory.decrypt_sync.refresh_decrypted", return_value=mock_refresh), \
                 patch.object(memory_ingest_module, "ingest_memory", return_value={"chats": 1, "messages": 5, "changed_rows": 0}), \
                 patch("memory.media_sync.sync_media", return_value={}), \
                 patch("core.account_worker.import_account", return_value={"chats": 1, "messages": 5, "message_changes": 0}):
                status = worker.run_account(account)

            # Assert file was NOT destructively deleted
            self.assertTrue(decrypted_db.exists(), "Decrypted DB must not be deleted merely for observability")
            self.assertEqual(decrypted_db.read_bytes(), b"existing-good-decrypted-sqlite-data")

            # But status correctly reflects degraded state
            self.assertFalse(status["ok"])
            self.assertEqual(status["completeness"], "incomplete")
            self.assertEqual(status["missing_key_shards"], ["message/message_0.db"])
        finally:
            store.close()

    def test_freshness_slo_evaluation_recent_vs_stale(self) -> None:
        now = datetime.now(timezone.utc)
        recent_time = (now - timedelta(seconds=10)).isoformat(timespec="seconds").replace("+00:00", "Z")
        stale_time = (now - timedelta(seconds=120)).isoformat(timespec="seconds").replace("+00:00", "Z")

        # Test recent sync: SLO = max(60, 5 * 6) = 60s. Age = 10s -> fresh
        account_recent = {
            "account_id": "fresh-acc",
            "state": "online",
            "sync": {"ok": True, "finished_at": recent_time, "freshness": {"status": "healthy"}},
        }
        res_recent = evaluate_account_freshness(account_recent, sync_interval=5.0, now=now)
        self.assertEqual(res_recent["state"], "online")
        self.assertFalse(res_recent["sync"]["stale"])
        self.assertEqual(res_recent["sync"]["slo_seconds"], 60.0)

        # Test stale sync: Age = 120s -> stale beyond 60s SLO
        account_stale = {
            "account_id": "stale-acc",
            "state": "online",
            "sync": {"ok": True, "finished_at": stale_time, "freshness": {"status": "healthy"}},
        }
        res_stale = evaluate_account_freshness(account_stale, sync_interval=5.0, now=now)
        self.assertEqual(res_stale["state"], "degraded", "Account with stale sync must be marked degraded")
        self.assertTrue(res_stale["sync"]["stale"])
        self.assertEqual(res_stale["sync"]["freshness"]["status"], "stale")
        self.assertGreaterEqual(res_stale["sync"]["staleness_seconds"], 120.0)

    def test_apply_runtime_status_does_not_mask_degraded_sync_with_online(self) -> None:
        account, store = self._create_test_account("acc-mask-prevent")
        try:
            # Set account to degraded due to missing key in sync
            store.upsert_account(
                account.account_id,
                account.display_name,
                state="degraded",
                sync={"ok": False, "completeness": "incomplete", "missing_key_shards": ["message/message_0.db"]},
            )

            registry = AccountRegistry([account], self.root / "accounts.json")
            service = CoreService(root=self.root, registry=registry, store=store)

            # Runtime container reports completely healthy & logged in
            runtime_status = {
                "account_id": account.account_id,
                "display_name": account.display_name,
                "runtime_provider": "agent_wechat",
                "running": True,
                "container_running": True,
                "agent_server_healthy": True,
                "wechat_login_status": "logged_in",
                "logged_in_user": "wxid_mask_prevent",
                "pids": [101, 102],
                "windows": [],
            }
            service._apply_runtime_status(runtime_status)

            stored = store.account(account.account_id)
            self.assertIsNotNone(stored)
            assert stored is not None
            self.assertEqual(stored["state"], "degraded", "Healthy runtime auth must NOT mask degraded sync pipeline!")
        finally:
            store.close()

    def test_api_accounts_surfaces_stale_account_as_degraded(self) -> None:
        account, store = self._create_test_account("acc-api-stale")
        try:
            now = datetime.now(timezone.utc)
            stale_finished_at = (now - timedelta(seconds=300)).isoformat(timespec="seconds").replace("+00:00", "Z")
            store.upsert_account(
                account.account_id,
                account.display_name,
                state="online",
                sync={"ok": True, "finished_at": stale_finished_at},
            )

            registry = AccountRegistry([account], self.root / "accounts.json")
            service = CoreService(root=self.root, registry=registry, store=store, sync_interval=5.0)

            accounts_list = service.accounts()
            self.assertEqual(len(accounts_list), 1)
            acct = accounts_list[0]
            self.assertEqual(acct["state"], "degraded", "accounts() must report stale account as degraded")
            self.assertTrue(acct["sync"]["stale"])
            self.assertGreaterEqual(acct["sync"]["staleness_seconds"], 300.0)
        finally:
            store.close()

    def test_sync_health_surfaces_worker_liveness_and_stale_accounts(self) -> None:
        account, store = self._create_test_account("acc-health-check")
        try:
            registry = AccountRegistry([account], self.root / "accounts.json")
            mock_loop = MagicMock()
            mock_loop.is_alive.return_value = True
            mock_loop.consecutive_failures = 0
            mock_loop.last_run_at = utc_now()
            mock_loop.last_error = ""

            service = CoreService(
                root=self.root,
                registry=registry,
                store=store,
                sync_loop=mock_loop,
                sync_interval=5.0,
            )

            # Case A: Fresh account
            store.upsert_account(
                account.account_id,
                account.display_name,
                state="online",
                sync={"ok": True, "finished_at": utc_now()},
            )
            health = service.sync_health()
            self.assertTrue(health["ok"])
            self.assertTrue(health["worker_alive"])
            self.assertEqual(health["stale_accounts"], [])
            self.assertEqual(health["degraded_accounts"], [])

            # Case B: Worker thread died (like the SQLite lock death in RB-003)
            mock_loop.is_alive.return_value = False
            mock_loop.last_error = "sqlite3.OperationalError: database is locked"
            mock_loop.consecutive_failures = 1
            health_dead = service.sync_health()
            self.assertFalse(health_dead["ok"], "sync_health must be False when worker thread is dead")
            self.assertFalse(health_dead["worker_alive"])
            self.assertEqual(health_dead["last_error"], "sqlite3.OperationalError: database is locked")

            # Case C: Account is stale
            mock_loop.is_alive.return_value = True
            now = datetime.now(timezone.utc)
            stale_finished_at = (now - timedelta(seconds=200)).isoformat(timespec="seconds").replace("+00:00", "Z")
            store.upsert_account(
                account.account_id,
                account.display_name,
                state="online",
                sync={"ok": True, "finished_at": stale_finished_at},
            )
            health_stale = service.sync_health()
            self.assertFalse(health_stale["ok"])
            self.assertIn(account.account_id, health_stale["stale_accounts"])
        finally:
            store.close()

    def test_sync_loop_resilience_and_failure_telemetry(self) -> None:
        registry = AccountRegistry([], self.root / "accounts.json")
        store = CoreStore(self.root / "loop_test.sqlite")
        try:
            worker = AccountWorker(registry, store)
            loop = AccountSyncLoop(worker, interval_seconds=1.0)
            self.assertTrue(loop.last_run_ok)
            self.assertEqual(loop.consecutive_failures, 0)

            # Simulate run_once error
            worker.run_once = MagicMock(side_effect=RuntimeError("injected lock error"))
            # Run one cycle manually through worker
            with patch.object(loop, "_stop", MagicMock()):
                loop._stop.is_set.side_effect = [False, True]
                loop._stop.wait = MagicMock()
                loop._run()

            self.assertFalse(loop.last_run_ok)
            self.assertEqual(loop.consecutive_failures, 1)
            self.assertIn("injected lock error", loop.last_error)
        finally:
            store.close()


if __name__ == "__main__":
    unittest.main()
