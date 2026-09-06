"""RB-003 sync worker liveness / SQLite-lock hardening regressions.

Deployed revision 1b797cad4305f9e3e3b746bbad1f9957a4f21f41 terminated the
``wechat-core-sync`` thread permanently when one account-status persistence
write raised ``sqlite3.OperationalError: database is locked`` (live evidence:
2026-09-05T06:00:01Z, "Exception in thread wechat-core-sync").  Three unguarded
steps formed the kill chain:

1. ``AccountWorker.run_account`` called the final ``store.upsert_account``
   outside its account-scoped try/except (deployed lines 128-148);
2. ``run_once`` propagated the first per-account failure through a list
   comprehension (deployed line 152), so a failure on account A also skipped
   account B for that cycle;
3. ``AccountSyncLoop._run`` had no Exception boundary (deployed lines 170-173),
   so the escaping exception killed the thread and sync stopped silently.

The tests below reproduce that kill chain deterministically against a real
``CoreStore`` and pin the fixed semantics: bounded lock retry on the idempotent
account-status persistence path, per-account isolation, a last-resort loop
boundary, and observable liveness telemetry.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import threading
import time
import unittest
import urllib.request
import uuid
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch

CORE_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CORE_ROOT))

from core.account_worker import AccountSyncLoop, AccountWorker, SyncLiveness  # noqa: E402
from core.app import CoreService, create_server  # noqa: E402
from core.registry import AccountRegistry, parse_account  # noqa: E402
from core.store import SQLITE_LOCK_RETRY_DELAYS, CoreStore  # noqa: E402


def temp_root(prefix: str) -> Path:
    root = CORE_ROOT / ".tmp" / f"{prefix}-{uuid.uuid4().hex}"
    root.mkdir(parents=True)
    return root


def build_registry(root: Path) -> AccountRegistry:
    return AccountRegistry(
        [
            parse_account({"account_id": "alpha", "display_name": "Alpha", "runtime_dir": "runtime/accounts/alpha"}, root=root),
            parse_account({"account_id": "beta", "display_name": "Beta", "runtime_dir": "runtime/accounts/beta"}, root=root),
        ],
        root / "accounts.json",
    )


def fast_lock_connect(original_connect):
    """Return a connect() wrapper with a short busy_timeout for tests."""

    def connect(self):
        conn = original_connect(self)
        conn.execute("PRAGMA busy_timeout=50")
        return conn

    return connect


class LockErrorInjector:
    """Raises the live incident error from CoreStore.upsert_account on demand.

    The replacement is installed as a class attribute, so Python binds the
    store instance as the first argument on every call.
    """

    def __init__(self, *, account_ids: set[str] | None = None, max_failures: int | None = None) -> None:
        self.account_ids = account_ids
        self.max_failures = max_failures
        self.failures = 0
        self.attempts = 0
        self._lock = threading.Lock()
        self._original = CoreStore.upsert_account
        self._patch = None

    def __enter__(self):
        injector = self

        def _upsert(store_ref, account_id, display_name, **kwargs):
            with injector._lock:
                injector.attempts += 1
                blocked = (injector.account_ids is None or account_id in injector.account_ids) and (
                    injector.max_failures is None or injector.failures < injector.max_failures
                )
                if blocked:
                    injector.failures += 1
                    raise sqlite3.OperationalError("database is locked")
            return injector._original(store_ref, account_id, display_name, **kwargs)

        self._patch = patch.object(CoreStore, "upsert_account", _upsert)
        self._patch.start()
        return self

    def __exit__(self, *exc_info):
        self._patch.stop()
        return False


class DeployedPatternReplica:
    """Deterministic replica of the deployed (rev 1b797cad) failure pattern.

    Mirrors exactly the three unguarded steps quoted above; the pipeline body
    is stubbed to the same account-scoped error the live dummy accounts
    produced, so the final unguarded ``upsert_account`` is the only way a
    cycle completes.
    """

    def __init__(self, worker: AccountWorker) -> None:
        self.worker = worker
        self.cycles = 0

    def run_once(self):
        # Deployed line 152: failure-propagating list comprehension.
        results = [self.run_account(account) for account in self.worker.registry.all()]
        self.cycles += 1
        return {"ok": all(result.get("ok") for result in results), "accounts": results}

    def run_account(self, account):
        status = {"account_id": account.account_id, "ok": False, "error": "pipeline unavailable"}
        # Deployed lines 142-148: final status persistence outside any guard.
        self.worker.store.upsert_account(
            account.account_id,
            account.display_name,
            state="error",
            runtime={"registered": True},
            sync=status,
        )
        return status


class DeployedPatternReproductionTest(unittest.TestCase):
    def test_unguarded_persistence_lock_terminates_deployed_pattern(self):
        root = temp_root("rb3d-deployed-repro")
        try:
            worker = AccountWorker(build_registry(root), CoreStore(root / "core.sqlite"))
            replica = DeployedPatternReplica(worker)
            stop = threading.Event()

            def deployed_loop():
                # Deployed lines 170-173: bare loop without an Exception boundary.
                while not stop.is_set():
                    replica.run_once()
                    stop.wait(0.05)

            thread = threading.Thread(target=deployed_loop, name="wechat-core-sync", daemon=True)
            with LockErrorInjector():
                thread.start()
                time.sleep(0.4)
            stop.set()
            thread.join(timeout=2)
            self.assertFalse(thread.is_alive(), "replica thread should have died from the injected lock")
            self.assertEqual(replica.cycles, 0, "deployed pattern must not complete any cycle once the lock hits")
        finally:
            shutil.rmtree(root, ignore_errors=True)


class AccountStatusPersistenceHardeningTest(unittest.TestCase):
    def test_persistence_lock_is_recorded_not_raised(self):
        root = temp_root("rb3d-persist-lock")
        try:
            registry = build_registry(root)
            store = CoreStore(root / "core.sqlite")
            worker = AccountWorker(registry, store)
            account = registry.get("alpha")
            assert account is not None
            with LockErrorInjector(account_ids={"alpha"}):
                status = worker.run_account(account)
            self.assertTrue(status.get("persistence_error"))
            self.assertIn("database is locked", status["persistence_error"])
            persisted = json.loads(account.sync_status_file.read_text(encoding="utf-8"))
            self.assertIn("persistence_error", persisted)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_persistence_failure_does_not_block_peer_account(self):
        root = temp_root("rb3d-persist-peer")
        try:
            store = CoreStore(root / "core.sqlite")
            worker = AccountWorker(build_registry(root), store)
            with LockErrorInjector(account_ids={"alpha"}):
                result = worker.run_once()
            by_account = {item["account_id"]: item for item in result["accounts"]}
            self.assertIn("persistence_error", by_account["alpha"])
            self.assertIn("beta", by_account, "beta must run in the same cycle as alpha's failure")
            self.assertIsNotNone(store.account("beta"), "beta status must still be persisted in the same cycle")
            self.assertIsNone(store.account("alpha"), "alpha persistence was locked and must stay absent, not partial")
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_persistence_retry_on_real_lock_then_success(self):
        root = temp_root("rb3d-real-lock-retry")
        try:
            store = CoreStore(root / "core.sqlite")
            release = threading.Event()
            holder_ready = threading.Event()

            def hold_lock():
                conn = sqlite3.connect(store.db_path, timeout=5)
                conn.execute("BEGIN IMMEDIATE")
                conn.execute("CREATE TABLE IF NOT EXISTS lock_probe (id INTEGER)")
                conn.execute("INSERT INTO lock_probe VALUES (1)")
                holder_ready.set()
                release.wait(5.0)
                conn.rollback()
                conn.close()

            thread = threading.Thread(target=hold_lock, daemon=True)
            thread.start()
            holder_ready.wait(5.0)
            try:
                with patch.object(CoreStore, "connect", fast_lock_connect(CoreStore.connect)), patch(
                    "core.store.SQLITE_LOCK_RETRY_DELAYS", (0.1, 0.2, 0.3, 0.4, 0.5)
                ):
                    outcome: dict = {}

                    def persist():
                        try:
                            store.upsert_account("alpha", "Alpha", state="online", runtime={"a": 1}, sync={"ok": True})
                            outcome["ok"] = True
                        except Exception as exc:  # pragma: no cover - assertion below
                            outcome["error"] = str(exc)

                    persister = threading.Thread(target=persist, daemon=True)
                    persister.start()
                    time.sleep(0.5)
                    release.set()
                    persister.join(timeout=10)
                self.assertTrue(outcome.get("ok"), f"retry must succeed after the lock clears: {outcome}")
                self.assertIsNotNone(store.account("alpha"))
                events = store.poll_events(after="0", limit=50, account_id="alpha")
                status_events = [item for item in events["events"] if item["event_type"] == "account.status"]
                self.assertEqual(len(status_events), 1, "a retried idempotent persistence must not duplicate events")
            finally:
                release.set()
                thread.join(timeout=5)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_persistence_retry_is_bounded_under_permanent_lock(self):
        root = temp_root("rb3d-permanent-lock")
        try:
            store = CoreStore(root / "core.sqlite")
            holder = sqlite3.connect(store.db_path, timeout=5)
            holder.execute("BEGIN IMMEDIATE")
            holder.execute("CREATE TABLE IF NOT EXISTS lock_probe (id INTEGER)")
            delays = (0.05, 0.05, 0.05)
            started = time.monotonic()
            try:
                with patch.object(CoreStore, "connect", fast_lock_connect(CoreStore.connect)), patch(
                    "core.store.SQLITE_LOCK_RETRY_DELAYS", delays
                ):
                    with self.assertRaises(sqlite3.OperationalError):
                        store.upsert_account("alpha", "Alpha", state="online")
                elapsed = time.monotonic() - started
                self.assertLess(elapsed, 15.0, "bounded retry must not busy-loop indefinitely")
                self.assertGreaterEqual(elapsed, 0.1, "at least one retry must have been attempted")
            finally:
                holder.rollback()
                holder.close()
        finally:
            shutil.rmtree(root, ignore_errors=True)


class StubWorker:
    """run_once contract stub for loop-level scheduling/liveness tests."""

    def __init__(self, script: list) -> None:
        self.script = list(script)
        self.calls = 0
        self._lock = threading.Lock()

    def run_once(self):
        with self._lock:
            self.calls += 1
            index = min(self.calls - 1, len(self.script) - 1)
            behavior = self.script[index]
        if isinstance(behavior, Exception):
            raise behavior
        return behavior


def make_loop(tmp: Path, worker, interval: float = 0.1) -> AccountSyncLoop:
    loop = AccountSyncLoop(worker, 1.0, liveness_path=tmp / "sync_liveness.json")
    loop.interval_seconds = interval  # bypass the production 1s floor for tests
    loop.liveness.interval_seconds = interval
    return loop


class SyncLoopLivenessTest(unittest.TestCase):
    def test_injected_lock_does_not_kill_loop_and_peer_cycles_continue(self):
        root = temp_root("rb3d-loop-lock")
        try:
            store = CoreStore(root / "core.sqlite")
            worker = AccountWorker(build_registry(root), store)
            loop = make_loop(root, worker)
            with LockErrorInjector(account_ids={"alpha"}):
                loop.start()
                deadline = time.monotonic() + 3.0
                while time.monotonic() < deadline and loop.liveness.snapshot()["cycle_count"] < 3:
                    time.sleep(0.05)
                snapshot = loop.liveness.snapshot()
                alive = loop._thread.is_alive()
            loop.stop()
            self.assertTrue(alive, "sync worker must survive repeated injected locks")
            self.assertGreaterEqual(snapshot["cycle_count"], 3)
            self.assertTrue(snapshot["last_account_errors"], "injected lock must surface in liveness telemetry")
            self.assertIn("alpha", snapshot["last_account_errors"])
            self.assertGreaterEqual(snapshot["consecutive_failed_cycles"], 1)
            self.assertGreaterEqual((root / "runtime" / "accounts" / "beta" / "memory" / "sync_status.json").stat().st_size, 0)
        finally:
            loop.stop()
            shutil.rmtree(root, ignore_errors=True)

    def test_unexpected_cycle_exception_is_recorded_and_bounded(self):
        root = temp_root("rb3d-loop-exception")
        try:
            worker = StubWorker([RuntimeError("boom"), RuntimeError("boom again"), {"ok": True, "accounts": []}])
            loop = make_loop(root, worker)
            loop.start()
            deadline = time.monotonic() + 5.0
            while time.monotonic() < deadline:
                snapshot = loop.liveness.snapshot()
                if snapshot["last_cycle_error"] and snapshot["consecutive_failed_cycles"] == 0 and snapshot["last_clean_cycle_at"]:
                    break
                time.sleep(0.05)
            loop.stop()
            self.assertGreaterEqual(worker.calls, 3, "loop must continue after unexpected exceptions")
            self.assertIn("RuntimeError: boom", snapshot["last_cycle_error"])
            self.assertEqual(snapshot["consecutive_failed_cycles"], 0, "clean cycle must reset the failure streak")
            self.assertTrue(snapshot["last_clean_cycle_at"])
        finally:
            loop.stop()
            shutil.rmtree(root, ignore_errors=True)

    def test_repeated_locks_become_observable_degradation_not_silence(self):
        root = temp_root("rb3d-loop-degraded")
        try:
            store = CoreStore(root / "core.sqlite")
            worker = AccountWorker(build_registry(root), store)
            loop = make_loop(root, worker)
            with LockErrorInjector(), patch("core.store.SQLITE_LOCK_RETRY_DELAYS", (0.01, 0.01)):
                loop.start()
                deadline = time.monotonic() + 3.0
                while time.monotonic() < deadline and loop.liveness.snapshot()["consecutive_failed_cycles"] < 2:
                    time.sleep(0.05)
                snapshot = loop.liveness.snapshot()
                alive = loop._thread.is_alive()
            loop.stop()
            self.assertTrue(alive)
            self.assertGreaterEqual(snapshot["cycle_count"], 2)
            self.assertGreaterEqual(snapshot["consecutive_failed_cycles"], 2)
            self.assertGreater(snapshot["last_completed_cycle_at"], "")
        finally:
            loop.stop()
            shutil.rmtree(root, ignore_errors=True)

    def test_recovery_after_lock_resumes_sync(self):
        root = temp_root("rb3d-loop-recovery")
        try:
            store = CoreStore(root / "core.sqlite")
            worker = AccountWorker(build_registry(root), store)
            loop = make_loop(root, worker)
            # The first two alpha persistence attempts hit the lock; afterwards
            # the lock clears and sync must resume automatically.
            injector = LockErrorInjector(account_ids={"alpha"}, max_failures=2)
            with injector:
                loop.start()
                deadline = time.monotonic() + 3.0
                while time.monotonic() < deadline and injector.failures < 2:
                    time.sleep(0.05)
            deadline = time.monotonic() + 3.0
            while time.monotonic() < deadline and loop.liveness.snapshot()["cycle_count"] < 4:
                time.sleep(0.05)
            loop.stop()
            snapshot = loop.liveness.snapshot()
            self.assertGreaterEqual(snapshot["cycle_count"], 4, "cycles must continue past the lock window")
            self.assertIsNotNone(store.account("alpha"), "alpha persistence must resume after the lock clears")
            status = json.loads(
                (root / "runtime" / "accounts" / "alpha" / "memory" / "sync_status.json").read_text(encoding="utf-8")
            )
            self.assertNotIn("persistence_error", status, "recovered cycles must persist alpha without lock errors")
        finally:
            loop.stop()
            shutil.rmtree(root, ignore_errors=True)

    def test_normal_cadence_preserved_without_failures(self):
        root = temp_root("rb3d-loop-cadence")
        try:
            worker = StubWorker([{"ok": True, "accounts": [{"account_id": "alpha", "ok": True}]}])
            loop = make_loop(root, worker)
            loop.start()
            time.sleep(1.0)
            snapshot = loop.liveness.snapshot()
            loop.stop()
            self.assertGreaterEqual(worker.calls, 4, "interval must keep its cadence (0.1s interval over 1s)")
            self.assertEqual(snapshot["consecutive_failed_cycles"], 0)
            self.assertTrue(snapshot["last_clean_cycle_at"])
            self.assertEqual(snapshot["last_cycle_error"], "")
        finally:
            loop.stop()
            shutil.rmtree(root, ignore_errors=True)

    def test_liveness_flushes_snapshot_to_disk(self):
        root = temp_root("rb3d-liveness-file")
        try:
            worker = StubWorker([{"ok": True, "accounts": []}])
            loop = make_loop(root, worker)
            loop.start()
            time.sleep(0.4)
            loop.stop()
            payload = json.loads((root / "sync_liveness.json").read_text(encoding="utf-8"))
            self.assertEqual(payload["worker"], "wechat-core-sync")
            self.assertGreaterEqual(payload["cycle_count"], 1)
            self.assertTrue(payload["last_completed_cycle_at"])
        finally:
            loop.stop()
            shutil.rmtree(root, ignore_errors=True)


class AccountsPollContentionTest(unittest.TestCase):
    def _service(self, root: Path) -> tuple[CoreService, CoreStore]:
        registry = build_registry(root)
        store = CoreStore(root / "core.sqlite")
        return CoreService(root=root, registry=registry, store=store), store

    def test_unchanged_runtime_status_write_is_skipped(self):
        root = temp_root("rb3d-poll-unchanged")
        try:
            service, store = self._service(root)
            status = {
                "account_id": "alpha",
                "display_name": "Alpha",
                "runtime_provider": "agent_wechat",
                "running": True,
                "container_running": True,
                "agent_server_healthy": True,
                "wechat_login_status": "logged_in",
                "pids": [11],
                "windows": ["w1"],
            }
            service._apply_runtime_status(status)
            first = store.account("alpha")
            writes = {"n": 0}
            original = CoreStore.upsert_account

            def counting_upsert(store_ref, *args, **kwargs):
                writes["n"] += 1
                return original(store_ref, *args, **kwargs)

            with patch.object(CoreStore, "upsert_account", counting_upsert):
                service._apply_runtime_status(status)
            self.assertEqual(writes["n"], 0, "identical runtime status must not become a DB write on every poll")
            self.assertEqual(store.account("alpha"), first)
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_changed_runtime_status_still_persists(self):
        root = temp_root("rb3d-poll-changed")
        try:
            service, store = self._service(root)
            status = {
                "account_id": "alpha",
                "display_name": "Alpha",
                "runtime_provider": "agent_wechat",
                "running": True,
                "agent_server_healthy": True,
                "wechat_login_status": "logged_in",
                "pids": [11],
                "windows": [],
            }
            service._apply_runtime_status(status)
            degraded = dict(status, agent_server_healthy=False, health_error="health timeout")
            service._apply_runtime_status(degraded)
            self.assertEqual(store.account("alpha")["state"], "degraded")
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_accounts_poll_survives_store_write_failure(self):
        root = temp_root("rb3d-poll-failure")
        try:
            service, store = self._service(root)
            store.upsert_account("alpha", "Alpha", state="online", runtime={"registered": True})

            class StubRuntimeControl:
                available = True

                def __init__(self, payload: dict) -> None:
                    self.payload = payload

                def request(self, action: str, **payload):
                    return self.payload

            status = {
                "account_id": "alpha",
                "display_name": "Alpha",
                "running": True,
                "agent_server_healthy": True,
                "wechat_login_status": "logged_in",
            }
            service.runtime_control = StubRuntimeControl({"accounts": [status]})
            with patch.object(CoreStore, "upsert_account", side_effect=sqlite3.OperationalError("database is locked")):
                output = service.accounts()
            account_ids = [item["account_id"] for item in output]
            self.assertIn("alpha", account_ids, "polling must keep serving the existing projection when a write fails")
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_health_exposes_sync_worker_liveness(self):
        root = temp_root("rb3d-health-liveness")
        try:
            service, _ = self._service(root)
            worker = StubWorker([{"ok": True, "accounts": []}])
            loop = make_loop(root, worker)
            service.sync_worker_liveness = loop.liveness
            server = create_server("127.0.0.1", 0, service)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            loop.start()
            try:
                time.sleep(0.4)
                with urllib.request.urlopen(f"http://127.0.0.1:{server.server_port}/health", timeout=3) as response:
                    health = json.loads(response.read())
                self.assertIn("sync_worker", health)
                self.assertEqual(health["sync_worker"]["worker"], "wechat-core-sync")
                self.assertGreaterEqual(health["sync_worker"]["cycle_count"], 1)
            finally:
                loop.stop()
                server.shutdown()
                server.server_close()
                thread.join(timeout=2)
        finally:
            shutil.rmtree(root, ignore_errors=True)


class StoreLockRetryGuardTest(unittest.TestCase):
    def test_non_lock_operational_errors_are_not_retried(self):
        root = temp_root("rb3d-nonlock-error")
        try:
            store = CoreStore(root / "core.sqlite")
            attempts = {"n": 0}

            @contextmanager
            def failing_connection(self):
                attempts["n"] += 1
                raise sqlite3.OperationalError("disk I/O error")
                yield  # pragma: no cover

            with patch.object(CoreStore, "connection", failing_connection):
                with self.assertRaises(sqlite3.OperationalError):
                    store.upsert_account("alpha", "Alpha", state="online")
            self.assertEqual(
                attempts["n"],
                1,
                "only transient lock errors may be retried; other failures must surface immediately",
            )
        finally:
            shutil.rmtree(root, ignore_errors=True)

    def test_lock_predicate_classification(self):
        from core.store import is_transient_sqlite_lock

        self.assertTrue(is_transient_sqlite_lock(sqlite3.OperationalError("database is locked")))
        self.assertTrue(is_transient_sqlite_lock(sqlite3.OperationalError("database schema is locked")))
        self.assertFalse(is_transient_sqlite_lock(sqlite3.OperationalError("no such table: accounts")))
        self.assertFalse(is_transient_sqlite_lock(sqlite3.OperationalError("disk I/O error")))
        self.assertFalse(is_transient_sqlite_lock(RuntimeError("database is locked")))

    def test_retry_delays_are_bounded_by_design(self):
        total = sum(SQLITE_LOCK_RETRY_DELAYS)
        self.assertLess(total, 30.0, "bounded retry budget must stay small")
        self.assertTrue(all(delay > 0 for delay in SQLITE_LOCK_RETRY_DELAYS))
        self.assertLessEqual(len(SQLITE_LOCK_RETRY_DELAYS), 8)


if __name__ == "__main__":
    import unittest

    unittest.main()
