"""RC.14 Storage Deduplication, Checkpoint Protocol, Compaction & Repository Tests.

Validates:
- Gate T1: Heartbeat suppression (1000 unchanged ticks -> 0 events, telemetry churn -> 0 events, 1 transition -> 1 event)
- Gate T3: Checkpoint monotonicity (forward, idempotent, backward rejected, beyond head rejected)
- Gate T4: 100,000 synthetic status event compaction on temporary DB
- Work Package F: Core storage repository protocol seam contract
"""

from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import tempfile
import unittest
import sqlite3

from core.compaction import apply_status_compaction, plan_status_compaction
from core.repository import (
    AccountRepositoryProtocol,
    CoreStorageRepositoryProtocol,
    EventRepositoryProtocol,
    MessageRepositoryProtocol,
    SQLiteCoreRepository,
)
from core.store import (
    CoreStore,
    StoreError,
    account_status_event_semantic,
    compact_json,
    utc_now,
)


class TestRC14StorageDedup(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "core_test.db"
        self.store = CoreStore(self.db_path)

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_gate_t1_heartbeat_suppression(self) -> None:
        """Gate T1: 1000 unchanged ticks -> 0 events, telemetry churn -> 0, transition -> 1."""
        account_id = "test-acc-1"
        display_name = "Test Account"

        # 1. Initial creation: exactly 1 event
        status_0 = {
            "started_at": "2026-09-11T10:00:00Z",
            "finished_at": "2026-09-11T10:00:01Z",
            "elapsed_seconds": 1.0,
            "cycle_count": 1,
            "ok": True,
            "enabled": True,
        }
        runtime_0 = {
            "runtime_provider": "agent_wechat",
            "running": True,
            "logged_in": True,
            "logged_in_user": "wxid_test123",
            "pid": 1001,
        }
        self.store.upsert_account(
            account_id, display_name, state="online", runtime=runtime_0, sync=status_0
        )

        with self.store.connection() as conn:
            event_count = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
        self.assertEqual(event_count, 1, "Initial account upsert must emit exactly 1 event")

        # 2. 1000 unchanged sync ticks with telemetry/timing churn
        for i in range(2, 1002):
            status_i = {
                "started_at": f"2026-09-11T10:00:{i % 60:02d}Z",
                "finished_at": f"2026-09-11T10:00:{(i + 1) % 60:02d}Z",
                "elapsed_seconds": 0.5 + (i * 0.001),
                "elapsed_ms": 500 + i,
                "cycle_count": i,
                "last_run_at": f"2026-09-11T10:00:{i % 60:02d}Z",
                "last_completed_cycle_at": f"2026-09-11T10:00:{i % 60:02d}Z",
                "ok": True,
                "enabled": True,
            }
            runtime_i = {
                "runtime_provider": "agent_wechat",
                "running": True,
                "logged_in": True,
                "logged_in_user": "wxid_test123",
                "pid": 1001 + (i % 5),
                "sample_timestamps": [1234567890 + i],
            }
            self.store.upsert_account(
                account_id, display_name, state="online", runtime=runtime_i, sync=status_i
            )

        with self.store.connection() as conn:
            event_count_after_1000 = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
            acc_row = conn.execute("SELECT * FROM accounts WHERE account_id=?", (account_id,)).fetchone()

        self.assertEqual(
            event_count_after_1000, 1,
            "1000 semantically identical ticks with telemetry churn must emit ZERO additional events"
        )
        # Verify account row updated in-place
        self.assertIn('"cycle_count":1001', acc_row["sync_json"])

        # 3. Meaningful transition 1: state goes offline -> exactly 1 event
        self.store.upsert_account(
            account_id, display_name, state="offline", runtime=runtime_0, sync=status_0
        )
        with self.store.connection() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0], 2)

        # 4. Meaningful transition 2: logged-in WeChat identity changes -> exactly 1 event
        runtime_new_user = dict(runtime_0, logged_in_user="wxid_diff999")
        self.store.upsert_account(
            account_id, display_name, state="offline", runtime=runtime_new_user, sync=status_0
        )
        with self.store.connection() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0], 3)

        # 5. Meaningful transition 3: sync degraded -> exactly 1 event
        status_degraded = dict(status_0, degraded=True, ok=False, error="timeout connecting")
        self.store.upsert_account(
            account_id, display_name, state="offline", runtime=runtime_new_user, sync=status_degraded
        )
        with self.store.connection() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM events").fetchone()[0], 4)

    def test_gate_t3_checkpoint_monotonicity(self) -> None:
        """Gate T3: Checkpoint monotonicity, bounds validation and idempotency."""
        consumer_id = "test-consumer"

        # Emit some events
        for i in range(5):
            self.store.upsert_account(f"acc-{i}", f"Account {i}", state="online")

        poll_res = self.store.poll_events(after="0", limit=10)
        stream_head = poll_res["stream_head_cursor"]
        self.assertGreaterEqual(stream_head, 5)
        self.assertEqual(poll_res["retention_floor_cursor"], 1)

        # 1. Forward checkpoint = PASS
        cp1 = self.store.checkpoint_consumer(consumer_id, 3)
        self.assertTrue(cp1["ok"])
        self.assertEqual(cp1["processed_through_cursor"], 3)
        self.assertFalse(cp1["idempotent"])

        # 2. Same checkpoint = idempotent PASS
        cp2 = self.store.checkpoint_consumer(consumer_id, 3)
        self.assertTrue(cp2["ok"])
        self.assertTrue(cp2["idempotent"])

        # 3. Backward checkpoint = rejected
        with self.assertRaises(StoreError) as ctx:
            self.store.checkpoint_consumer(consumer_id, 2)
        self.assertEqual(ctx.exception.code, "cursor_regression")

        # 4. Checkpoint beyond stream head = rejected
        with self.assertRaises(StoreError) as ctx:
            self.store.checkpoint_consumer(consumer_id, stream_head + 100)
        self.assertEqual(ctx.exception.code, "cursor_exceeds_head")

        # 5. Checkpoint with valid last_event_id
        last_ev_id = poll_res["events"][3]["event_id"]
        last_ev_cursor = int(poll_res["events"][3]["cursor"])
        cp3 = self.store.checkpoint_consumer(
            consumer_id, last_ev_cursor, last_event_id=last_ev_id, subscription_account_id="acc-3"
        )
        self.assertTrue(cp3["ok"])

        # Verify persisted checkpoint
        saved = self.store.get_checkpoint(consumer_id)
        self.assertIsNotNone(saved)
        self.assertEqual(saved["processed_through_cursor"], last_ev_cursor)
        self.assertEqual(saved["last_event_id"], last_ev_id)
        self.assertEqual(saved["subscription_account_id"], "acc-3")

    def test_gate_t4_status_compaction_correctness_100k(self) -> None:
        """Gate T4: 100,000 synthetic status event compaction test on temporary DB."""
        # 1. Populate temporary database with 100,000 synthetic status events
        # across 2 accounts, with 10 intentional state transitions each,
        # interleaved with 50 non-status events.
        print("Generating 100,000 synthetic status events...")
        now = utc_now()
        status_events_acc1 = 50000
        status_events_acc2 = 50000

        with self.store.connection() as conn:
            conn.execute(
                "INSERT INTO accounts (account_id, display_name, state, runtime_json, sync_json, updated_at) VALUES ('acc-1', 'Account One', 'online', '{}', '{}', ?)",
                (now,),
            )
            conn.execute(
                "INSERT INTO accounts (account_id, display_name, state, runtime_json, sync_json, updated_at) VALUES ('acc-2', 'Account Two', 'online', '{}', '{}', ?)",
                (now,),
            )
            # We can insert events directly in bulk for speed
            # 50 interleaved non-status events
            non_status_events = []
            for i in range(50):
                ev_id = f"evt-msg-{i:04d}"
                occurred = f"2026-09-11T12:{i % 60:02d}:00Z"
                payload = {
                    "message": {
                        "message_id": f"msg-{i:04d}",
                        "chat_id": f"chat-{i % 5}@chatroom",
                        "text": f"Non-status message test {i}",
                        "direction": "incoming",
                    }
                }
                non_status_events.append((ev_id, "acc-1", "message.created", occurred, compact_json(payload)))

            # Acc 1 status events
            events_to_insert = []
            current_state = "online"
            for i in range(status_events_acc1):
                # Intentional transition every 5,000 ticks
                if i > 0 and i % 5000 == 0:
                    current_state = "offline" if current_state == "online" else "online"
                ev_id = f"evt-s1-{i:06d}"
                payload = {
                    "account": {
                        "account_id": "acc-1",
                        "display_name": "Account One",
                        "state": current_state,
                        "runtime": {"running": current_state == "online", "logged_in": True, "pid": 1000 + (i % 10)},
                        "sync": {"cycle_count": i, "elapsed_ms": 100 + (i % 50), "ok": True},
                    }
                }
                events_to_insert.append((ev_id, "acc-1", "account.status", now, compact_json(payload)))

            # Acc 2 status events
            current_state_2 = "online"
            for i in range(status_events_acc2):
                if i > 0 and i % 5000 == 0:
                    current_state_2 = "degraded" if current_state_2 == "online" else "online"
                ev_id = f"evt-s2-{i:06d}"
                payload = {
                    "account": {
                        "account_id": "acc-2",
                        "display_name": "Account Two",
                        "state": current_state_2,
                        "runtime": {"running": True, "logged_in": True, "cycle": i},
                        "sync": {"cycle_count": i, "elapsed_ms": 100 + (i % 50), "ok": current_state_2 == "online"},
                    }
                }
                events_to_insert.append((ev_id, "acc-2", "account.status", now, compact_json(payload)))

            # Interleave events
            all_events = []
            step = (len(events_to_insert) // len(non_status_events))
            ns_idx = 0
            for idx, item in enumerate(events_to_insert):
                all_events.append(item)
                if ns_idx < len(non_status_events) and idx % step == 0:
                    all_events.append(non_status_events[ns_idx])
                    ns_idx += 1
            while ns_idx < len(non_status_events):
                all_events.append(non_status_events[ns_idx])
                ns_idx += 1

            conn.executemany(
                "INSERT INTO events (event_id, account_id, event_type, occurred_at, payload_json) VALUES (?, ?, ?, ?, ?)",
                all_events,
            )

            # Insert some event_acks
            sample_acks = [
                ("cons-1", all_events[j][0], now) for j in range(0, len(all_events), 100)
            ]
            conn.executemany(
                "INSERT INTO event_acks (consumer_id, event_id, acknowledged_at) VALUES (?, ?, ?)",
                sample_acks,
            )

        print("Dataset generated. Planning status compaction...")
        plan = plan_status_compaction(self.store)

        self.assertEqual(plan["total_account_status_rows"], 100000)
        # Expected preserved:
        # Acc 1: first row + ~9 transitions + newest row = ~11 rows
        # Acc 2: first row + ~9 transitions + newest row = ~11 rows
        # Total preserved status rows <= 25 rows
        self.assertLessEqual(plan["total_preserved_rows"], 25)
        self.assertGreaterEqual(plan["redundant_candidate_rows"], 99975)
        self.assertGreater(plan["event_acks_rows_to_remove"], 0)

        # Snapshot non-status events before compaction
        with self.store.connection() as conn:
            non_status_before = conn.execute(
                "SELECT cursor, event_id, account_id, event_type, occurred_at, payload_json "
                "FROM events WHERE event_type != 'account.status' ORDER BY cursor ASC"
            ).fetchall()
            non_status_before_data = [dict(r) for r in non_status_before]

        print("Applying status compaction...")
        result = apply_status_compaction(self.store, plan, confirmed=True)
        self.assertTrue(result["applied"])
        self.assertEqual(result["deleted_events"], plan["redundant_candidate_rows"])
        self.assertTrue(result["foreign_key_check_clean"])

        # Post-compaction verifications:
        with self.store.connection() as conn:
            # 1. Non-status events preserved byte-for-byte with identical cursors
            non_status_after = conn.execute(
                "SELECT cursor, event_id, account_id, event_type, occurred_at, payload_json "
                "FROM events WHERE event_type != 'account.status' ORDER BY cursor ASC"
            ).fetchall()
            non_status_after_data = [dict(r) for r in non_status_after]
            self.assertEqual(non_status_before_data, non_status_after_data)

            # 2. PRAGMA foreign_key_check is clean
            fks = conn.execute("PRAGMA foreign_key_check").fetchall()
            self.assertEqual(len(fks), 0, f"Foreign key check must be completely clean, got: {fks}")

            # 3. No dangling event_acks
            dangling = conn.execute(
                "SELECT COUNT(*) FROM event_acks WHERE event_id NOT IN (SELECT event_id FROM events)"
            ).fetchone()[0]
            self.assertEqual(dangling, 0, "Zero dangling event_acks allowed")

            # 4. Status events count matches preserved plan
            remaining_status = conn.execute(
                "SELECT COUNT(*) FROM events WHERE event_type='account.status'"
            ).fetchone()[0]
            self.assertEqual(remaining_status, plan["total_preserved_rows"])

    def test_work_package_f_repository_protocols(self) -> None:
        """Work Package F: Verify repository protocol and SQLite adapter contract."""
        repo = SQLiteCoreRepository(self.store)

        # Protocol conformance
        self.assertIsInstance(repo, AccountRepositoryProtocol)
        self.assertIsInstance(repo, MessageRepositoryProtocol)
        self.assertIsInstance(repo, EventRepositoryProtocol)
        self.assertIsInstance(repo, CoreStorageRepositoryProtocol)

        caps = repo.storage_capabilities()
        self.assertEqual(caps["backend"], "sqlite")
        self.assertTrue(caps["postgres_ready"])
        self.assertTrue(caps["durable_checkpoints"])

        # Test operations through repo interface
        acc = repo.upsert_account("repo-acc", "Repo Account", state="online")
        self.assertEqual(acc["account_id"], "repo-acc")
        self.assertIsNotNone(repo.account("repo-acc"))

        polled = repo.poll_events(after="0", limit=10)
        self.assertGreater(len(polled["events"]), 0)


if __name__ == "__main__":
    unittest.main()
