"""RC.14 Required-Consumer Retention Governance tests.

Governing taskbook: docs/RC14_OPTIONAL_CONSUMER_RETENTION_GOVERNANCE_TASKBOOK.md

Covers the mandatory automated scenarios:
  1. all three consumers caught up
  2. Agent lagging            (safe ceiling = min required cursor)
  3. EFB lagging              (safe ceiling = min required cursor)
  4. missing Agent checkpoint (fail closed)
  5. missing EFB checkpoint   (fail closed)
  6. checkpoint ahead of head (fail closed)
  7. stale checkpoint         (fail closed under freshness policy)
  8. extra non-required disposable/test checkpoint row never enters the ceiling
  9. candidate exactly at ceiling is eligible (boundary inclusive)
 10. candidate above ceiling is rejected
 11. checkpoint monotonicity API regression (store.checkpoint_consumer unchanged)
 12. Contract V1 regression (poll_events response shape + payload passthrough intact;
     governance evaluation is read-only over the store)

Plus: machine-readable governance config validation and a documentation test
that the governance-repo production config matches the expected required IDs.
"""

from __future__ import annotations

import json
import sys
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from core.compaction import plan_status_compaction
from core.consumer_governance import (
    VERDICT_FAIL,
    VERDICT_PASS,
    consumer_ids_for_set,
    effective_deletion_ceiling,
    evaluate_required_consumers,
    filter_candidates_above_ceiling,
    load_required_consumer_config,
    read_live_consumer_state,
    validate_required_consumer_config,
)
from core.store import CoreStore, StoreError, compact_json

CONSOLE_ID = "wechat-console"
AGENT_ID = "wechat-agent"
EFB_ID = "efb-linux-wechat:wechat.linux"
TEST_CONSUMER_ID = "disposable-test-consumer"
REQUIRED_THREE = [CONSOLE_ID, AGENT_ID, EFB_ID]


def _epoch(year: int, month: int, day: int, hour: int, minute: int = 0) -> float:
    return datetime(year, month, day, hour, minute, tzinfo=timezone.utc).timestamp()


def _observed(cursors: dict[str, int], updated_at: str = "2026-09-13T14:00:00Z") -> dict:
    observed = {}
    for cid, cursor in cursors.items():
        observed[cid] = {
            "consumer_id": cid,
            "processed_through_cursor": cursor,
            "last_event_id": "",
            "subscription_account_id": "",
            "updated_at": updated_at,
        }
    return observed


class TestRequiredConsumerGovernancePrimitive(unittest.TestCase):
    """Pure-logic evaluation of the required-consumer set (fail-closed)."""

    def test_1_all_required_consumers_caught_up(self) -> None:
        result = evaluate_required_consumers(
            REQUIRED_THREE, _observed({CONSOLE_ID: 100, AGENT_ID: 100, EFB_ID: 100}), 100
        )
        self.assertEqual(result["verdict"], VERDICT_PASS)
        self.assertEqual(result["fail_reasons"], [])
        self.assertEqual(result["safe_consumer_ceiling"], 100)
        self.assertEqual(result["per_consumer_lag"], {CONSOLE_ID: 0, AGENT_ID: 0, EFB_ID: 0})

    def test_2_agent_lagging_yields_safe_ceiling(self) -> None:
        result = evaluate_required_consumers(
            REQUIRED_THREE, _observed({CONSOLE_ID: 100, AGENT_ID: 80, EFB_ID: 100}), 100
        )
        self.assertEqual(result["verdict"], VERDICT_PASS)
        self.assertEqual(result["safe_consumer_ceiling"], 80)
        self.assertEqual(result["per_consumer_lag"][AGENT_ID], 20)
        self.assertEqual(result["per_consumer_lag"][CONSOLE_ID], 0)

    def test_3_efb_lagging_yields_safe_ceiling(self) -> None:
        result = evaluate_required_consumers(
            REQUIRED_THREE, _observed({CONSOLE_ID: 100, AGENT_ID: 100, EFB_ID: 55}), 100
        )
        self.assertEqual(result["verdict"], VERDICT_PASS)
        self.assertEqual(result["safe_consumer_ceiling"], 55)
        self.assertEqual(result["per_consumer_lag"][EFB_ID], 45)

    def test_4_missing_agent_checkpoint_fail_closed(self) -> None:
        result = evaluate_required_consumers(
            REQUIRED_THREE, _observed({CONSOLE_ID: 100, EFB_ID: 100}), 100
        )
        self.assertEqual(result["verdict"], VERDICT_FAIL)
        self.assertIn(AGENT_ID, result["missing_consumers"])
        self.assertIn(f"missing_required_consumer:{AGENT_ID}", result["fail_reasons"])
        self.assertIsNone(result["safe_consumer_ceiling"])

    def test_5_missing_efb_checkpoint_fail_closed(self) -> None:
        result = evaluate_required_consumers(
            REQUIRED_THREE, _observed({CONSOLE_ID: 100, AGENT_ID: 100}), 100
        )
        self.assertEqual(result["verdict"], VERDICT_FAIL)
        self.assertIn(EFB_ID, result["missing_consumers"])
        self.assertIsNone(result["safe_consumer_ceiling"])

    def test_6_checkpoint_ahead_of_head_fail_closed(self) -> None:
        result = evaluate_required_consumers(
            REQUIRED_THREE, _observed({CONSOLE_ID: 100, AGENT_ID: 100, EFB_ID: 120}), 100
        )
        self.assertEqual(result["verdict"], VERDICT_FAIL)
        self.assertIn(EFB_ID, result["ahead_consumers"])
        self.assertIn(f"checkpoint_ahead_of_head:{EFB_ID}", result["fail_reasons"])
        self.assertIsNone(result["safe_consumer_ceiling"])

    def test_7_stale_checkpoint_fail_closed_under_freshness_policy(self) -> None:
        # All checkpoints updated at 10:00Z; now is 14:20Z with a 1h window.
        observed = _observed(
            {CONSOLE_ID: 100, AGENT_ID: 100, EFB_ID: 100},
            updated_at="2026-09-13T10:00:00Z",
        )
        result = evaluate_required_consumers(
            REQUIRED_THREE,
            observed,
            100,
            freshness={"required": True, "max_age_seconds": 3600},
            now_epoch=_epoch(2026, 9, 13, 14, 20),
        )
        self.assertEqual(result["verdict"], VERDICT_FAIL)
        self.assertEqual(sorted(result["stale_consumers"]), sorted(REQUIRED_THREE))
        self.assertIsNone(result["safe_consumer_ceiling"])

    def test_7b_fresh_checkpoint_passes_freshness_policy(self) -> None:
        observed = _observed(
            {CONSOLE_ID: 100, AGENT_ID: 100, EFB_ID: 100},
            updated_at="2026-09-13T14:00:00Z",
        )
        result = evaluate_required_consumers(
            REQUIRED_THREE,
            observed,
            100,
            freshness={"required": True, "max_age_seconds": 3600},
            now_epoch=_epoch(2026, 9, 13, 14, 0),
        )
        self.assertEqual(result["verdict"], VERDICT_PASS)
        self.assertEqual(result["stale_consumers"], [])
        self.assertEqual(result["safe_consumer_ceiling"], 100)

    def test_8_extra_non_required_consumer_never_enters_ceiling(self) -> None:
        # Disposable/test consumer sits BELOW every required cursor; it must not
        # drag the ceiling down, and its presence must be reported, not adopted.
        observed = _observed({CONSOLE_ID: 100, AGENT_ID: 100, EFB_ID: 90, TEST_CONSUMER_ID: 5})
        result = evaluate_required_consumers(REQUIRED_THREE, observed, 100)
        self.assertEqual(result["verdict"], VERDICT_PASS)
        self.assertEqual(result["safe_consumer_ceiling"], 90)
        self.assertIn(TEST_CONSUMER_ID, result["extra_consumers"])
        self.assertNotIn(TEST_CONSUMER_ID, result["observed_checkpoints"])

    def test_9_extra_consumer_with_higher_cursor_also_ignored(self) -> None:
        observed = _observed({CONSOLE_ID: 100, AGENT_ID: 100, EFB_ID: 90, TEST_CONSUMER_ID: 500})
        result = evaluate_required_consumers(REQUIRED_THREE, observed, 100)
        self.assertEqual(result["verdict"], VERDICT_PASS)
        self.assertEqual(result["safe_consumer_ceiling"], 90)

    def test_10_empty_required_set_fail_closed(self) -> None:
        result = evaluate_required_consumers([], _observed({CONSOLE_ID: 100}), 100)
        self.assertEqual(result["verdict"], VERDICT_FAIL)
        self.assertIn("empty_required_consumer_set", result["fail_reasons"])
        self.assertIsNone(result["safe_consumer_ceiling"])

    def test_11_checkpoint_regression_vs_frozen_evidence_fail_closed(self) -> None:
        observed = _observed({CONSOLE_ID: 100, AGENT_ID: 95, EFB_ID: 100})
        result = evaluate_required_consumers(
            REQUIRED_THREE, observed, 100, frozen_cursors={AGENT_ID: 97}
        )
        self.assertEqual(result["verdict"], VERDICT_FAIL)
        self.assertIn(AGENT_ID, result["regressed_consumers"])
        self.assertIn(f"checkpoint_regression_vs_frozen:{AGENT_ID}", result["fail_reasons"])

    def test_12_malformed_checkpoint_fail_closed(self) -> None:
        observed = _observed({CONSOLE_ID: 100, AGENT_ID: 100})
        observed[EFB_ID] = {"consumer_id": EFB_ID, "processed_through_cursor": "not-an-int"}
        result = evaluate_required_consumers(REQUIRED_THREE, observed, 100)
        self.assertEqual(result["verdict"], VERDICT_FAIL)
        self.assertIn(EFB_ID, result["invalid_checkpoints"])


class TestEffectiveDeletionCeiling(unittest.TestCase):
    def test_boundary_candidate_at_ceiling_is_eligible(self) -> None:
        candidates = [{"event_id": "e1", "cursor": 60}, {"event_id": "e2", "cursor": 61}]
        outcome = filter_candidates_above_ceiling(candidates, 60)
        self.assertEqual([row["event_id"] for row in outcome["eligible_candidates"]], ["e1"])
        self.assertEqual([row["event_id"] for row in outcome["rejected_candidates"]], ["e2"])

    def test_candidate_above_ceiling_rejected(self) -> None:
        candidates = [{"cursor": 10}, {"cursor": 99}, {"cursor": 100}]
        outcome = filter_candidates_above_ceiling(candidates, 99)
        self.assertEqual([row["cursor"] for row in outcome["eligible_candidates"]], [10, 99])
        self.assertEqual([row["cursor"] for row in outcome["rejected_candidates"]], [100])

    def test_effective_ceiling_is_min_of_frozen_head_and_safe_ceiling(self) -> None:
        self.assertEqual(effective_deletion_ceiling(100, 80), 80)
        self.assertEqual(effective_deletion_ceiling(100, 100), 100)
        self.assertEqual(effective_deletion_ceiling(80, 80), 80)

    def test_effective_ceiling_fails_closed_without_safe_ceiling(self) -> None:
        with self.assertRaises(ValueError):
            effective_deletion_ceiling(100, None)

    def test_effective_ceiling_refuses_ceiling_above_frozen_head(self) -> None:
        # A safe ceiling above the frozen head implies a required checkpoint
        # ahead of the stream — an illegal state that must fail closed here too.
        with self.assertRaises(ValueError):
            effective_deletion_ceiling(70, 80)


class TestCheckpointApiRegression(unittest.TestCase):
    """Regression: current checkpoint monotonicity API must stay unchanged."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.store = CoreStore(Path(self.temp_dir.name) / "core_test.db")
        self.store.upsert_account("acc-1", "Account 1", state="online")
        for _ in range(5):
            self.store.record_identity_event("acc-1", "message.created", {"message": {"text": "x"}})

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_forward_and_idempotent_checkpoint_accepted(self) -> None:
        first = self.store.checkpoint_consumer(CONSOLE_ID, 3)
        self.assertTrue(first["ok"])
        self.assertEqual(first["processed_through_cursor"], 3)
        again = self.store.checkpoint_consumer(CONSOLE_ID, 3)
        self.assertTrue(again["ok"])
        self.assertTrue(again["idempotent"])

    def test_backward_checkpoint_rejected(self) -> None:
        self.store.checkpoint_consumer(CONSOLE_ID, 4)
        with self.assertRaises(StoreError) as ctx:
            self.store.checkpoint_consumer(CONSOLE_ID, 3)
        self.assertEqual(ctx.exception.code, "cursor_regression")

    def test_checkpoint_beyond_head_rejected(self) -> None:
        # head = 6 (1 account.status from upsert_account + 5 message.created)
        with self.assertRaises(StoreError) as ctx:
            self.store.checkpoint_consumer(CONSOLE_ID, 7)
        self.assertEqual(ctx.exception.code, "cursor_exceeds_head")

    def test_get_checkpoint_shape_unchanged(self) -> None:
        self.store.checkpoint_consumer(AGENT_ID, 2)
        row = self.store.get_checkpoint(AGENT_ID)
        self.assertIsNotNone(row)
        self.assertEqual(
            sorted(row.keys()),
            ["consumer_id", "last_event_id", "processed_through_cursor", "subscription_account_id", "updated_at"],
        )
        self.assertEqual(row["processed_through_cursor"], 2)

    def test_governance_evaluation_over_live_store_state(self) -> None:
        self.store.checkpoint_consumer(CONSOLE_ID, 6)
        self.store.checkpoint_consumer(AGENT_ID, 5)
        self.store.bootstrap_consumer(EFB_ID, mode="at_head")
        self.store.checkpoint_consumer(EFB_ID, 6)
        live = read_live_consumer_state(self.store.db_path)
        self.assertEqual(live["stream_head"], 6)
        result = evaluate_required_consumers(REQUIRED_THREE, live["checkpoints"], live["stream_head"])
        self.assertEqual(result["verdict"], VERDICT_PASS)
        self.assertEqual(result["safe_consumer_ceiling"], 5)
        # Frozen evidence at the same cursor stays monotonic => PASS.
        result_frozen_ok = evaluate_required_consumers(
            REQUIRED_THREE, live["checkpoints"], live["stream_head"], frozen_cursors={AGENT_ID: 5}
        )
        self.assertEqual(result_frozen_ok["verdict"], VERDICT_PASS)


class TestContractV1Regression(unittest.TestCase):
    """Contract V1 regression: event payload passthrough intact; governance read-only."""

    CONTRACT_V1_POLL_KEYS = {
        "events",
        "next_cursor",
        "has_more",
        "stream_head_cursor",
        "retention_floor_cursor",
    }
    CONTRACT_V1_EVENT_KEYS = {
        "event_id",
        "cursor",
        "account_id",
        "event_type",
        "occurred_at",
        "payload",
    }

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "core_test.db"
        self.store = CoreStore(self.db_path)
        self.store.upsert_account("acc-1", "Account 1", state="online")

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def test_poll_events_shape_and_payload_passthrough_unchanged(self) -> None:
        payload = {"message": {"message_id": "m-1", "chat_id": "chat-1", "text": "hello", "type": "text"}}
        appended = self.store.record_identity_event("acc-1", "message.created", payload)
        page = self.store.poll_events(after="0", limit=50)
        self.assertEqual(set(page.keys()), self.CONTRACT_V1_POLL_KEYS)
        event = next(e for e in page["events"] if e["event_id"] == appended["event_id"])
        self.assertEqual(set(event.keys()), self.CONTRACT_V1_EVENT_KEYS)
        self.assertEqual(event["payload"], payload)

    def test_governance_evaluation_is_read_only_over_the_store(self) -> None:
        self.store.record_identity_event("acc-1", "message.created", {"message": {"text": "t"}})
        self.store.checkpoint_consumer(CONSOLE_ID, 1)
        self.store.checkpoint_consumer(AGENT_ID, 1)
        self.store.bootstrap_consumer(EFB_ID, mode="bounded_window", window={"events": 1}, operator_token="test-token")
        self.store.checkpoint_consumer(EFB_ID, 1)
        before = self.db_path.read_bytes()
        live = read_live_consumer_state(self.db_path)
        evaluate_required_consumers(REQUIRED_THREE, live["checkpoints"], live["stream_head"])
        after = self.db_path.read_bytes()
        self.assertEqual(before, after, "governance evaluation must never write to Core SQLite")


class TestCompactionCeilingIntegration(unittest.TestCase):
    """Integration: plan_status_compaction candidates constrained by required-consumer ceiling."""

    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "core_test.db"
        self.store = CoreStore(self.db_path)
        now = "2026-09-13T10:00:00Z"
        with self.store.connection() as conn:
            conn.execute(
                "INSERT INTO accounts (account_id, display_name, state, runtime_json, sync_json, updated_at) "
                "VALUES ('acc-1', 'Account 1', 'online', '{}', '{}', ?)",
                (now,),
            )
            # 30 legacy status events: state changes every 5 rows (6 runs), so the
            # 3 middle rows of each run are redundant compaction candidates —
            # mirroring pre-hardening data that plan_status_compaction targets.
            states = ["online", "away", "busy", "offline", "online", "away"]
            for i in range(30):
                state = states[i // 5]
                payload = {"account": {"account_id": "acc-1", "display_name": "Account 1", "state": state, "runtime": {}, "sync": {}}}
                conn.execute(
                    "INSERT INTO events (event_id, account_id, event_type, occurred_at, payload_json) VALUES (?, ?, ?, ?, ?)",
                    (f"evt-status-{i:03d}", "acc-1", "account.status", now, compact_json(payload)),
                )
            # 20 unconditional message events => deterministic headroom above candidates
            for i in range(20):
                payload = {"message": {"message_id": f"m-{i}", "chat_id": "chat-1", "text": f"m{i}"}}
                conn.execute(
                    "INSERT INTO events (event_id, account_id, event_type, occurred_at, payload_json) VALUES (?, ?, ?, ?, ?)",
                    (f"evt-msg-{i:03d}", "acc-1", "message.created", now, compact_json(payload)),
                )

    def tearDown(self) -> None:
        self.temp_dir.cleanup()

    def _candidate_cursors(self) -> list[int]:
        plan = plan_status_compaction(self.store)
        self.assertGreater(plan["redundant_candidate_rows"], 0)
        event_ids = plan["candidate_event_ids"]
        with self.store.connection() as conn:
            cursor_by_event = {
                row["event_id"]: int(row["cursor"]) for row in conn.execute("SELECT event_id, cursor FROM events")
            }
        return sorted(cursor_by_event[event_id] for event_id in event_ids)

    def test_plan_candidates_constrained_by_required_consumer_ceiling(self) -> None:
        live = read_live_consumer_state(self.db_path)
        head = live["stream_head"]
        candidates = self._candidate_cursors()
        self.assertTrue(candidates)
        # Pin the EFB checkpoint exactly at the median candidate cursor: some
        # candidates end up below/at the ceiling and some strictly above.
        efb_cursor = candidates[len(candidates) // 2]
        self.assertGreater(efb_cursor, 3)
        self.store.checkpoint_consumer(CONSOLE_ID, head)
        self.store.checkpoint_consumer(AGENT_ID, head)
        self.store.bootstrap_consumer(EFB_ID, mode="bounded_window", window={"events": head - efb_cursor}, operator_token="test-token")
        self.store.checkpoint_consumer(EFB_ID, efb_cursor)
        # disposable/test consumer far behind — must NOT constrain the ceiling
        self.store.checkpoint_consumer(TEST_CONSUMER_ID, 3)

        live = read_live_consumer_state(self.db_path)
        self.assertIn(TEST_CONSUMER_ID, live["checkpoints"])
        result = evaluate_required_consumers(REQUIRED_THREE, live["checkpoints"], live["stream_head"])
        self.assertEqual(result["verdict"], VERDICT_PASS)
        self.assertEqual(result["safe_consumer_ceiling"], efb_cursor)
        self.assertIn(TEST_CONSUMER_ID, result["extra_consumers"])

        rows = [{"event_id": f"e{cursor}", "cursor": cursor} for cursor in candidates]
        outcome = filter_candidates_above_ceiling(rows, efb_cursor)
        eligible_cursors = [row["cursor"] for row in outcome["eligible_candidates"]]
        rejected_cursors = [row["cursor"] for row in outcome["rejected_candidates"]]
        self.assertTrue(eligible_cursors)
        self.assertTrue(rejected_cursors)
        self.assertLessEqual(max(eligible_cursors), efb_cursor)
        self.assertTrue(all(cursor > efb_cursor for cursor in rejected_cursors))
        # exact-boundary candidate at the ceiling must be eligible, not rejected
        self.assertIn(efb_cursor, eligible_cursors)

        effective = effective_deletion_ceiling(live["stream_head"], efb_cursor)
        self.assertEqual(effective, min(live["stream_head"], efb_cursor))
        # disposable test consumer at cursor 3 must never influence the ceiling
        self.assertGreater(effective, 3)


def _find_governance_config() -> Path | None:
    here = Path(__file__).resolve()
    for parent in here.parents:
        candidate = parent / "release" / "required-consumers.production.json"
        if candidate.is_file():
            return candidate
    return None


class TestMachineReadableGovernanceConfig(unittest.TestCase):
    """Config schema + documentation checks (production required IDs)."""

    FIXTURE_CONFIG = {
        "version": 1,
        "config_id": "wechat-hub-production-required-consumers",
        "scope": "wechat-hub-f-live",
        "governing_taskbook": "docs/RC14_OPTIONAL_CONSUMER_RETENTION_GOVERNANCE_TASKBOOK.md",
        "updated_utc": "2026-09-13T14:30:00Z",
        "production_activation": "NOT_AUTHORIZED",
        "consumer_registry": [
            {"consumer_id": CONSOLE_ID, "service": "wechat-hub-console", "tier": "required", "status": "active"},
            {
                "consumer_id": AGENT_ID,
                "service": "wechat-hub-agent (standalone optional service)",
                "tier": "required",
                "status": "pending_activation",
            },
            {
                "consumer_id": EFB_ID,
                "service": "efb-multi (optional service)",
                "tier": "required",
                "status": "pending_activation_id_confirmation",
            },
        ],
        "consumer_sets": {
            "current_live": {"consumer_ids": [CONSOLE_ID]},
            "post_optional_promotion": {"consumer_ids": [CONSOLE_ID, AGENT_ID, EFB_ID]},
        },
        "default_consumer_set": "current_live",
    }

    def test_config_validates_and_exposes_expected_ids(self) -> None:
        self.assertEqual(validate_required_consumer_config(self.FIXTURE_CONFIG), [])
        self.assertEqual(consumer_ids_for_set(self.FIXTURE_CONFIG, "current_live"), [CONSOLE_ID])
        self.assertEqual(
            consumer_ids_for_set(self.FIXTURE_CONFIG, "post_optional_promotion"), REQUIRED_THREE
        )

    def test_config_rejects_secret_material_and_bad_ids(self) -> None:
        bad = dict(self.FIXTURE_CONFIG)
        bad["wechat_api_secret"] = "hunter2"
        self.assertTrue(any("forbidden_key" in err for err in validate_required_consumer_config(bad)))
        bad2 = json.loads(json.dumps(self.FIXTURE_CONFIG))
        bad2["consumer_registry"][0]["consumer_id"] = "bad id with spaces"
        self.assertTrue(
            any("invalid_consumer_id" in err for err in validate_required_consumer_config(bad2))
        )

    def test_load_rejects_invalid_config(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "broken.json"
            broken = dict(self.FIXTURE_CONFIG)
            broken["consumer_sets"] = {}
            path.write_text(json.dumps(broken), encoding="utf-8")
            with self.assertRaises(ValueError):
                load_required_consumer_config(path)

    def test_governance_repo_production_config_matches_expected_ids(self) -> None:
        """Documentation test: the governance repo's released config must keep the
        production required IDs aligned with this lane's sealed expectation."""
        candidate = _find_governance_config()
        if candidate is None:
            self.skipTest("governance repo release/required-consumers.production.json not reachable")
        config = load_required_consumer_config(candidate)
        self.assertEqual(
            consumer_ids_for_set(config, "post_optional_promotion"), REQUIRED_THREE
        )
        registry_ids = [entry["consumer_id"] for entry in config["consumer_registry"]]
        for cid in REQUIRED_THREE:
            self.assertIn(cid, registry_ids)
        efb_entry = next(e for e in config["consumer_registry"] if e["consumer_id"] == EFB_ID)
        self.assertEqual(efb_entry.get("status"), "pending_activation_id_confirmation")
        self.assertIn("confirm", str(efb_entry.get("id_confirmation", "")))
        self.assertEqual(config.get("production_activation"), "NOT_AUTHORIZED")


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
