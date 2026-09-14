"""RC.14 required-consumer retention governance primitive.

Machine-checkable required-consumer safety for destructive event retention
(compaction / rebuild / deletion). Governing taskbook:
``docs/RC14_OPTIONAL_CONSUMER_RETENTION_GOVERNANCE_TASKBOOK.md``.

Why this exists
---------------
The historical maintenance governance froze while ``wechat-console`` was the sole
required consumer, and ``poll_events().retention_floor_cursor`` is only the minimum
cursor physically present in ``events`` — it is NOT a computed minimum of required
consumer checkpoints. ``plan_status_compaction()`` likewise does not itself enforce
a required-consumer checkpoint ceiling. This module makes that governance
machine-checkable instead of memory-based.

Contract
--------
``safe_consumer_ceiling = min(processed_through_cursor of every required consumer)``
only when every required consumer row is present and legal. Otherwise the
evaluation fails closed and no ceiling is produced.

``effective_deletion_ceiling = min(frozen_stream_head, safe_consumer_ceiling)``

Fail closed on:
- any required consumer row missing;
- checkpoint cursor > stream head;
- checkpoint cursor regressing relative to frozen evidence;
- empty required-consumer set in a context that intends destructive retention;
- freshness policy violation where freshness is required;
- malformed checkpoint rows.

Extra non-required checkpoint rows (disposable/test consumers) are reported
informationally and NEVER enter the ceiling merely because they exist.

Read-only: this module computes over plain inputs and never writes to Core
SQLite. The CLI helpers open the Core database with SQLite ``mode=ro`` URIs only.
"""

from __future__ import annotations

import json
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence

__all__ = [
    "VERDICT_PASS",
    "VERDICT_FAIL",
    "evaluate_required_consumers",
    "effective_deletion_ceiling",
    "filter_candidates_above_ceiling",
    "load_required_consumer_config",
    "validate_required_consumer_config",
    "read_live_consumer_state",
    "build_report",
    "main",
]


VERDICT_PASS = "PASS"
VERDICT_FAIL = "FAIL"

_SECRET_KEY_PATTERN = re.compile(r"secret|password|passwd|token|api_key|apikey|credential", re.IGNORECASE)
_CONSUMER_ID_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:@-]*$")

_REQUIRED_CONSUMER_SETS_KEY = "consumer_sets"


# ---------------------------------------------------------------------------
# timestamp helpers (tolerant RFC3339 parsing; Core writes ``utc_now()`` ISO)
# ---------------------------------------------------------------------------


def _parse_epoch(value: Any) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    if text.endswith(("Z", "z")):
        text = text[:-1] + "+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.timestamp()


def _normalize_checkpoint_row(consumer_id: str, row: Mapping[str, Any]) -> dict[str, Any]:
    norm = {
        "consumer_id": consumer_id,
        "processed_through_cursor": row.get("processed_through_cursor"),
        "last_event_id": str(row.get("last_event_id") or ""),
        "subscription_account_id": str(row.get("subscription_account_id") or ""),
        "updated_at": str(row.get("updated_at") or ""),
    }
    if "bootstrap_mode" in row:
        norm["bootstrap_mode"] = str(row.get("bootstrap_mode") or "")
    if "bootstrap_source" in row:
        norm["bootstrap_source"] = str(row.get("bootstrap_source") or "")
    if "bootstrap_at" in row:
        norm["bootstrap_at"] = str(row.get("bootstrap_at") or "")
    if "initial_cursor" in row:
        norm["initial_cursor"] = row.get("initial_cursor")
    return norm


# ---------------------------------------------------------------------------
# core evaluation primitive
# ---------------------------------------------------------------------------


def evaluate_required_consumers(
    required_consumer_ids: Sequence[str],
    observed_checkpoints: Mapping[str, Mapping[str, Any]],
    stream_head: int,
    *,
    frozen_cursors: Mapping[str, int] | None = None,
    freshness: Mapping[str, Any] | None = None,
    now_epoch: float | None = None,
) -> dict[str, Any]:
    """Evaluate the explicitly configured required-consumer set (fail-closed).

    Parameters
    ----------
    required_consumer_ids:
        Explicit, stable, operator-configured consumer IDs. Never derived from
        whatever happens to be present in ``consumer_checkpoints``.
    observed_checkpoints:
        Mapping consumer_id -> durable checkpoint row (as returned by
        ``CoreStore.get_checkpoint`` / the ``consumer_checkpoints`` table).
    stream_head:
        Current frozen Core event stream head (``MAX(cursor)``).
    frozen_cursors:
        Optional mapping consumer_id -> previously sealed cursor. A checkpoint
        below its frozen cursor is a regression (fail closed).
    freshness:
        Optional ``{"required": bool, "max_age_seconds": int}``. When required,
        a checkpoint whose ``updated_at`` is older than the window is stale.
    now_epoch:
        Injection point for tests; defaults to the current time.
    """
    required = [str(cid) for cid in required_consumer_ids]
    head = int(stream_head)
    if head < 0:
        raise ValueError("stream_head must be >= 0")

    fail_reasons: list[str] = []
    missing: list[str] = []
    stale: list[str] = []
    ahead: list[str] = []
    regressed: list[str] = []
    invalid: list[str] = []
    normalized: dict[str, dict[str, Any]] = {}
    lags: dict[str, int] = {}

    if not required:
        fail_reasons.append("empty_required_consumer_set")

    for cid in required:
        row = observed_checkpoints.get(cid)
        if row is None:
            missing.append(cid)
            fail_reasons.append(f"missing_required_consumer:{cid}")
            continue
        if not isinstance(row, Mapping):
            invalid.append(cid)
            fail_reasons.append(f"invalid_checkpoint_shape:{cid}")
            continue
        cursor_raw = row.get("processed_through_cursor")
        try:
            cursor = int(cursor_raw)
        except (TypeError, ValueError):
            invalid.append(cid)
            fail_reasons.append(f"invalid_checkpoint_shape:{cid}")
            continue
        if cursor < 0:
            invalid.append(cid)
            fail_reasons.append(f"invalid_checkpoint_shape:{cid}")
            continue
        entry = _normalize_checkpoint_row(cid, row)
        entry["processed_through_cursor"] = cursor
        normalized[cid] = entry

        if cursor > head:
            ahead.append(cid)
            fail_reasons.append(f"checkpoint_ahead_of_head:{cid}")
            continue
        lags[cid] = head - cursor

        if frozen_cursors is not None and cid in frozen_cursors:
            frozen = int(frozen_cursors[cid])
            if cursor < frozen:
                regressed.append(cid)
                fail_reasons.append(f"checkpoint_regression_vs_frozen:{cid}")

        if freshness and freshness.get("required"):
            max_age = int(freshness.get("max_age_seconds") or 0)
            updated_epoch = _parse_epoch(entry["updated_at"])
            if updated_epoch is None:
                stale.append(cid)
                fail_reasons.append(f"stale_consumer_unparseable_updated_at:{cid}")
            else:
                now = now_epoch if now_epoch is not None else datetime.now(timezone.utc).timestamp()
                if max_age <= 0 or (now - updated_epoch) > max_age:
                    stale.append(cid)
                    fail_reasons.append(f"stale_consumer:{cid}")

    extra = sorted(set(observed_checkpoints) - set(required))

    if fail_reasons:
        return {
            "required_consumers": required,
            "observed_checkpoints": normalized,
            "missing_consumers": missing,
            "stale_consumers": stale,
            "ahead_consumers": ahead,
            "regressed_consumers": regressed,
            "invalid_checkpoints": invalid,
            "extra_consumers": extra,
            "stream_head": head,
            "per_consumer_lag": lags,
            "safe_consumer_ceiling": None,
            "verdict": VERDICT_FAIL,
            "fail_reasons": sorted(set(fail_reasons)),
        }

    ceiling = min(int(normalized[cid]["processed_through_cursor"]) for cid in required)
    return {
        "required_consumers": required,
        "observed_checkpoints": normalized,
        "missing_consumers": [],
        "stale_consumers": stale,
        "ahead_consumers": ahead,
        "regressed_consumers": regressed,
        "invalid_checkpoints": invalid,
        "extra_consumers": extra,
        "stream_head": head,
        "per_consumer_lag": lags,
        "safe_consumer_ceiling": ceiling,
        "verdict": VERDICT_PASS,
        "fail_reasons": [],
    }


def effective_deletion_ceiling(
    frozen_stream_head: int,
    safe_consumer_ceiling: int | None,
) -> int:
    """``effective_deletion_ceiling = min(frozen_stream_head, safe_consumer_ceiling)``.

    Fails closed (raises) when no legal ``safe_consumer_ceiling`` exists — a
    destructive retention operation must never proceed with an uncomputed
    ceiling.
    """
    head = int(frozen_stream_head)
    if head < 0:
        raise ValueError("frozen_stream_head must be >= 0")
    if safe_consumer_ceiling is None:
        raise ValueError("safe_consumer_ceiling_unavailable: required-consumer evaluation did not PASS")
    ceiling = int(safe_consumer_ceiling)
    if ceiling < 0:
        raise ValueError("safe_consumer_ceiling must be >= 0")
    if ceiling > head:
        # A required checkpoint above the frozen head is illegal upstream of
        # this function (evaluate_required_consumers fails closed); refuse here
        # too so the helper stays safe under direct use.
        raise ValueError("safe_consumer_ceiling_exceeds_frozen_stream_head")
    return min(head, ceiling)


def filter_candidates_above_ceiling(
    candidates: Iterable[Mapping[str, Any]],
    ceiling: int,
    *,
    cursor_key: str = "cursor",
) -> dict[str, Any]:
    """Split compaction candidate rows into eligible vs rejected by ceiling.

    A candidate exactly AT the ceiling is eligible (boundary inclusive);
    strictly above the ceiling is rejected. Deleting an event at cursor == the
    consumer checkpoint is safe only when the consumer has processed THROUGH
    that cursor (checkpoint semantics: everything <= cursor consumed), which is
    exactly the boundary-inclusive rule.
    """
    limit = int(ceiling)
    eligible: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for row in candidates:
        cursor = int(row[cursor_key])
        (eligible if cursor <= limit else rejected).append(dict(row))
    return {
        "ceiling": limit,
        "eligible_candidates": eligible,
        "rejected_candidates": rejected,
        "eligible_count": len(eligible),
        "rejected_count": len(rejected),
    }


# ---------------------------------------------------------------------------
# machine-readable required-consumer configuration
# ---------------------------------------------------------------------------


def validate_required_consumer_config(config: Mapping[str, Any]) -> list[str]:
    """Validate the machine-readable required-consumer governance config.

    Returns a list of human-readable errors (empty list == valid). Deliberately
    strict: stable IDs, no secrets, explicit consumer sets, activation metadata.
    """
    errors: list[str] = []
    if not isinstance(config, Mapping):
        return ["config_root_must_be_object"]
    if int(config.get("version") or 0) != 1:
        errors.append("unsupported_config_version")
    config_id = str(config.get("config_id") or "")
    if not config_id:
        errors.append("config_id_required")
    for key in config:
        if _SECRET_KEY_PATTERN.search(str(key)):
            errors.append(f"forbidden_key:{key}")
    sets = config.get(_REQUIRED_CONSUMER_SETS_KEY)
    if not isinstance(sets, Mapping) or not sets:
        errors.append("consumer_sets_required")
        sets = {}
    seen_ids: set[str] = set()
    registry = config.get("consumer_registry")
    if isinstance(registry, list):
        for entry in registry:
            if not isinstance(entry, Mapping):
                errors.append("consumer_registry_entry_must_be_object")
                continue
            cid = str(entry.get("consumer_id") or "")
            if not cid or not _CONSUMER_ID_PATTERN.match(cid):
                errors.append(f"invalid_consumer_id:{cid or '<empty>'}")
                continue
            if cid in seen_ids:
                errors.append(f"duplicate_consumer_id:{cid}")
            seen_ids.add(cid)
            for key in entry:
                if _SECRET_KEY_PATTERN.search(str(key)):
                    errors.append(f"forbidden_key:{key}")
            if str(entry.get("tier") or "") != "required":
                errors.append(f"consumer_tier_must_be_required:{cid}")
    else:
        errors.append("consumer_registry_required")
    for set_name, set_def in sets.items():
        if not isinstance(set_def, Mapping):
            errors.append(f"consumer_set_must_be_object:{set_name}")
            continue
        ids = set_def.get("consumer_ids")
        if not isinstance(ids, list) or not ids:
            errors.append(f"consumer_set_empty:{set_name}")
            continue
        for cid in ids:
            cid = str(cid)
            if not _CONSUMER_ID_PATTERN.match(cid):
                errors.append(f"invalid_consumer_id:{cid}:{set_name}")
        if len(set(ids)) != len(ids):
            errors.append(f"consumer_set_duplicate_ids:{set_name}")
    return errors


def load_required_consumer_config(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as handle:
        config = json.load(handle)
    errors = validate_required_consumer_config(config)
    if errors:
        raise ValueError("invalid_required_consumer_config: " + "; ".join(errors))
    return config


def consumer_ids_for_set(config: Mapping[str, Any], set_id: str) -> list[str]:
    sets = config.get(_REQUIRED_CONSUMER_SETS_KEY) or {}
    set_def = sets.get(set_id)
    if not isinstance(set_def, Mapping):
        raise ValueError(f"unknown_consumer_set:{set_id}")
    return [str(cid) for cid in set_def.get("consumer_ids") or []]


# ---------------------------------------------------------------------------
# read-only live state reader (Core SQLite, mode=ro only)
# ---------------------------------------------------------------------------


def read_live_consumer_state(db_path: str | Path) -> dict[str, Any]:
    """Read ``consumer_checkpoints`` + stream head from a Core SQLite file.

    Opens the database with a ``mode=ro`` URI — this function can never write.
    """
    uri = f"file:{Path(db_path).as_posix()}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    try:
        cursor = conn.cursor()
        cols = {col[1] for col in cursor.execute("PRAGMA table_info(consumer_checkpoints)").fetchall()}
        has_bootstrap = "bootstrap_mode" in cols
        if has_bootstrap:
            rows = cursor.execute(
                "SELECT consumer_id, processed_through_cursor, last_event_id, "
                "subscription_account_id, updated_at, bootstrap_mode, bootstrap_source, "
                "bootstrap_at, initial_cursor FROM consumer_checkpoints"
            ).fetchall()
        else:
            rows = cursor.execute(
                "SELECT consumer_id, processed_through_cursor, last_event_id, "
                "subscription_account_id, updated_at FROM consumer_checkpoints"
            ).fetchall()
        head_row = conn.execute("SELECT COALESCE(MAX(cursor), 0) FROM events").fetchone()
        floor_row = conn.execute("SELECT COALESCE(MIN(cursor), 0) FROM events").fetchone()
    finally:
        conn.close()

    checkpoints = {}
    for row in rows:
        cid = str(row["consumer_id"])
        item = {
            "consumer_id": cid,
            "processed_through_cursor": int(row["processed_through_cursor"]),
            "last_event_id": str(row["last_event_id"] or ""),
            "subscription_account_id": str(row["subscription_account_id"] or ""),
            "updated_at": str(row["updated_at"] or ""),
        }
        if "bootstrap_mode" in row.keys():
            item["bootstrap_mode"] = str(row["bootstrap_mode"] or "")
            item["bootstrap_source"] = str(row["bootstrap_source"] or "")
            item["bootstrap_at"] = str(row["bootstrap_at"] or "")
            item["initial_cursor"] = row["initial_cursor"]
        checkpoints[cid] = item

    return {
        "checkpoints": checkpoints,
        "stream_head": int(head_row[0] or 0),
        "retention_floor_cursor": int(floor_row[0] or 0),
    }


def build_report(
    *,
    config: Mapping[str, Any],
    set_id: str,
    live_state: Mapping[str, Any],
    frozen_cursors: Mapping[str, int] | None = None,
    freshness: Mapping[str, Any] | None = None,
    now_epoch: float | None = None,
) -> dict[str, Any]:
    """Observability report: required set, per-consumer lag, ceiling, verdict."""
    required_ids = consumer_ids_for_set(config, set_id)
    set_def = (config.get(_REQUIRED_CONSUMER_SETS_KEY) or {}).get(set_id) or {}
    evaluation = evaluate_required_consumers(
        required_ids,
        live_state.get("checkpoints") or {},
        int(live_state.get("stream_head") or 0),
        frozen_cursors=frozen_cursors,
        freshness=freshness,
        now_epoch=now_epoch,
    )
    ceiling: int | None = None
    ceiling_error = ""
    if evaluation["verdict"] == VERDICT_PASS:
        try:
            ceiling = effective_deletion_ceiling(int(live_state.get("stream_head") or 0), evaluation["safe_consumer_ceiling"])
        except ValueError as exc:
            ceiling_error = str(exc)
    return {
        "report": "required_consumer_retention_governance",
        "config_id": str(config.get("config_id") or ""),
        "consumer_set_id": set_id,
        "consumer_set_applies": str(set_def.get("applies") or ""),
        "retention_floor_cursor_note": (
            "poll_events().retention_floor_cursor is the minimum cursor physically "
            "present in events; it is NOT a required-consumer-safe deletion ceiling"
        ),
        "evaluation": evaluation,
        "effective_deletion_ceiling": ceiling,
        "effective_deletion_ceiling_error": ceiling_error,
    }


# ---------------------------------------------------------------------------
# CLI (read-only observability path; no production API mutation)
# ---------------------------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    import argparse

    parser = argparse.ArgumentParser(
        prog="python -m core.consumer_governance",
        description="Read-only required-consumer retention governance report (Core SQLite mode=ro).",
    )
    parser.add_argument("--db", required=True, help="Path to wechat_core.sqlite (opened read-only)")
    parser.add_argument("--config", required=True, help="Machine-readable required-consumer governance JSON")
    parser.add_argument(
        "--set-id",
        default="",
        help="consumer_sets entry to evaluate (e.g. current_live / post_optional_promotion)",
    )
    parser.add_argument("--json", action="store_true", help="Print machine-readable JSON report")
    args = parser.parse_args(argv)

    config = load_required_consumer_config(args.config)
    live = read_live_consumer_state(args.db)
    set_id = args.set_id or str(config.get("default_consumer_set") or "")
    if not set_id:
        print("error: no --set-id and config has no default_consumer_set", file=sys.stderr)
        return 2
    report = build_report(config=config, set_id=set_id, live_state=live)
    if args.json:
        print(json.dumps(report, indent=2, ensure_ascii=False))
    else:
        evaluation = report["evaluation"]
        print(f"consumer_set          : {set_id} ({report['consumer_set_applies']})")
        print(f"required_consumers    : {', '.join(evaluation['required_consumers'])}")
        print(f"stream_head           : {evaluation['stream_head']}")
        for cid, lag in sorted(evaluation["per_consumer_lag"].items()):
            cursor = evaluation["observed_checkpoints"][cid]["processed_through_cursor"]
            b_mode = evaluation["observed_checkpoints"][cid].get("bootstrap_mode")
            b_info = f" bootstrap={b_mode}" if b_mode else ""
            print(f"  {cid:<32} cursor={cursor} lag={lag} updated={updated}{b_info}")
        for cid in evaluation["missing_consumers"]:
            print(f"  MISSING : {cid}")
        for cid in evaluation["stale_consumers"]:
            print(f"  STALE   : {cid}")
        for cid in evaluation["ahead_consumers"]:
            print(f"  AHEAD   : {cid}")
        for cid in evaluation["regressed_consumers"]:
            print(f"  REGRESSED: {cid}")
        if evaluation["extra_consumers"]:
            print(f"extra (non-required)  : {', '.join(evaluation['extra_consumers'])}")
        print(f"safe_consumer_ceiling : {evaluation['safe_consumer_ceiling']}")
        print(f"effective_deletion_ceiling: {report['effective_deletion_ceiling']}")
        print(f"verdict               : {evaluation['verdict']}")
        if evaluation["fail_reasons"]:
            print(f"fail_reasons          : {', '.join(evaluation['fail_reasons'])}")
    return 0 if report["evaluation"]["verdict"] == VERDICT_PASS else 1


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
