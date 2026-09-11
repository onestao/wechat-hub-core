"""Historical account.status event deduplication and compaction primitive (RC.14).

This module implements:
1. Dry-run status compaction planner (plan_status_compaction)
2. Safe apply primitive on local/temporary test databases (apply_status_compaction)

CRITICAL INVARIANTS:
- Preserves the first status event per account.
- Preserves every transition event where semantic fingerprint changes.
- Preserves the newest status event per account.
- Preserves all non-status events byte-for-byte.
- Never rewrites event cursors.
- Deletes matching event_acks before events to preserve referential integrity.
- Never runs automatically; requires explicit confirmation.
- Live apply is strictly prohibited by authorization.
"""

from __future__ import annotations

import sqlite3
from typing import Any, Sequence

from .store import CoreStore, account_status_event_semantic, parse_json


CHUNK_SIZE = 500


def _get_connection(target: CoreStore | sqlite3.Connection) -> sqlite3.Connection:
    if isinstance(target, CoreStore):
        return target.connect()
    return target


def plan_status_compaction(
    target: CoreStore | sqlite3.Connection,
    *,
    account_ids: Sequence[str] | None = None,
) -> dict[str, Any]:
    """Analyze historical account.status events and compute dry-run deduplication plan."""
    conn = _get_connection(target)
    should_close = isinstance(target, CoreStore)
    try:
        query = (
            "SELECT cursor, event_id, account_id, occurred_at, payload_json "
            "FROM events WHERE event_type='account.status'"
        )
        params: list[Any] = []
        if account_ids:
            placeholders = ",".join("?" for _ in account_ids)
            query += f" AND account_id IN ({placeholders})"
            params.extend(account_ids)
        query += " ORDER BY account_id, cursor ASC"

        rows = conn.execute(query, tuple(params)).fetchall()

        events_by_account: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            acc = row["account_id"]
            if acc not in events_by_account:
                events_by_account[acc] = []
            events_by_account[acc].append(
                {
                    "cursor": int(row["cursor"]),
                    "event_id": str(row["event_id"]),
                    "account_id": acc,
                    "occurred_at": str(row["occurred_at"]),
                    "payload": parse_json(row["payload_json"], {}),
                    "raw_bytes": len(str(row["payload_json"]).encode("utf-8")) + len(str(row["event_id"])),
                }
            )

        candidate_event_ids: list[str] = []
        candidate_cursors: list[int] = []
        preserved_first_count = 0
        preserved_transition_count = 0
        preserved_newest_count = 0
        total_status_rows = len(rows)
        reclaimable_bytes = 0

        for acc, ev_list in events_by_account.items():
            if not ev_list:
                continue
            if len(ev_list) == 1:
                preserved_first_count += 1
                continue

            first_ev = ev_list[0]
            newest_ev = ev_list[-1]
            preserved_first_count += 1

            prev_semantic = account_status_event_semantic(first_ev["payload"].get("account"))

            for ev in ev_list[1:-1]:
                curr_semantic = account_status_event_semantic(ev["payload"].get("account"))
                if curr_semantic != prev_semantic:
                    preserved_transition_count += 1
                    prev_semantic = curr_semantic
                else:
                    candidate_event_ids.append(ev["event_id"])
                    candidate_cursors.append(ev["cursor"])
                    reclaimable_bytes += ev["raw_bytes"]

            # Newest event is always preserved per account
            preserved_newest_count += 1

        # Check matching event_acks count
        event_acks_count = 0
        for i in range(0, len(candidate_event_ids), CHUNK_SIZE):
            chunk = candidate_event_ids[i : i + CHUNK_SIZE]
            ph = ",".join("?" for _ in chunk)
            c = conn.execute(f"SELECT COUNT(*) FROM event_acks WHERE event_id IN ({ph})", tuple(chunk)).fetchone()[0]
            event_acks_count += int(c)

        min_cursor = min(candidate_cursors) if candidate_cursors else 0
        max_cursor = max(candidate_cursors) if candidate_cursors else 0

        return {
            "total_account_status_rows": total_status_rows,
            "redundant_candidate_rows": len(candidate_event_ids),
            "preserved_first_rows": preserved_first_count,
            "preserved_transition_rows": preserved_transition_count,
            "preserved_newest_rows": preserved_newest_count,
            "total_preserved_rows": total_status_rows - len(candidate_event_ids),
            "candidate_event_ids": candidate_event_ids,
            "candidate_cursor_range": {"min_cursor": min_cursor, "max_cursor": max_cursor},
            "event_acks_rows_to_remove": event_acks_count,
            "estimated_reclaimable_logical_bytes": reclaimable_bytes,
        }
    finally:
        if should_close:
            conn.close()


def apply_status_compaction(
    target: CoreStore | sqlite3.Connection,
    plan: dict[str, Any],
    *,
    confirmed: bool = False,
) -> dict[str, Any]:
    """Execute removal of planned redundant status events and their acks.

    MUST ONLY BE RUN ON TEST/TEMPORARY DATABASES.
    Requires confirmed=True.
    """
    if not confirmed:
        raise ValueError("Compaction apply requires explicit confirmed=True parameter")

    candidate_ids = plan.get("candidate_event_ids") or []
    if not candidate_ids:
        return {
            "applied": True,
            "deleted_events": 0,
            "deleted_event_acks": 0,
            "foreign_key_check_clean": True,
        }

    conn = _get_connection(target)
    should_close = isinstance(target, CoreStore)
    try:
        deleted_events = 0
        deleted_acks = 0

        with conn:
            for i in range(0, len(candidate_ids), CHUNK_SIZE):
                chunk = candidate_ids[i : i + CHUNK_SIZE]
                ph = ",".join("?" for _ in chunk)
                cur_acks = conn.execute(f"DELETE FROM event_acks WHERE event_id IN ({ph})", tuple(chunk))
                deleted_acks += cur_acks.rowcount
                cur_ev = conn.execute(f"DELETE FROM events WHERE event_id IN ({ph})", tuple(chunk))
                deleted_events += cur_ev.rowcount

            # Verify foreign keys
            fk_violations = conn.execute("PRAGMA foreign_key_check").fetchall()
            if fk_violations:
                raise RuntimeError(f"foreign_key_check failed after compaction: {fk_violations}")

        return {
            "applied": True,
            "deleted_events": deleted_events,
            "deleted_event_acks": deleted_acks,
            "foreign_key_check_clean": True,
        }
    finally:
        if should_close:
            conn.close()
