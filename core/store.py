"""Normalized, durable account-aware Core SQLite store and event log."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import sqlite3
import threading
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable


MAX_INLINE_MEDIA_BYTES = 20 * 1024 * 1024


class StoreError(RuntimeError):
    def __init__(self, code: str, message: str, *, status: int = 400, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.status = status
        self.details = details or {}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def compact_json(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def parse_json(value: str | None, default: Any) -> Any:
    if not value:
        return default
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return default


def digest(value: Any) -> str:
    return hashlib.sha256(compact_json(value).encode("utf-8")).hexdigest()


def stable_event_value(value: Any) -> Any:
    """Remove per-run timing fields before comparing account status events."""
    if isinstance(value, dict):
        return {
            key: stable_event_value(item)
            for key, item in value.items()
            if key not in {"started_at", "finished_at", "elapsed_seconds"}
        }
    if isinstance(value, list):
        return [stable_event_value(item) for item in value]
    return value


def clean_filename(value: str) -> str:
    name = Path(value or "upload.bin").name.replace("\x00", "")
    return name or "upload.bin"


class CoreStore:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self._transaction_state = threading.local()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.init_schema()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    @contextmanager
    def connection(self):
        active = getattr(self._transaction_state, "connection", None)
        if active is not None:
            yield active
            return
        conn = self.connect()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    @contextmanager
    def transaction(self):
        """Reuse one connection and commit for a thread-local batch of store operations."""
        active = getattr(self._transaction_state, "connection", None)
        if active is not None:
            yield active
            return
        conn = self.connect()
        self._transaction_state.connection = conn
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            del self._transaction_state.connection
            conn.close()

    def init_schema(self) -> None:
        with self.connection() as conn:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;

                CREATE TABLE IF NOT EXISTS accounts (
                    account_id TEXT PRIMARY KEY,
                    display_name TEXT NOT NULL,
                    state TEXT NOT NULL,
                    runtime_json TEXT NOT NULL DEFAULT '{}',
                    sync_json TEXT NOT NULL DEFAULT '{}',
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS chats (
                    account_id TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    type TEXT NOT NULL,
                    display_name TEXT NOT NULL,
                    alias TEXT NOT NULL DEFAULT '',
                    member_count INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    vendor_json TEXT NOT NULL DEFAULT '{}',
                    digest TEXT NOT NULL,
                    PRIMARY KEY (account_id, chat_id),
                    FOREIGN KEY (account_id) REFERENCES accounts(account_id)
                );
                CREATE INDEX IF NOT EXISTS idx_chats_account_updated ON chats(account_id, updated_at DESC, chat_id);

                CREATE TABLE IF NOT EXISTS contacts (
                    account_id TEXT NOT NULL,
                    member_id TEXT NOT NULL,
                    display_name TEXT NOT NULL,
                    alias TEXT NOT NULL DEFAULT '',
                    remark TEXT NOT NULL DEFAULT '',
                    updated_at TEXT NOT NULL,
                    digest TEXT NOT NULL,
                    PRIMARY KEY (account_id, member_id),
                    FOREIGN KEY (account_id) REFERENCES accounts(account_id)
                );

                CREATE TABLE IF NOT EXISTS chat_members (
                    account_id TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    member_id TEXT NOT NULL,
                    display_name TEXT NOT NULL,
                    alias TEXT NOT NULL DEFAULT '',
                    is_self INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    digest TEXT NOT NULL,
                    PRIMARY KEY (account_id, chat_id, member_id),
                    FOREIGN KEY (account_id, chat_id) REFERENCES chats(account_id, chat_id)
                );
                CREATE INDEX IF NOT EXISTS idx_members_account_chat ON chat_members(account_id, chat_id);

                CREATE TABLE IF NOT EXISTS messages (
                    account_id TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    type TEXT NOT NULL,
                    direction TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    author_json TEXT NOT NULL,
                    text TEXT NOT NULL DEFAULT '',
                    media_id TEXT NOT NULL DEFAULT '',
                    filename TEXT NOT NULL DEFAULT '',
                    mime_type TEXT NOT NULL DEFAULT '',
                    target_message_id TEXT NOT NULL DEFAULT '',
                    substitutions_json TEXT NOT NULL DEFAULT '[]',
                    attributes_json TEXT NOT NULL DEFAULT '{}',
                    vendor_json TEXT NOT NULL DEFAULT '{}',
                    digest TEXT NOT NULL,
                    PRIMARY KEY (account_id, message_id),
                    FOREIGN KEY (account_id, chat_id) REFERENCES chats(account_id, chat_id)
                );
                CREATE INDEX IF NOT EXISTS idx_messages_account_chat_time ON messages(account_id, chat_id, created_at, message_id);

                CREATE TABLE IF NOT EXISTS media (
                    account_id TEXT NOT NULL,
                    media_id TEXT NOT NULL,
                    filename TEXT NOT NULL DEFAULT '',
                    mime_type TEXT NOT NULL DEFAULT 'application/octet-stream',
                    local_path TEXT NOT NULL,
                    disposition TEXT NOT NULL DEFAULT 'inline',
                    status TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    digest TEXT NOT NULL,
                    PRIMARY KEY (account_id, media_id),
                    FOREIGN KEY (account_id) REFERENCES accounts(account_id)
                );

                CREATE TABLE IF NOT EXISTS events (
                    cursor INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT NOT NULL UNIQUE,
                    account_id TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    occurred_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    FOREIGN KEY (account_id) REFERENCES accounts(account_id)
                );
                CREATE INDEX IF NOT EXISTS idx_events_account_cursor ON events(account_id, cursor);

                CREATE TABLE IF NOT EXISTS event_acks (
                    consumer_id TEXT NOT NULL,
                    event_id TEXT NOT NULL,
                    acknowledged_at TEXT NOT NULL,
                    PRIMARY KEY (consumer_id, event_id),
                    FOREIGN KEY (event_id) REFERENCES events(event_id)
                );

                CREATE TABLE IF NOT EXISTS outbox (
                    send_id TEXT PRIMARY KEY,
                    idempotency_key TEXT UNIQUE,
                    kind TEXT NOT NULL,
                    account_id TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    status TEXT NOT NULL,
                    client_request_id TEXT NOT NULL DEFAULT '',
                    request_json TEXT NOT NULL,
                    request_digest TEXT NOT NULL DEFAULT '',
                    details_json TEXT NOT NULL DEFAULT '{}',
                    error TEXT NOT NULL DEFAULT '',
                    attempt_count INTEGER NOT NULL DEFAULT 0,
                    accepted_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    echo_message_id TEXT NOT NULL DEFAULT '',
                    FOREIGN KEY (account_id, chat_id) REFERENCES chats(account_id, chat_id)
                );
                CREATE INDEX IF NOT EXISTS idx_outbox_pending ON outbox(status, updated_at);
                """
            )
            outbox_columns = {row[1] for row in conn.execute("PRAGMA table_info(outbox)")}
            if "request_digest" not in outbox_columns:
                conn.execute("ALTER TABLE outbox ADD COLUMN request_digest TEXT NOT NULL DEFAULT ''")

    def _append_event(self, conn: sqlite3.Connection, account_id: str, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        occurred_at = utc_now()
        event_id = f"event-{uuid.uuid4().hex}"
        cursor = conn.execute(
            "INSERT INTO events (event_id, account_id, event_type, occurred_at, payload_json) VALUES (?, ?, ?, ?, ?)",
            (event_id, account_id, event_type, occurred_at, compact_json(payload)),
        ).lastrowid
        return {
            "event_id": event_id,
            "cursor": str(cursor),
            "account_id": account_id,
            "event_type": event_type,
            "occurred_at": occurred_at,
            "payload": payload,
        }

    def account(self, account_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM accounts WHERE account_id=?", (account_id,)).fetchone()
        return self._account_row(row) if row else None

    def _account_row(self, row: sqlite3.Row) -> dict[str, Any]:
        return {
            "account_id": row["account_id"],
            "display_name": row["display_name"],
            "state": row["state"],
            "runtime": parse_json(row["runtime_json"], {}),
            "sync": parse_json(row["sync_json"], {}),
        }

    def list_accounts(self) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute("SELECT * FROM accounts ORDER BY account_id").fetchall()
        return [self._account_row(row) for row in rows]

    def upsert_account(
        self,
        account_id: str,
        display_name: str,
        *,
        state: str,
        runtime: dict[str, Any] | None = None,
        sync: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        now = utc_now()
        output = {
            "account_id": account_id,
            "display_name": display_name,
            "state": state,
            "runtime": runtime or {},
            "sync": sync or {},
        }
        with self.connection() as conn:
            before = conn.execute("SELECT * FROM accounts WHERE account_id=?", (account_id,)).fetchone()
            conn.execute(
                """
                INSERT INTO accounts (account_id, display_name, state, runtime_json, sync_json, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id) DO UPDATE SET
                    display_name=excluded.display_name, state=excluded.state,
                    runtime_json=excluded.runtime_json, sync_json=excluded.sync_json,
                    updated_at=excluded.updated_at
                """,
                (account_id, display_name, state, compact_json(output["runtime"]), compact_json(output["sync"]), now),
            )
            before_value = self._account_row(before) if before is not None else None
            if before_value is None or stable_event_value(before_value) != stable_event_value(output):
                self._append_event(conn, account_id, "account.status", {"account": output})
        return output

    def chat(self, account_id: str, chat_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM chats WHERE account_id=? AND chat_id=?", (account_id, chat_id)).fetchone()
        return self._chat_row(row) if row else None

    def _chat_row(self, row: sqlite3.Row) -> dict[str, Any]:
        output = {
            "account_id": row["account_id"],
            "chat_id": row["chat_id"],
            "type": row["type"],
            "display_name": row["display_name"],
            "updated_at": row["updated_at"],
        }
        if row["alias"]:
            output["alias"] = row["alias"]
        if row["member_count"]:
            output["member_count"] = int(row["member_count"])
        vendor = parse_json(row["vendor_json"], {})
        if vendor:
            output["vendor_specific"] = vendor
        return output

    def list_chats(self, account_id: str, *, cursor: str = "", limit: int = 100, query: str = "") -> dict[str, Any]:
        limit = max(1, min(int(limit), 200))
        offset = 0
        if cursor:
            try:
                decoded = base64.urlsafe_b64decode(cursor.encode("ascii") + b"===").decode("ascii")
                offset = max(0, int(decoded))
            except (ValueError, UnicodeDecodeError):
                raise StoreError("invalid_cursor", "cursor is not valid for this Core", details={"field": "cursor"})
        statement = "SELECT * FROM chats WHERE account_id=?"
        args: list[Any] = [account_id]
        if query:
            statement += " AND (display_name LIKE ? OR alias LIKE ? OR chat_id LIKE ?)"
            marker = f"%{query}%"
            args.extend([marker, marker, marker])
        statement += " ORDER BY updated_at DESC, chat_id LIMIT ? OFFSET ?"
        args.extend([limit + 1, offset])
        with self.connection() as conn:
            rows = conn.execute(statement, tuple(args)).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]
        next_cursor = ""
        if has_more:
            next_cursor = base64.urlsafe_b64encode(str(offset + len(rows)).encode("ascii")).decode("ascii").rstrip("=")
        return {"account_id": account_id, "chats": [self._chat_row(row) for row in rows], "next_cursor": next_cursor}

    def upsert_chat(self, chat: dict[str, Any]) -> bool:
        account_id = str(chat["account_id"])
        chat_id = str(chat["chat_id"])
        normalized = {
            "account_id": account_id,
            "chat_id": chat_id,
            "type": str(chat.get("type") or "private"),
            "display_name": str(chat.get("display_name") or chat_id),
            "alias": str(chat.get("alias") or ""),
            "member_count": max(0, int(chat.get("member_count") or 0)),
            "updated_at": str(chat.get("updated_at") or utc_now()),
            "vendor_specific": chat.get("vendor_specific") if isinstance(chat.get("vendor_specific"), dict) else {},
        }
        value_digest = digest({key: value for key, value in normalized.items() if key != "updated_at"})
        with self.connection() as conn:
            before = conn.execute("SELECT digest FROM chats WHERE account_id=? AND chat_id=?", (account_id, chat_id)).fetchone()
            conn.execute(
                """
                INSERT INTO chats (account_id, chat_id, type, display_name, alias, member_count, updated_at, vendor_json, digest)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, chat_id) DO UPDATE SET
                    type=excluded.type, display_name=excluded.display_name, alias=excluded.alias,
                    member_count=excluded.member_count, updated_at=excluded.updated_at,
                    vendor_json=excluded.vendor_json, digest=excluded.digest
                """,
                (
                    account_id, chat_id, normalized["type"], normalized["display_name"], normalized["alias"],
                    normalized["member_count"], normalized["updated_at"], compact_json(normalized["vendor_specific"]), value_digest,
                ),
            )
            changed = before is None or before["digest"] != value_digest
            if changed:
                self._append_event(conn, account_id, "chat.updated", {"chat": self._chat_row_from_value(normalized)})
        return changed

    @staticmethod
    def _chat_row_from_value(value: dict[str, Any]) -> dict[str, Any]:
        output = {key: value[key] for key in ("account_id", "chat_id", "type", "display_name", "updated_at")}
        if value.get("alias"):
            output["alias"] = value["alias"]
        if value.get("member_count"):
            output["member_count"] = value["member_count"]
        if value.get("vendor_specific"):
            output["vendor_specific"] = value["vendor_specific"]
        return output

    def upsert_contact(self, account_id: str, contact: dict[str, Any]) -> None:
        member_id = str(contact.get("member_id") or "").strip()
        if not member_id:
            return
        value = {
            "display_name": str(contact.get("display_name") or member_id),
            "alias": str(contact.get("alias") or ""),
            "remark": str(contact.get("remark") or ""),
        }
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO contacts (account_id, member_id, display_name, alias, remark, updated_at, digest)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, member_id) DO UPDATE SET
                    display_name=excluded.display_name, alias=excluded.alias, remark=excluded.remark,
                    updated_at=excluded.updated_at, digest=excluded.digest
                """,
                (account_id, member_id, value["display_name"], value["alias"], value["remark"], utc_now(), digest(value)),
            )

    def upsert_member(self, account_id: str, chat_id: str, member: dict[str, Any]) -> None:
        member_id = str(member.get("member_id") or "").strip()
        if not member_id:
            return
        value = {
            "display_name": str(member.get("display_name") or member_id),
            "alias": str(member.get("alias") or ""),
            "is_self": bool(member.get("is_self", False)),
        }
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO chat_members (account_id, chat_id, member_id, display_name, alias, is_self, updated_at, digest)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, chat_id, member_id) DO UPDATE SET
                    display_name=excluded.display_name, alias=excluded.alias, is_self=excluded.is_self,
                    updated_at=excluded.updated_at, digest=excluded.digest
                """,
                (account_id, chat_id, member_id, value["display_name"], value["alias"], int(value["is_self"]), utc_now(), digest(value)),
            )

    def member_count(self, account_id: str, chat_id: str) -> int:
        with self.connection() as conn:
            return int(conn.execute("SELECT COUNT(*) FROM chat_members WHERE account_id=? AND chat_id=?", (account_id, chat_id)).fetchone()[0])

    def member(self, account_id: str, chat_id: str, member_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM chat_members WHERE account_id=? AND chat_id=? AND member_id=?",
                (account_id, chat_id, member_id),
            ).fetchone()
        if row is None:
            return None
        return {
            "account_id": row["account_id"],
            "chat_id": row["chat_id"],
            "member_id": row["member_id"],
            "display_name": row["display_name"],
            "alias": row["alias"],
            "is_self": bool(row["is_self"]),
        }

    def _message_row(self, row: sqlite3.Row) -> dict[str, Any]:
        output = {
            "account_id": row["account_id"],
            "message_id": row["message_id"],
            "chat_id": row["chat_id"],
            "type": row["type"],
            "direction": row["direction"],
            "created_at": row["created_at"],
            "author": parse_json(row["author_json"], {}),
        }
        optional = (("text", row["text"]), ("media_id", row["media_id"]), ("filename", row["filename"]), ("mime_type", row["mime_type"]), ("target_message_id", row["target_message_id"]))
        for key, value in optional:
            if value:
                output[key] = value
        substitutions = parse_json(row["substitutions_json"], [])
        attributes = parse_json(row["attributes_json"], {})
        vendor = parse_json(row["vendor_json"], {})
        if substitutions:
            output["substitutions"] = substitutions
        if attributes:
            output["attributes"] = attributes
        if vendor:
            output["vendor_specific"] = vendor
        return output

    def upsert_message(self, message: dict[str, Any]) -> str:
        required = ("account_id", "message_id", "chat_id", "type", "direction", "created_at", "author")
        if any(not message.get(key) for key in required):
            raise StoreError("invalid_message", "normalized message is missing a required field")
        account_id = str(message["account_id"])
        message_id = str(message["message_id"])
        author = message.get("author") if isinstance(message.get("author"), dict) else {}
        author.setdefault("member_id", "unknown")
        author.setdefault("display_name", author["member_id"])
        author["is_self"] = bool(author.get("is_self", False))
        value = {
            "account_id": account_id,
            "message_id": message_id,
            "chat_id": str(message["chat_id"]),
            "type": str(message["type"]),
            "direction": str(message["direction"]),
            "created_at": str(message["created_at"]),
            "author": author,
            "text": str(message.get("text") or ""),
            "media_id": str(message.get("media_id") or ""),
            "filename": str(message.get("filename") or ""),
            "mime_type": str(message.get("mime_type") or ""),
            "target_message_id": str(message.get("target_message_id") or ""),
            "substitutions": message.get("substitutions") if isinstance(message.get("substitutions"), list) else [],
            "attributes": message.get("attributes") if isinstance(message.get("attributes"), dict) else {},
            "vendor_specific": message.get("vendor_specific") if isinstance(message.get("vendor_specific"), dict) else {},
        }
        value_digest = digest(value)
        with self.connection() as conn:
            before = conn.execute("SELECT digest FROM messages WHERE account_id=? AND message_id=?", (account_id, message_id)).fetchone()
            conn.execute(
                """
                INSERT INTO messages (
                    account_id, message_id, chat_id, type, direction, created_at, author_json, text, media_id,
                    filename, mime_type, target_message_id, substitutions_json, attributes_json, vendor_json, digest
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, message_id) DO UPDATE SET
                    chat_id=excluded.chat_id, type=excluded.type, direction=excluded.direction, created_at=excluded.created_at,
                    author_json=excluded.author_json, text=excluded.text, media_id=excluded.media_id,
                    filename=excluded.filename, mime_type=excluded.mime_type, target_message_id=excluded.target_message_id,
                    substitutions_json=excluded.substitutions_json, attributes_json=excluded.attributes_json,
                    vendor_json=excluded.vendor_json, digest=excluded.digest
                """,
                (
                    account_id, message_id, value["chat_id"], value["type"], value["direction"], value["created_at"],
                    compact_json(author), value["text"], value["media_id"], value["filename"], value["mime_type"],
                    value["target_message_id"], compact_json(value["substitutions"]), compact_json(value["attributes"]),
                    compact_json(value["vendor_specific"]), value_digest,
                ),
            )
            event_type = "message.created" if before is None else "message.updated"
            if before is None or before["digest"] != value_digest:
                self._append_event(conn, account_id, event_type, {"message": value})
        return "created" if before is None else "updated" if before["digest"] != value_digest else "unchanged"

    def upsert_media(self, media: dict[str, Any]) -> bool:
        account_id = str(media["account_id"])
        media_id = str(media["media_id"])
        value = {
            "account_id": account_id,
            "media_id": media_id,
            "filename": clean_filename(str(media.get("filename") or media_id)),
            "mime_type": str(media.get("mime_type") or "application/octet-stream"),
            "local_path": str(media["local_path"]),
            "disposition": str(media.get("disposition") or "inline"),
            "status": str(media.get("status") or "ready"),
        }
        value_digest = digest(value)
        with self.connection() as conn:
            before = conn.execute("SELECT digest, status FROM media WHERE account_id=? AND media_id=?", (account_id, media_id)).fetchone()
            conn.execute(
                """
                INSERT INTO media (account_id, media_id, filename, mime_type, local_path, disposition, status, updated_at, digest)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, media_id) DO UPDATE SET
                    filename=excluded.filename, mime_type=excluded.mime_type, local_path=excluded.local_path,
                    disposition=excluded.disposition, status=excluded.status, updated_at=excluded.updated_at, digest=excluded.digest
                """,
                (account_id, media_id, value["filename"], value["mime_type"], value["local_path"], value["disposition"], value["status"], utc_now(), value_digest),
            )
            changed = before is None or before["digest"] != value_digest
            if changed and value["status"] == "ready":
                self._append_event(conn, account_id, "media.ready", {"media": {key: value[key] for key in value if key != "local_path"}})
        return changed

    def media(self, account_id: str, media_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM media WHERE account_id=? AND media_id=?", (account_id, media_id)).fetchone()
        return dict(row) if row else None

    def poll_events(self, *, after: str, limit: int, account_id: str = "") -> dict[str, Any]:
        try:
            cursor = int(after or "0")
        except ValueError as exc:
            raise StoreError("invalid_cursor", "after must be a Core cursor", details={"field": "after"}) from exc
        if cursor < 0:
            raise StoreError("invalid_cursor", "after must not be negative", details={"field": "after"})
        limit = max(1, min(int(limit), 200))
        statement = "SELECT * FROM events WHERE cursor>?"
        args: list[Any] = [cursor]
        if account_id:
            statement += " AND account_id=?"
            args.append(account_id)
        statement += " ORDER BY cursor ASC LIMIT ?"
        args.append(limit + 1)
        with self.connection() as conn:
            rows = conn.execute(statement, tuple(args)).fetchall()
        has_more = len(rows) > limit
        selected = rows[:limit]
        events = [
            {
                "event_id": row["event_id"], "cursor": str(row["cursor"]), "account_id": row["account_id"],
                "event_type": row["event_type"], "occurred_at": row["occurred_at"],
                "payload": parse_json(row["payload_json"], {}),
            }
            for row in selected
        ]
        return {"events": events, "next_cursor": events[-1]["cursor"] if events else str(cursor), "has_more": has_more}

    def ack_events(self, consumer_id: str, event_ids: Iterable[str]) -> dict[str, Any]:
        event_ids = [str(item).strip() for item in event_ids if str(item).strip()]
        if not consumer_id.strip():
            raise StoreError("invalid_request", "consumer_id must be a non-empty string", details={"field": "consumer_id"})
        if not event_ids:
            raise StoreError("invalid_event_ids", "event_ids must be a non-empty list of strings", details={"field": "event_ids"})
        with self.connection() as conn:
            placeholders = ",".join("?" for _ in event_ids)
            known = {row[0] for row in conn.execute(f"SELECT event_id FROM events WHERE event_id IN ({placeholders})", tuple(event_ids))}
            unknown = [item for item in event_ids if item not in known]
            if unknown:
                raise StoreError("event_not_found", "One or more event IDs are unknown", status=404, details={"event_ids": unknown})
            now = utc_now()
            conn.executemany(
                "INSERT OR REPLACE INTO event_acks (consumer_id, event_id, acknowledged_at) VALUES (?, ?, ?)",
                [(consumer_id, event_id, now) for event_id in event_ids],
            )
        return {"consumer_id": consumer_id, "acked_event_ids": event_ids, "acked_count": len(event_ids)}

    def put_inline_media(
        self, account_id: str, content_base64: str, *, filename: str, mime_type: str, media_root: Path
    ) -> str:
        try:
            content = base64.b64decode(content_base64, validate=True)
        except (ValueError, TypeError) as exc:
            raise StoreError("invalid_base64", "content_base64 is not valid base64", details={"field": "content_base64"}) from exc
        if len(content) > MAX_INLINE_MEDIA_BYTES:
            raise StoreError("media_too_large", "Inline media exceeds 20 MiB decoded limit", status=413)
        media_id = f"upload-{uuid.uuid4().hex}"
        target_dir = media_root / account_id / "outbox"
        target_dir.mkdir(parents=True, exist_ok=True)
        target = target_dir / f"{media_id}-{clean_filename(filename)}"
        temporary = target.with_suffix(target.suffix + ".tmp")
        temporary.write_bytes(content)
        temporary.replace(target)
        self.upsert_media(
            {
                "account_id": account_id, "media_id": media_id, "filename": clean_filename(filename),
                "mime_type": mime_type or "application/octet-stream", "local_path": str(target),
                "disposition": "attachment", "status": "ready",
            }
        )
        return media_id

    @staticmethod
    def send_request_digest(kind: str, payload: dict[str, Any]) -> str:
        return digest({"kind": str(kind), "payload": payload})

    @staticmethod
    def _assert_idempotency_match(
        row: sqlite3.Row,
        *,
        kind: str,
        account_id: str,
        chat_id: str,
        request_digest: str,
    ) -> None:
        same_scope = (
            str(row["kind"]) == kind
            and str(row["account_id"]) == account_id
            and str(row["chat_id"]) == chat_id
        )
        stored_digest = str(row["request_digest"] or "")
        same_request = not stored_digest or stored_digest == request_digest
        if same_scope and same_request:
            return
        raise StoreError(
            "idempotency_conflict",
            "Idempotency-Key was already used for a different send request",
            status=409,
            details={"field": "Idempotency-Key"},
        )

    def queue_send(
        self,
        kind: str,
        payload: dict[str, Any],
        idempotency_key: str = "",
        *,
        request_digest: str = "",
    ) -> dict[str, Any]:
        account_id = str(payload.get("account_id") or "").strip()
        chat_id = str(payload.get("chat_id") or "").strip()
        key = str(idempotency_key or payload.get("client_request_id") or "").strip()
        request_digest = request_digest or self.send_request_digest(kind, payload)
        if key and len(key) > 200:
            raise StoreError("invalid_request", "Idempotency-Key must be at most 200 characters", details={"field": "Idempotency-Key"})
        now = utc_now()
        with self.connection() as conn:
            if key:
                previous = conn.execute("SELECT * FROM outbox WHERE idempotency_key=?", (key,)).fetchone()
                if previous:
                    self._assert_idempotency_match(
                        previous,
                        kind=kind,
                        account_id=account_id,
                        chat_id=chat_id,
                        request_digest=request_digest,
                    )
                    return self._receipt(previous)
            send_id = f"send-{uuid.uuid4().hex}"
            inserted = conn.execute(
                """
                INSERT OR IGNORE INTO outbox (send_id, idempotency_key, kind, account_id, chat_id, status, client_request_id,
                                    request_json, request_digest, accepted_at, updated_at)
                VALUES (?, ?, ?, ?, ?, 'accepted', ?, ?, ?, ?, ?)
                """,
                (
                    send_id,
                    key or None,
                    kind,
                    account_id,
                    chat_id,
                    str(payload.get("client_request_id") or ""),
                    compact_json(payload),
                    request_digest,
                    now,
                    now,
                ),
            )
            if not inserted.rowcount:
                previous = conn.execute("SELECT * FROM outbox WHERE idempotency_key=?", (key,)).fetchone()
                if previous:
                    self._assert_idempotency_match(
                        previous,
                        kind=kind,
                        account_id=account_id,
                        chat_id=chat_id,
                        request_digest=request_digest,
                    )
                    return self._receipt(previous)
                raise StoreError("idempotency_conflict", "Unable to create idempotent send", status=409)
            row = conn.execute("SELECT * FROM outbox WHERE send_id=?", (send_id,)).fetchone()
            receipt = self._receipt(row)
            self._append_event(conn, account_id, "send.updated", {"send": receipt})
        return receipt

    def receipt_by_idempotency_key(
        self,
        key: str,
        *,
        kind: str = "",
        account_id: str = "",
        chat_id: str = "",
        request_digest: str = "",
    ) -> dict[str, Any] | None:
        key = str(key or "").strip()
        if not key:
            return None
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM outbox WHERE idempotency_key=?", (key,)).fetchone()
        if row is not None and kind:
            self._assert_idempotency_match(
                row,
                kind=kind,
                account_id=account_id,
                chat_id=chat_id,
                request_digest=request_digest,
            )
        return self._receipt(row) if row else None

    def _receipt(self, row: sqlite3.Row) -> dict[str, Any]:
        receipt = {
            "send_id": row["send_id"], "status": row["status"], "kind": row["kind"],
            "account_id": row["account_id"], "chat_id": row["chat_id"], "accepted_at": row["accepted_at"],
        }
        if row["client_request_id"]:
            receipt["client_request_id"] = row["client_request_id"]
        if row["echo_message_id"]:
            receipt["echo_message_id"] = row["echo_message_id"]
        return receipt

    def pending_sends(self, *, limit: int = 20) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM outbox WHERE status IN ('accepted', 'queued') ORDER BY accepted_at LIMIT ?", (max(1, min(limit, 200)),)
            ).fetchall()
        return [dict(row) for row in rows]

    def recover_stale_sends(self, *, max_age_seconds: float = 120.0) -> int:
        """Fail interrupted in-flight sends after their lease instead of wedging forever.

        A GUI submit may have happened immediately before a process crash, so automatic
        retry could duplicate a message. Stale ``sending`` rows therefore become a
        durable failure and require an explicit new client request.
        """
        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=max(1.0, float(max_age_seconds)))).isoformat(
            timespec="seconds"
        ).replace("+00:00", "Z")
        recovered = 0
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM outbox WHERE status='sending' AND updated_at<=? ORDER BY updated_at",
                (cutoff,),
            ).fetchall()
            for row in rows:
                details = parse_json(row["details_json"], {})
                if not isinstance(details, dict):
                    details = {}
                details["recovery"] = {
                    "reason": "sending_lease_expired",
                    "previous_updated_at": row["updated_at"],
                }
                error = "sender process stopped or exceeded the sending lease; delivery state is unknown"
                conn.execute(
                    "UPDATE outbox SET status='failed', details_json=?, error=?, updated_at=? WHERE send_id=? AND status='sending'",
                    (compact_json(details), error, utc_now(), row["send_id"]),
                )
                updated = conn.execute("SELECT * FROM outbox WHERE send_id=?", (row["send_id"],)).fetchone()
                receipt = self._receipt(updated)
                self._append_event(
                    conn,
                    updated["account_id"],
                    "send.updated",
                    {
                        "send": receipt,
                        "details": details,
                        "error": {"code": "sender_interrupted", "message": error},
                    },
                )
                recovered += 1
        return recovered

    def transition_send(
        self, send_id: str, status: str, *, details: dict[str, Any] | None = None, error: str = "", echo_message_id: str = ""
    ) -> dict[str, Any]:
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM outbox WHERE send_id=?", (send_id,)).fetchone()
            if row is None:
                raise StoreError("send_not_found", f"Unknown send_id: {send_id}", status=404)
            conn.execute(
                """
                UPDATE outbox SET status=?, details_json=?, error=?, echo_message_id=?,
                    attempt_count=attempt_count+?, updated_at=? WHERE send_id=?
                """,
                (status, compact_json(details or {}), error, echo_message_id, 1 if status == "sending" else 0, utc_now(), send_id),
            )
            updated = conn.execute("SELECT * FROM outbox WHERE send_id=?", (send_id,)).fetchone()
            receipt = self._receipt(updated)
            payload = {"send": receipt}
            if details:
                payload["details"] = details
            if error:
                payload["error"] = {"code": "sender_failed", "message": error}
            self._append_event(conn, updated["account_id"], "send.updated", payload)
        return receipt

    def close(self) -> None:
        # Connections are deliberately short-lived so a process crash cannot keep a lock.
        return
