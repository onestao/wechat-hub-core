"""Normalized, durable account-aware Core SQLite store and event log."""

from __future__ import annotations

import base64
import hashlib
import json
import os
import sqlite3
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

from . import identity
from .identity import IdentityError  # noqa: F401  (re-exported for app/worker)
from .avatar import detect_image_mime, fallback_avatar_svg


MAX_INLINE_MEDIA_BYTES = 20 * 1024 * 1024

# Bounded extra patience on top of the per-connection busy_timeout for
# idempotent account-status persistence.  A long writer (for example a
# multi-minute import transaction) can exceed busy_timeout itself; these
# delays give it a final chance to finish without any infinite busy loop.
SQLITE_LOCK_RETRY_DELAYS: tuple[float, ...] = (0.25, 0.5, 1.0, 2.0, 4.0)


def is_transient_sqlite_lock(exc: BaseException) -> bool:
    return isinstance(exc, sqlite3.OperationalError) and "locked" in str(exc).lower()


# Business tables that carry additive identity v2 ownership columns.
IDENTITY_STAMPED_TABLES = ("chats", "contacts", "chat_members", "messages", "media", "events", "outbox")


class StoreError(RuntimeError):
    def __init__(self, code: str, message: str, *, status: int = 400, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.status = status
        self.details = details or {}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def parse_rfc3339(value: str) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


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


def resolve_contact_display_name(
    *,
    remark: str = "",
    nickname: str = "",
    alias: str = "",
    member_id: str = "",
) -> str:
    """Contact display name priority: remark -> nickname -> alias -> member_id."""
    return str(remark or "").strip() or str(nickname or "").strip() or str(alias or "").strip() or str(member_id or "").strip()


def resolve_group_member_display_name(
    *,
    group_nickname: str = "",
    remark: str = "",
    nickname: str = "",
    alias: str = "",
    member_id: str = "",
) -> str:
    """Group member display name priority: group_nickname -> remark -> nickname -> alias -> member_id."""
    return (
        str(group_nickname or "").strip()
        or str(remark or "").strip()
        or str(nickname or "").strip()
        or str(alias or "").strip()
        or str(member_id or "").strip()
    )


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
                    nickname TEXT NOT NULL DEFAULT '',
                    avatar_ref TEXT NOT NULL DEFAULT '',
                    head_img_md5 TEXT NOT NULL DEFAULT '',
                    big_head_url TEXT NOT NULL DEFAULT '',
                    small_head_url TEXT NOT NULL DEFAULT '',
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
                    group_nickname TEXT NOT NULL DEFAULT '',
                    remark TEXT NOT NULL DEFAULT '',
                    nickname TEXT NOT NULL DEFAULT '',
                    avatar_ref TEXT NOT NULL DEFAULT '',
                    is_self INTEGER NOT NULL DEFAULT 0,
                    updated_at TEXT NOT NULL,
                    digest TEXT NOT NULL,
                    PRIMARY KEY (account_id, chat_id, member_id),
                    FOREIGN KEY (account_id, chat_id) REFERENCES chats(account_id, chat_id)
                );

                CREATE TABLE IF NOT EXISTS avatar_cache (
                    cache_key TEXT PRIMARY KEY,
                    mime_type TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    content BLOB NOT NULL,
                    fetched_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_avatar_cache_fetched ON avatar_cache(fetched_at);
                CREATE INDEX IF NOT EXISTS idx_members_account_chat ON chat_members(account_id, chat_id);

                CREATE TABLE IF NOT EXISTS messages (
                    account_id TEXT NOT NULL,
                    message_id TEXT NOT NULL,
                    chat_id TEXT NOT NULL,
                    source_local_id TEXT NOT NULL DEFAULT '',
                    source_message_table TEXT NOT NULL DEFAULT '',
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

                CREATE TABLE IF NOT EXISTS runtime_instances (
                    instance_uuid TEXT PRIMARY KEY,
                    runtime_alias TEXT NOT NULL UNIQUE,
                    display_name TEXT NOT NULL,
                    resource_key TEXT NOT NULL,
                    runtime_provider TEXT NOT NULL DEFAULT 'agent_wechat',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS wechat_identities (
                    wechat_identity_uuid TEXT PRIMARY KEY,
                    wechat_user_id TEXT UNIQUE,
                    nickname TEXT NOT NULL DEFAULT '',
                    avatar_ref TEXT NOT NULL DEFAULT '',
                    profile_json TEXT NOT NULL DEFAULT '{}',
                    created_at TEXT NOT NULL,
                    updated_at TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS instance_identity_bindings (
                    binding_uuid TEXT PRIMARY KEY,
                    instance_uuid TEXT NOT NULL,
                    wechat_identity_uuid TEXT NOT NULL,
                    bound_at TEXT NOT NULL,
                    unbound_at TEXT,
                    active INTEGER NOT NULL DEFAULT 1,
                    verified_source TEXT NOT NULL,
                    FOREIGN KEY (instance_uuid) REFERENCES runtime_instances(instance_uuid),
                    FOREIGN KEY (wechat_identity_uuid) REFERENCES wechat_identities(wechat_identity_uuid)
                );
                CREATE INDEX IF NOT EXISTS idx_bindings_instance_active ON instance_identity_bindings(instance_uuid, active);

                CREATE TABLE IF NOT EXISTS instance_identity_observations (
                    observation_uuid TEXT PRIMARY KEY,
                    instance_uuid TEXT NOT NULL,
                    observed_wechat_user_id TEXT NOT NULL DEFAULT '',
                    resolved_state TEXT NOT NULL,
                    verified_source TEXT NOT NULL DEFAULT '',
                    created_at TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_observations_instance ON instance_identity_observations(instance_uuid, created_at);

                CREATE TABLE IF NOT EXISTS identity_backfills (
                    account_id TEXT PRIMARY KEY,
                    wechat_identity_uuid TEXT NOT NULL,
                    evidence TEXT NOT NULL,
                    migrated_at TEXT NOT NULL,
                    FOREIGN KEY (wechat_identity_uuid) REFERENCES wechat_identities(wechat_identity_uuid)
                );
                """
            )
            outbox_columns = {row[1] for row in conn.execute("PRAGMA table_info(outbox)")}
            if "request_digest" not in outbox_columns:
                conn.execute("ALTER TABLE outbox ADD COLUMN request_digest TEXT NOT NULL DEFAULT ''")
            message_columns = {row[1] for row in conn.execute("PRAGMA table_info(messages)")}
            if "source_local_id" not in message_columns:
                conn.execute("ALTER TABLE messages ADD COLUMN source_local_id TEXT NOT NULL DEFAULT ''")
            if "source_message_table" not in message_columns:
                conn.execute("ALTER TABLE messages ADD COLUMN source_message_table TEXT NOT NULL DEFAULT ''")
            self._migrate_message_source_identity(conn)
            conn.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_source_identity
                ON messages(account_id, chat_id, source_local_id)
                WHERE source_local_id<>''
                """
            )
            self._migrate_identity_v2_columns(conn)

    def _migrate_identity_v2_columns(self, conn: sqlite3.Connection) -> None:
        """Additive identity v2 ownership columns on all business tables.

        Contract §4.2: ``instance_uuid``/``wechat_identity_uuid`` default to ''
        on legacy rows and are backfilled by ``migrate_identity_v2``; existing
        tables are never rebuilt or renamed.
        """
        for table in IDENTITY_STAMPED_TABLES:
            columns = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
            if "instance_uuid" not in columns:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN instance_uuid TEXT NOT NULL DEFAULT ''")
            if "wechat_identity_uuid" not in columns:
                conn.execute(f"ALTER TABLE {table} ADD COLUMN wechat_identity_uuid TEXT NOT NULL DEFAULT ''")
        contact_cols = {row[1] for row in conn.execute("PRAGMA table_info(contacts)")}
        for col in ("nickname", "avatar_ref", "head_img_md5", "big_head_url", "small_head_url"):
            if col not in contact_cols:
                conn.execute(f"ALTER TABLE contacts ADD COLUMN {col} TEXT NOT NULL DEFAULT ''")
        member_cols = {row[1] for row in conn.execute("PRAGMA table_info(chat_members)")}
        for col in ("group_nickname", "remark", "nickname", "avatar_ref"):
            if col not in member_cols:
                conn.execute(f"ALTER TABLE chat_members ADD COLUMN {col} TEXT NOT NULL DEFAULT ''")
        existing_tables = {row[0] for row in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        if "avatar_cache" not in existing_tables:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS avatar_cache (
                    cache_key TEXT PRIMARY KEY,
                    mime_type TEXT NOT NULL,
                    size_bytes INTEGER NOT NULL,
                    content BLOB NOT NULL,
                    fetched_at TEXT NOT NULL
                )
            """)
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_messages_identity_chat
            ON messages(wechat_identity_uuid, chat_id, created_at, message_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_chats_identity_updated
            ON chats(wechat_identity_uuid, updated_at DESC, chat_id)
            """
        )
        conn.execute(
            """
            CREATE INDEX IF NOT EXISTS idx_contacts_identity_member
            ON contacts(wechat_identity_uuid, member_id)
            """
        )

    @staticmethod
    def _message_migration_score(row: sqlite3.Row) -> tuple[int, int, int]:
        vendor = parse_json(row["vendor_json"], {})
        if not isinstance(vendor, dict):
            vendor = {}
        server_id = vendor.get("source_server_id")
        acked = int(server_id not in (None, "", 0, "0"))
        complete = sum(
            bool(row[name])
            for name in (
                "text",
                "media_id",
                "filename",
                "mime_type",
                "target_message_id",
                "attributes_json",
                "vendor_json",
            )
        )
        return acked, complete, int(row["rowid"])

    def _migrate_message_source_identity(self, conn: sqlite3.Connection) -> None:
        """Backfill and dedupe normalized rows created before source identity existed.

        The oldest existing Core ``message_id`` is canonical because it may
        already have been exposed to Console/EFB.  Mutable/message content is
        refreshed from the most complete/newest duplicate before the duplicate
        row is removed.
        """

        rows = conn.execute(
            "SELECT rowid, message_id, vendor_json FROM messages WHERE source_local_id=''"
        ).fetchall()
        for row in rows:
            vendor = parse_json(row["vendor_json"], {})
            if not isinstance(vendor, dict):
                continue
            local_id = vendor.get("source_local_id")
            if local_id in (None, ""):
                continue
            conn.execute(
                "UPDATE messages SET source_local_id=?, source_message_table=? WHERE rowid=?",
                (
                    str(local_id),
                    str(vendor.get("source_message_table") or ""),
                    int(row["rowid"]),
                ),
            )

        duplicate_groups = conn.execute(
            """
            SELECT account_id, chat_id, source_local_id
            FROM messages
            WHERE source_local_id<>''
            GROUP BY account_id, chat_id, source_local_id
            HAVING COUNT(*)>1
            """
        ).fetchall()
        optional_fields = ("text", "media_id", "filename", "mime_type", "target_message_id")
        for group in duplicate_groups:
            duplicates = conn.execute(
                """
                SELECT rowid, * FROM messages
                WHERE account_id=? AND chat_id=? AND source_local_id=?
                ORDER BY rowid ASC
                """,
                (group["account_id"], group["chat_id"], group["source_local_id"]),
            ).fetchall()
            if len(duplicates) < 2:
                continue
            canonical = duplicates[0]
            best = max(duplicates, key=self._message_migration_score)
            merged_optional = {
                name: best[name] if best[name] not in (None, "") else canonical[name]
                for name in optional_fields
            }
            conn.execute(
                """
                UPDATE messages SET
                    source_message_table=?, type=?, direction=?, created_at=?, author_json=?,
                    text=?, media_id=?, filename=?, mime_type=?, target_message_id=?,
                    substitutions_json=?, attributes_json=?, vendor_json=?, digest=?
                WHERE rowid=?
                """,
                (
                    best["source_message_table"] or canonical["source_message_table"],
                    best["type"],
                    best["direction"],
                    best["created_at"],
                    best["author_json"],
                    merged_optional["text"],
                    merged_optional["media_id"],
                    merged_optional["filename"],
                    merged_optional["mime_type"],
                    merged_optional["target_message_id"],
                    best["substitutions_json"],
                    best["attributes_json"],
                    best["vendor_json"],
                    best["digest"],
                    int(canonical["rowid"]),
                ),
            )
            conn.executemany(
                "DELETE FROM messages WHERE rowid=?",
                [(int(row["rowid"]),) for row in duplicates[1:]],
            )

    def _append_event(self, conn: sqlite3.Connection, account_id: str, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        occurred_at = utc_now()
        event_id = f"event-{uuid.uuid4().hex}"
        instance_uuid, identity_uuid = identity.stamp_ref(conn, account_id)
        cursor = conn.execute(
            """
            INSERT INTO events (event_id, account_id, instance_uuid, wechat_identity_uuid, event_type, occurred_at, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (event_id, account_id, instance_uuid, identity_uuid, event_type, occurred_at, compact_json(payload)),
        ).lastrowid
        return {
            "event_id": event_id,
            "cursor": str(cursor),
            "account_id": account_id,
            "instance_uuid": instance_uuid,
            "wechat_identity_uuid": identity_uuid,
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
        # Account-status persistence is idempotent: a failed attempt rolls back
        # its whole transaction and the retry rewrites the identical row, while
        # event emission below is digest-guarded.  Only this idempotent path may
        # retry on a transient SQLite lock; business-data writes never do.
        for attempt, retry_delay in enumerate((0.0, *SQLITE_LOCK_RETRY_DELAYS)):
            if retry_delay:
                time.sleep(retry_delay)
            try:
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
            except sqlite3.OperationalError as exc:
                if not is_transient_sqlite_lock(exc) or attempt >= len(SQLITE_LOCK_RETRY_DELAYS):
                    raise
        raise RuntimeError("unreachable: upsert_account retry loop must return or raise")  # pragma: no cover

    # ------------------------------------------------------------------
    # Identity v2 (instance / wechat identity / binding) — contract §3-§5

    def binding_state(self, account_id: str) -> dict[str, Any]:
        with self.connection() as conn:
            instance = identity.instance_by_alias(conn, account_id)
            if instance is None:
                return {
                    "state": identity.STATE_UNBOUND,
                    "instance_uuid": "",
                    "binding": None,
                    "identity": None,
                    "observation": None,
                }
            info = identity.binding_state(conn, instance["instance_uuid"], account_id)
            info["instance_uuid"] = instance["instance_uuid"]
            return info

    def ensure_instance(
        self,
        account_id: str,
        *,
        instance_uuid: str = "",
        runtime_alias: str = "",
        resource_key: str = "",
        display_name: str = "",
        runtime_provider: str = "",
    ) -> dict[str, Any]:
        with self.connection() as conn:
            return identity.ensure_instance(
                conn,
                account_id,
                instance_uuid=instance_uuid,
                runtime_alias=runtime_alias,
                resource_key=resource_key,
                display_name=display_name,
                runtime_provider=runtime_provider,
            )

    def observe_login(
        self,
        account_id: str,
        logged_in_user: str,
        *,
        verified_source: str,
        instance_uuid: str = "",
        runtime_alias: str = "",
        resource_key: str = "",
        display_name: str = "",
        runtime_provider: str = "",
    ) -> dict[str, Any]:
        """Record a runtime-verified login observation and update binding state."""
        with self.connection() as conn:
            result = identity.observe_login(
                conn,
                account_id,
                logged_in_user,
                verified_source=verified_source,
                instance_uuid=instance_uuid,
                runtime_alias=runtime_alias,
                resource_key=resource_key,
                display_name=display_name,
                runtime_provider=runtime_provider,
            )
            if result["changed"]:
                self._append_event(conn, account_id, "identity.binding_changed", {"binding": result})
            return result

    def confirm_switch(self, account_id: str, *, observed_wechat_user_id: str = "") -> dict[str, Any]:
        """Operator-confirmed identity switch (contract §5.3)."""
        with self.connection() as conn:
            result = identity.confirm_switch(conn, account_id, observed_wxid=observed_wechat_user_id)
            self._append_event(conn, account_id, "identity.binding_changed", {"switch": result})
            return result

    def migrate_identity_v2(
        self,
        accounts: list[dict[str, Any]],
        *,
        identity_map: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Idempotent legacy account → identity backfill (contract §4.2)."""
        with self.connection() as conn:
            return identity.migrate_legacy_accounts(conn, accounts, identity_map=identity_map)

    def identity_view(self, account_id: str) -> dict[str, Any]:
        with self.connection() as conn:
            return identity.identity_view(conn, account_id)

    def wechat_identity(self, wechat_identity_uuid: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            return identity.identity_by_uuid(conn, wechat_identity_uuid)

    def identity_send_gate(
        self,
        account_id: str,
        *,
        expected_wechat_identity_uuid: str = "",
    ) -> dict[str, Any]:
        """Send Gate for API intake and sender dispatch (contract §3.2 rule 2)."""
        with self.connection() as conn:
            return identity.send_gate(conn, account_id, expected_wechat_identity_uuid=expected_wechat_identity_uuid)

    def record_identity_event(self, account_id: str, event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        with self.connection() as conn:
            return self._append_event(conn, account_id, event_type, payload)

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
        if row["instance_uuid"]:
            output["instance_uuid"] = row["instance_uuid"]
        if row["wechat_identity_uuid"]:
            output["wechat_identity_uuid"] = row["wechat_identity_uuid"]
        if row["alias"]:
            output["alias"] = row["alias"]
        if row["member_count"]:
            output["member_count"] = int(row["member_count"])
        vendor = parse_json(row["vendor_json"], {})
        if vendor:
            output["vendor_specific"] = vendor
        return output

    def list_chats(
        self,
        account_id: str,
        *,
        cursor: str = "",
        limit: int = 100,
        query: str = "",
        wechat_identity_uuid: str = "",
    ) -> dict[str, Any]:
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
        if wechat_identity_uuid:
            statement += " AND wechat_identity_uuid=?"
            args.append(str(wechat_identity_uuid))
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
        with self.connection() as conn:
            gate = identity.sync_gate(conn, account_id)
            instance_uuid = str(gate["instance"]["instance_uuid"])
            identity_uuid = str(gate["stamp_identity"])
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
            value_digest = digest(
                {key: value for key, value in normalized.items() if key != "updated_at"}
            )
            before = conn.execute("SELECT digest FROM chats WHERE account_id=? AND chat_id=?", (account_id, chat_id)).fetchone()
            conn.execute(
                """
                INSERT INTO chats (account_id, chat_id, instance_uuid, wechat_identity_uuid, type, display_name, alias, member_count, updated_at, vendor_json, digest)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, chat_id) DO UPDATE SET
                    type=excluded.type, display_name=excluded.display_name, alias=excluded.alias,
                    member_count=excluded.member_count, updated_at=excluded.updated_at,
                    vendor_json=excluded.vendor_json, digest=excluded.digest,
                    instance_uuid=CASE WHEN chats.instance_uuid<>'' THEN chats.instance_uuid ELSE excluded.instance_uuid END,
                    wechat_identity_uuid=CASE WHEN chats.wechat_identity_uuid<>'' THEN chats.wechat_identity_uuid ELSE excluded.wechat_identity_uuid END
                """,
                (
                    account_id, chat_id, instance_uuid, identity_uuid, normalized["type"], normalized["display_name"], normalized["alias"],
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
        remark = str(contact.get("remark") or "").strip()
        nickname = str(contact.get("nickname") or contact.get("nick_name") or "").strip()
        alias = str(contact.get("alias") or "").strip()
        avatar_ref = str(contact.get("avatar_ref") or contact.get("avatar_url") or "").strip()
        head_img_md5 = str(contact.get("head_img_md5") or "").strip()
        big_head_url = str(contact.get("big_head_url") or "").strip()
        small_head_url = str(contact.get("small_head_url") or "").strip()
        display_name = str(
            contact.get("display_name")
            or resolve_contact_display_name(remark=remark, nickname=nickname, alias=alias, member_id=member_id)
        )
        value = {
            "display_name": display_name,
            "alias": alias,
            "remark": remark,
            "nickname": nickname,
            "avatar_ref": avatar_ref,
            "head_img_md5": head_img_md5,
            "big_head_url": big_head_url,
            "small_head_url": small_head_url,
        }
        with self.connection() as conn:
            gate = identity.sync_gate(conn, account_id)
            conn.execute(
                """
                INSERT INTO contacts (
                    account_id, member_id, instance_uuid, wechat_identity_uuid,
                    display_name, alias, remark, nickname, avatar_ref, head_img_md5,
                    big_head_url, small_head_url, updated_at, digest
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, member_id) DO UPDATE SET
                    display_name=excluded.display_name,
                    alias=excluded.alias,
                    remark=excluded.remark,
                    nickname=excluded.nickname,
                    avatar_ref=excluded.avatar_ref,
                    head_img_md5=excluded.head_img_md5,
                    big_head_url=excluded.big_head_url,
                    small_head_url=excluded.small_head_url,
                    updated_at=excluded.updated_at,
                    digest=excluded.digest,
                    instance_uuid=CASE WHEN contacts.instance_uuid<>'' THEN contacts.instance_uuid ELSE excluded.instance_uuid END,
                    wechat_identity_uuid=CASE WHEN contacts.wechat_identity_uuid<>'' THEN contacts.wechat_identity_uuid ELSE excluded.wechat_identity_uuid END
                """,
                (
                    account_id, member_id, str(gate["instance"]["instance_uuid"]), str(gate["stamp_identity"]),
                    value["display_name"], value["alias"], value["remark"], value["nickname"],
                    value["avatar_ref"], value["head_img_md5"], value["big_head_url"], value["small_head_url"],
                    utc_now(), digest(value),
                ),
            )

    def upsert_member(self, account_id: str, chat_id: str, member: dict[str, Any]) -> None:
        member_id = str(member.get("member_id") or "").strip()
        if not member_id:
            return
        group_nickname = str(member.get("group_nickname") or member.get("group_alias") or "").strip()
        remark = str(member.get("remark") or "").strip()
        nickname = str(member.get("nickname") or member.get("nick_name") or "").strip()
        alias = str(member.get("alias") or "").strip()
        avatar_ref = str(member.get("avatar_ref") or member.get("avatar_url") or "").strip()
        is_self = bool(member.get("is_self", False))
        display_name = str(
            member.get("display_name")
            or resolve_group_member_display_name(
                group_nickname=group_nickname,
                remark=remark,
                nickname=nickname,
                alias=alias,
                member_id=member_id,
            )
        )
        value = {
            "display_name": display_name,
            "alias": alias,
            "group_nickname": group_nickname,
            "remark": remark,
            "nickname": nickname,
            "avatar_ref": avatar_ref,
            "is_self": is_self,
        }
        with self.connection() as conn:
            gate = identity.sync_gate(conn, account_id)
            conn.execute(
                """
                INSERT INTO chat_members (
                    account_id, chat_id, member_id, instance_uuid, wechat_identity_uuid,
                    display_name, alias, group_nickname, remark, nickname, avatar_ref,
                    is_self, updated_at, digest
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, chat_id, member_id) DO UPDATE SET
                    display_name=excluded.display_name,
                    alias=excluded.alias,
                    group_nickname=excluded.group_nickname,
                    remark=excluded.remark,
                    nickname=excluded.nickname,
                    avatar_ref=excluded.avatar_ref,
                    is_self=excluded.is_self,
                    updated_at=excluded.updated_at,
                    digest=excluded.digest,
                    instance_uuid=CASE WHEN chat_members.instance_uuid<>'' THEN chat_members.instance_uuid ELSE excluded.instance_uuid END,
                    wechat_identity_uuid=CASE WHEN chat_members.wechat_identity_uuid<>'' THEN chat_members.wechat_identity_uuid ELSE excluded.wechat_identity_uuid END
                """,
                (
                    account_id, chat_id, member_id, str(gate["instance"]["instance_uuid"]), str(gate["stamp_identity"]),
                    value["display_name"], value["alias"], value["group_nickname"], value["remark"],
                    value["nickname"], value["avatar_ref"], int(value["is_self"]), utc_now(), digest(value),
                ),
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
        if row["instance_uuid"]:
            output["instance_uuid"] = row["instance_uuid"]
        if row["wechat_identity_uuid"]:
            output["wechat_identity_uuid"] = row["wechat_identity_uuid"]
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

    def _reconcile_text_echo(self, conn: sqlite3.Connection, message: dict[str, Any]) -> str:
        """Conservatively link one outgoing text to one submitted outbox row.

        The X11 controller cannot return a WeChat message ID.  We therefore
        only reconcile when there is exactly one recent, already-submitted,
        plain-text candidate with an exact text match.  Mention sends are
        skipped because the GUI may materialize the blue mention differently
        from the request text.  Ambiguity intentionally leaves the send
        unconfirmed rather than risking a false echo mapping.
        """
        if message.get("direction") != "outgoing" or message.get("type") != "text":
            return ""
        text = str(message.get("text") or "").strip()
        message_id = str(message.get("message_id") or "").strip()
        message_time = parse_rfc3339(str(message.get("created_at") or ""))
        if not text or not message_id or message_time is None:
            return ""

        rows = conn.execute(
            """
            SELECT * FROM outbox
            WHERE account_id=? AND chat_id=? AND kind='text'
              AND status='submitted' AND echo_message_id=''
            ORDER BY updated_at DESC
            LIMIT 20
            """,
            (str(message["account_id"]), str(message["chat_id"])),
        ).fetchall()
        candidates: list[sqlite3.Row] = []
        for row in rows:
            request = parse_json(row["request_json"], {})
            if not isinstance(request, dict):
                continue
            if request.get("mention_member_ids"):
                continue
            if str(request.get("text") or "").strip() != text:
                continue
            sent_time = parse_rfc3339(str(row["updated_at"] or row["accepted_at"] or ""))
            if sent_time is None:
                continue
            delta = (message_time - sent_time).total_seconds()
            if -10 <= delta <= 180:
                candidates.append(row)
        if len(candidates) != 1:
            return ""

        row = candidates[0]
        now = utc_now()
        details = parse_json(row["details_json"], {})
        if not isinstance(details, dict):
            details = {}
        details["echo_reconciliation"] = {
            "method": "unique_exact_text",
            "message_id": message_id,
            "matched_at": now,
        }
        details["delivery_certainty"] = "confirmed"
        details["automatic_retry"] = False
        changed = conn.execute(
            """
            UPDATE outbox
            SET status='sent', echo_message_id=?, details_json=?, error='', updated_at=?
            WHERE send_id=? AND status='submitted' AND echo_message_id=''
            """,
            (message_id, compact_json(details), now, row["send_id"]),
        ).rowcount
        if changed != 1:
            return ""
        updated = conn.execute("SELECT * FROM outbox WHERE send_id=?", (row["send_id"],)).fetchone()
        receipt = self._receipt(updated)
        self._append_event(
            conn,
            str(message["account_id"]),
            "send.updated",
            {"send": receipt, "details": {"echo_reconciliation": details["echo_reconciliation"]}},
        )
        return str(row["send_id"])

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
            "source_local_id": "",
            "source_message_table": "",
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
        source_local_id = value["vendor_specific"].get("source_local_id")
        if source_local_id not in (None, ""):
            value["source_local_id"] = str(source_local_id)
        value["source_message_table"] = str(value["vendor_specific"].get("source_message_table") or "")
        with self.connection() as conn:
            gate = identity.sync_gate(conn, account_id)
            instance_uuid = str(gate["instance"]["instance_uuid"])
            identity_uuid = str(gate["stamp_identity"])
            if value["source_local_id"]:
                canonical = conn.execute(
                    """
                    SELECT message_id FROM messages
                    WHERE account_id=? AND chat_id=? AND source_local_id=?
                    LIMIT 1
                    """,
                    (account_id, value["chat_id"], value["source_local_id"]),
                ).fetchone()
                if canonical is not None:
                    value["message_id"] = str(canonical["message_id"])
                    message_id = value["message_id"]
            value_digest = digest(
                {key: item for key, item in value.items() if key not in ("instance_uuid", "wechat_identity_uuid")}
            )
            before = conn.execute("SELECT digest FROM messages WHERE account_id=? AND message_id=?", (account_id, message_id)).fetchone()
            conn.execute(
                """
                INSERT INTO messages (
                    account_id, message_id, chat_id, instance_uuid, wechat_identity_uuid,
                    source_local_id, source_message_table,
                    type, direction, created_at, author_json, text, media_id,
                    filename, mime_type, target_message_id, substitutions_json, attributes_json, vendor_json, digest
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, message_id) DO UPDATE SET
                    chat_id=excluded.chat_id, source_local_id=excluded.source_local_id,
                    source_message_table=excluded.source_message_table,
                    type=excluded.type, direction=excluded.direction, created_at=excluded.created_at,
                    author_json=excluded.author_json, text=excluded.text, media_id=excluded.media_id,
                    filename=excluded.filename, mime_type=excluded.mime_type, target_message_id=excluded.target_message_id,
                    substitutions_json=excluded.substitutions_json, attributes_json=excluded.attributes_json,
                    vendor_json=excluded.vendor_json, digest=excluded.digest,
                    instance_uuid=CASE WHEN messages.instance_uuid<>'' THEN messages.instance_uuid ELSE excluded.instance_uuid END,
                    wechat_identity_uuid=CASE WHEN messages.wechat_identity_uuid<>'' THEN messages.wechat_identity_uuid ELSE excluded.wechat_identity_uuid END
                """,
                (
                    account_id, message_id, value["chat_id"], instance_uuid, identity_uuid,
                    value["source_local_id"], value["source_message_table"],
                    value["type"], value["direction"], value["created_at"],
                    compact_json(author), value["text"], value["media_id"], value["filename"], value["mime_type"],
                    value["target_message_id"], compact_json(value["substitutions"]), compact_json(value["attributes"]),
                    compact_json(value["vendor_specific"]), value_digest,
                ),
            )
            event_type = "message.created" if before is None else "message.updated"
            # Emit a successful echo link before the corresponding
            # message.created event.  Consumers such as EFB can then learn the
            # send_id -> WeChat message_id alias before deciding whether this
            # outgoing message is a native self-message or the echo of their
            # own send.
            self._reconcile_text_echo(conn, value)
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
            gate = identity.sync_gate(conn, account_id)
            before = conn.execute("SELECT digest, status FROM media WHERE account_id=? AND media_id=?", (account_id, media_id)).fetchone()
            conn.execute(
                """
                INSERT INTO media (account_id, media_id, instance_uuid, wechat_identity_uuid, filename, mime_type, local_path, disposition, status, updated_at, digest)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(account_id, media_id) DO UPDATE SET
                    filename=excluded.filename, mime_type=excluded.mime_type, local_path=excluded.local_path,
                    disposition=excluded.disposition, status=excluded.status, updated_at=excluded.updated_at, digest=excluded.digest,
                    instance_uuid=CASE WHEN media.instance_uuid<>'' THEN media.instance_uuid ELSE excluded.instance_uuid END,
                    wechat_identity_uuid=CASE WHEN media.wechat_identity_uuid<>'' THEN media.wechat_identity_uuid ELSE excluded.wechat_identity_uuid END
                """,
                (
                    account_id, media_id, str(gate["instance"]["instance_uuid"]), str(gate["stamp_identity"]),
                    value["filename"], value["mime_type"], value["local_path"], value["disposition"], value["status"], utc_now(), value_digest,
                ),
            )
            changed = before is None or before["digest"] != value_digest
            if changed and value["status"] == "ready":
                self._append_event(conn, account_id, "media.ready", {"media": {key: value[key] for key in value if key != "local_path"}})
        return changed

    def media(self, account_id: str, media_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute("SELECT * FROM media WHERE account_id=? AND media_id=?", (account_id, media_id)).fetchone()
        return dict(row) if row else None

    # ------------------------------------------------------------------
    # Identity-keyed data access (contract B8) — every business entity is
    # queryable by wechat_identity_uuid; the account-scoped compat layer
    # resolves the current binding and filters through it.

    def list_messages(
        self,
        account_id: str,
        chat_id: str,
        *,
        wechat_identity_uuid: str | None = None,
        cursor: str = "",
        limit: int = 100,
    ) -> dict[str, Any]:
        """Account-compat message listing with optional identity scoping.

        ``wechat_identity_uuid=None`` disables identity filtering (used by the
        legacy passthrough window); a concrete UUID scopes results to one
        identity's data space.
        """
        limit = max(1, min(int(limit), 200))
        offset = 0
        if cursor:
            try:
                decoded = base64.urlsafe_b64decode(cursor.encode("ascii") + b"===").decode("ascii")
                offset = max(0, int(decoded))
            except (ValueError, UnicodeDecodeError):
                raise StoreError("invalid_cursor", "cursor is not valid for this Core", details={"field": "cursor"})
        statement = "SELECT * FROM messages WHERE account_id=? AND chat_id=?"
        args: list[Any] = [account_id, chat_id]
        if wechat_identity_uuid:
            statement += " AND wechat_identity_uuid=?"
            args.append(str(wechat_identity_uuid))
        statement += " ORDER BY created_at ASC, message_id LIMIT ? OFFSET ?"
        args.extend([limit + 1, offset])
        with self.connection() as conn:
            rows = conn.execute(statement, tuple(args)).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]
        next_cursor = ""
        if has_more:
            next_cursor = base64.urlsafe_b64encode(str(offset + len(rows)).encode("ascii")).decode("ascii").rstrip("=")
        return {"account_id": account_id, "chat_id": chat_id, "messages": [self._message_row(row) for row in rows], "next_cursor": next_cursor}

    def identity_list_chats(
        self,
        wechat_identity_uuid: str,
        *,
        cursor: str = "",
        limit: int = 100,
        query: str = "",
    ) -> dict[str, Any]:
        limit = max(1, min(int(limit), 200))
        offset = 0
        if cursor:
            try:
                decoded = base64.urlsafe_b64decode(cursor.encode("ascii") + b"===").decode("ascii")
                offset = max(0, int(decoded))
            except (ValueError, UnicodeDecodeError):
                raise StoreError("invalid_cursor", "cursor is not valid for this Core", details={"field": "cursor"})
        statement = "SELECT * FROM chats WHERE wechat_identity_uuid=?"
        args: list[Any] = [str(wechat_identity_uuid)]
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
        return {"wechat_identity_uuid": str(wechat_identity_uuid), "chats": [self._chat_row(row) for row in rows], "next_cursor": next_cursor}

    def identity_list_messages(
        self,
        wechat_identity_uuid: str,
        *,
        chat_id: str = "",
        cursor: str = "",
        limit: int = 100,
    ) -> dict[str, Any]:
        limit = max(1, min(int(limit), 200))
        offset = 0
        if cursor:
            try:
                decoded = base64.urlsafe_b64decode(cursor.encode("ascii") + b"===").decode("ascii")
                offset = max(0, int(decoded))
            except (ValueError, UnicodeDecodeError):
                raise StoreError("invalid_cursor", "cursor is not valid for this Core", details={"field": "cursor"})
        statement = "SELECT * FROM messages WHERE wechat_identity_uuid=?"
        args: list[Any] = [str(wechat_identity_uuid)]
        if chat_id:
            statement += " AND chat_id=?"
            args.append(str(chat_id))
        statement += " ORDER BY created_at ASC, message_id LIMIT ? OFFSET ?"
        args.extend([limit + 1, offset])
        with self.connection() as conn:
            rows = conn.execute(statement, tuple(args)).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]
        next_cursor = ""
        if has_more:
            next_cursor = base64.urlsafe_b64encode(str(offset + len(rows)).encode("ascii")).decode("ascii").rstrip("=")
        return {"wechat_identity_uuid": str(wechat_identity_uuid), "messages": [self._message_row(row) for row in rows], "next_cursor": next_cursor}

    def identity_list_contacts(
        self,
        wechat_identity_uuid: str,
        *,
        query: str = "",
        limit: int = 200,
        cursor: str = "",
    ) -> dict[str, Any]:
        limit = max(1, min(int(limit), 500))
        offset = 0
        if cursor:
            try:
                decoded = base64.urlsafe_b64decode(cursor.encode("ascii") + b"===").decode("ascii")
                offset = max(0, int(decoded))
            except (ValueError, UnicodeDecodeError):
                raise StoreError("invalid_cursor", "cursor is not valid for this Core", details={"field": "cursor"})

        statement = "SELECT * FROM contacts WHERE wechat_identity_uuid=?"
        args: list[Any] = [str(wechat_identity_uuid)]
        if query:
            q = f"%{query}%"
            statement += " AND (display_name LIKE ? OR remark LIKE ? OR nickname LIKE ? OR alias LIKE ? OR member_id LIKE ?)"
            args.extend([q, q, q, q, q])
        statement += " ORDER BY display_name COLLATE NOCASE ASC, member_id ASC LIMIT ? OFFSET ?"
        args.extend([limit + 1, offset])

        with self.connection() as conn:
            rows = conn.execute(statement, tuple(args)).fetchall()

        has_more = len(rows) > limit
        rows = rows[:limit]
        next_cursor = ""
        if has_more:
            next_cursor = base64.urlsafe_b64encode(str(offset + len(rows)).encode("ascii")).decode("ascii").rstrip("=")

        contacts = [
            {
                "account_id": row["account_id"],
                "wechat_identity_uuid": str(row["wechat_identity_uuid"] or wechat_identity_uuid),
                "member_id": row["member_id"],
                "display_name": row["display_name"],
                "remark": row["remark"] if "remark" in row.keys() else "",
                "nickname": row["nickname"] if "nickname" in row.keys() else "",
                "alias": row["alias"] if "alias" in row.keys() else "",
                "avatar_ref": row["avatar_ref"] if "avatar_ref" in row.keys() else "",
                "avatar_url": f"/v1/identities/{wechat_identity_uuid}/contacts/{row['member_id']}/avatar",
                "head_img_md5": row["head_img_md5"] if "head_img_md5" in row.keys() else "",
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]
        return {
            "wechat_identity_uuid": str(wechat_identity_uuid),
            "contacts": contacts,
            "next_cursor": next_cursor,
            "has_more": has_more,
            "limit": limit,
        }

    def identity_list_members(
        self,
        wechat_identity_uuid: str,
        chat_id: str,
        *,
        query: str = "",
        limit: int = 200,
        cursor: str = "",
    ) -> dict[str, Any]:
        limit = max(1, min(int(limit), 500))
        offset = 0
        if cursor:
            try:
                decoded = base64.urlsafe_b64decode(cursor.encode("ascii") + b"===").decode("ascii")
                offset = max(0, int(decoded))
            except (ValueError, UnicodeDecodeError):
                raise StoreError("invalid_cursor", "cursor is not valid for this Core", details={"field": "cursor"})

        statement = "SELECT * FROM chat_members WHERE wechat_identity_uuid=? AND chat_id=?"
        args: list[Any] = [str(wechat_identity_uuid), str(chat_id)]
        if query:
            q = f"%{query}%"
            statement += " AND (display_name LIKE ? OR group_nickname LIKE ? OR remark LIKE ? OR nickname LIKE ? OR alias LIKE ? OR member_id LIKE ?)"
            args.extend([q, q, q, q, q, q])
        statement += " ORDER BY display_name COLLATE NOCASE ASC, member_id ASC LIMIT ? OFFSET ?"
        args.extend([limit + 1, offset])

        with self.connection() as conn:
            rows = conn.execute(statement, tuple(args)).fetchall()

        has_more = len(rows) > limit
        rows = rows[:limit]
        next_cursor = ""
        if has_more:
            next_cursor = base64.urlsafe_b64encode(str(offset + len(rows)).encode("ascii")).decode("ascii").rstrip("=")

        members = [
            {
                "account_id": row["account_id"],
                "wechat_identity_uuid": str(row["wechat_identity_uuid"] or wechat_identity_uuid),
                "chat_id": row["chat_id"],
                "member_id": row["member_id"],
                "group_nickname": row["group_nickname"] if "group_nickname" in row.keys() else "",
                "display_name": row["display_name"],
                "remark": row["remark"] if "remark" in row.keys() else "",
                "nickname": row["nickname"] if "nickname" in row.keys() else "",
                "alias": row["alias"] if "alias" in row.keys() else "",
                "avatar_ref": row["avatar_ref"] if "avatar_ref" in row.keys() else "",
                "avatar_url": f"/v1/identities/{wechat_identity_uuid}/contacts/{row['member_id']}/avatar",
                "is_self": bool(row["is_self"]),
                "updated_at": row["updated_at"],
            }
            for row in rows
        ]
        return {
            "wechat_identity_uuid": str(wechat_identity_uuid),
            "chat_id": str(chat_id),
            "members": members,
            "next_cursor": next_cursor,
            "has_more": has_more,
            "limit": limit,
        }

    def identity_contact(self, wechat_identity_uuid: str, member_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM contacts WHERE wechat_identity_uuid=? AND member_id=?",
                (str(wechat_identity_uuid), str(member_id)),
            ).fetchone()
        if row is None:
            return None
        return {
            "account_id": row["account_id"],
            "wechat_identity_uuid": str(row["wechat_identity_uuid"]),
            "member_id": row["member_id"],
            "display_name": row["display_name"],
            "remark": row["remark"] if "remark" in row.keys() else "",
            "nickname": row["nickname"] if "nickname" in row.keys() else "",
            "alias": row["alias"] if "alias" in row.keys() else "",
            "avatar_ref": row["avatar_ref"] if "avatar_ref" in row.keys() else "",
            "avatar_url": f"/v1/identities/{wechat_identity_uuid}/contacts/{row['member_id']}/avatar",
            "head_img_md5": row["head_img_md5"] if "head_img_md5" in row.keys() else "",
            "big_head_url": row["big_head_url"] if "big_head_url" in row.keys() else "",
            "small_head_url": row["small_head_url"] if "small_head_url" in row.keys() else "",
            "updated_at": row["updated_at"],
        }

    def identity_member(self, wechat_identity_uuid: str, chat_id: str, member_id: str) -> dict[str, Any] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM chat_members WHERE wechat_identity_uuid=? AND chat_id=? AND member_id=?",
                (str(wechat_identity_uuid), str(chat_id), str(member_id)),
            ).fetchone()
        if row is None:
            return None
        return {
            "account_id": row["account_id"],
            "wechat_identity_uuid": str(row["wechat_identity_uuid"]),
            "chat_id": row["chat_id"],
            "member_id": row["member_id"],
            "group_nickname": row["group_nickname"] if "group_nickname" in row.keys() else "",
            "display_name": row["display_name"],
            "remark": row["remark"] if "remark" in row.keys() else "",
            "nickname": row["nickname"] if "nickname" in row.keys() else "",
            "alias": row["alias"] if "alias" in row.keys() else "",
            "avatar_ref": row["avatar_ref"] if "avatar_ref" in row.keys() else "",
            "avatar_url": f"/v1/identities/{wechat_identity_uuid}/contacts/{row['member_id']}/avatar",
            "is_self": bool(row["is_self"]),
            "updated_at": row["updated_at"],
        }

    def identity_profile(self, wechat_identity_uuid: str) -> dict[str, Any]:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT * FROM wechat_identities WHERE wechat_identity_uuid=?",
                (str(wechat_identity_uuid),),
            ).fetchone()
        if row is None:
            raise StoreError("identity_not_found", f"Unknown wechat_identity_uuid: {wechat_identity_uuid}", status=404)
        wechat_user_id = str(row["wechat_user_id"] or "")
        nickname = str(row["nickname"] or "")
        effective_nickname = nickname or wechat_user_id
        avatar_ref = str(row["avatar_ref"] or "")
        avatar_url = avatar_ref if avatar_ref.startswith(("/", "http://", "https://")) else f"/v1/identities/{wechat_identity_uuid}/avatar"
        return {
            "wechat_identity_uuid": str(row["wechat_identity_uuid"]),
            "wechat_user_id": wechat_user_id,
            "nickname": effective_nickname,
            "avatar_ref": avatar_ref,
            "avatar_url": avatar_url,
            "profile_json": parse_json(row["profile_json"], {}),
            "created_at": row["created_at"],
            "updated_at": row["updated_at"],
        }

    def update_identity_profile(
        self,
        wechat_identity_uuid: str,
        *,
        nickname: str | None = None,
        avatar_ref: str | None = None,
        profile_json: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        with self.connection() as conn:
            existing = conn.execute(
                "SELECT * FROM wechat_identities WHERE wechat_identity_uuid=?",
                (str(wechat_identity_uuid),),
            ).fetchone()
            if existing is None:
                raise StoreError("identity_not_found", f"Unknown wechat_identity_uuid: {wechat_identity_uuid}", status=404)
            updates: list[str] = ["updated_at=?"]
            args: list[Any] = [utc_now()]
            if nickname is not None:
                updates.append("nickname=?")
                args.append(str(nickname))
            if avatar_ref is not None:
                updates.append("avatar_ref=?")
                args.append(str(avatar_ref))
            if profile_json is not None:
                updates.append("profile_json=?")
                args.append(compact_json(profile_json))
            args.append(str(wechat_identity_uuid))
            conn.execute(
                f"UPDATE wechat_identities SET {', '.join(updates)} WHERE wechat_identity_uuid=?",
                tuple(args),
            )
        return self.identity_profile(wechat_identity_uuid)

    def get_avatar_cache(self, cache_key: str) -> tuple[bytes, str] | None:
        with self.connection() as conn:
            row = conn.execute(
                "SELECT content, mime_type FROM avatar_cache WHERE cache_key=?",
                (str(cache_key),),
            ).fetchone()
        if row is None:
            return None
        return bytes(row["content"]), str(row["mime_type"])

    def set_avatar_cache(self, cache_key: str, content: bytes, mime_type: str) -> None:
        with self.connection() as conn:
            conn.execute(
                """
                INSERT INTO avatar_cache (cache_key, mime_type, size_bytes, content, fetched_at)
                VALUES (?, ?, ?, ?, ?)
                ON CONFLICT(cache_key) DO UPDATE SET
                    mime_type=excluded.mime_type,
                    size_bytes=excluded.size_bytes,
                    content=excluded.content,
                    fetched_at=excluded.fetched_at
                """,
                (str(cache_key), str(mime_type), len(content), sqlite3.Binary(content), utc_now()),
            )

    def identity_list_media(self, wechat_identity_uuid: str, *, limit: int = 200) -> dict[str, Any]:
        limit = max(1, min(int(limit), 500))
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM media WHERE wechat_identity_uuid=? ORDER BY updated_at DESC, media_id LIMIT ?",
                (str(wechat_identity_uuid), limit),
            ).fetchall()
        media = [dict(row) for row in rows]
        return {"wechat_identity_uuid": str(wechat_identity_uuid), "media": media}

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
            # Send Gate (contract §3.2 rule 2): the authoritative identity check
            # happens here so no queue path can bypass the API-level gate.
            expected_identity = str(
                payload.get("expected_wechat_identity_uuid") or payload.get("wechat_identity_uuid") or ""
            ).strip()
            gate = identity.send_gate(conn, account_id, expected_wechat_identity_uuid=expected_identity)
            send_id = f"send-{uuid.uuid4().hex}"
            inserted = conn.execute(
                """
                INSERT OR IGNORE INTO outbox (send_id, idempotency_key, kind, account_id, chat_id, instance_uuid, wechat_identity_uuid,
                                    status, client_request_id, request_json, request_digest, accepted_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 'accepted', ?, ?, ?, ?, ?)
                """,
                (
                    send_id,
                    key or None,
                    kind,
                    account_id,
                    chat_id,
                    str(gate["instance"]["instance_uuid"]),
                    str(gate["stamp_identity"]),
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
        details = parse_json(row["details_json"], {})
        if isinstance(details, dict):
            if details.get("delivery_certainty"):
                receipt["delivery_certainty"] = details["delivery_certainty"]
            if "automatic_retry" in details:
                receipt["automatic_retry"] = bool(details["automatic_retry"])
        return receipt

    def pending_sends(self, *, limit: int = 20) -> list[dict[str, Any]]:
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM outbox WHERE status IN ('accepted', 'queued') ORDER BY accepted_at LIMIT ?", (max(1, min(limit, 200)),)
            ).fetchall()
        return [dict(row) for row in rows]

    def fail_pending_sends_for_account(self, account_id: str, *, reason: str) -> int:
        """Fail only not-yet-dispatched sends when an account leaves the live registry.

        ``sending`` rows are deliberately not rewritten here because GUI delivery may
        already have happened; their existing lease recovery keeps that uncertainty
        explicit instead of risking a duplicate send.
        """
        failed = 0
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM outbox WHERE account_id=? AND status IN ('accepted', 'queued') ORDER BY accepted_at",
                (account_id,),
            ).fetchall()
            for row in rows:
                details = parse_json(row["details_json"], {})
                if not isinstance(details, dict):
                    details = {}
                details["registry"] = {"reason": "account_unregistered"}
                conn.execute(
                    "UPDATE outbox SET status='failed', details_json=?, error=?, updated_at=? WHERE send_id=? AND status IN ('accepted', 'queued')",
                    (compact_json(details), reason, utc_now(), row["send_id"]),
                )
                updated = conn.execute("SELECT * FROM outbox WHERE send_id=?", (row["send_id"],)).fetchone()
                if updated["status"] != "failed":
                    continue
                receipt = self._receipt(updated)
                self._append_event(
                    conn,
                    account_id,
                    "send.updated",
                    {
                        "send": receipt,
                        "details": details,
                        "error": {"code": "account_unregistered", "message": reason},
                    },
                )
                failed += 1
        return failed

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

    def expire_submitted_sends(self, *, max_age_seconds: float = 120.0) -> int:
        """Turn unconfirmed submissions into durable uncertainty without retrying.

        ``submitted`` means the sender/FSM returned success but Core has not
        observed a unique WeChat DB echo.  Once the confirmation window closes,
        retrying automatically would risk a duplicate message.
        """

        cutoff = (datetime.now(timezone.utc) - timedelta(seconds=max(1.0, float(max_age_seconds)))).isoformat(
            timespec="seconds"
        ).replace("+00:00", "Z")
        expired = 0
        with self.connection() as conn:
            rows = conn.execute(
                "SELECT * FROM outbox WHERE status='submitted' AND updated_at<=? ORDER BY updated_at",
                (cutoff,),
            ).fetchall()
            for row in rows:
                details = parse_json(row["details_json"], {})
                if not isinstance(details, dict):
                    details = {}
                details["delivery_certainty"] = "unknown"
                details["automatic_retry"] = False
                details["confirmation"] = {
                    "reason": "db_echo_timeout",
                    "previous_updated_at": row["updated_at"],
                    "window_seconds": max(1.0, float(max_age_seconds)),
                }
                error = (
                    "sender reported submission success, but WeChat DB/Core did not observe "
                    "a unique outgoing echo within the confirmation window"
                )
                changed = conn.execute(
                    """
                    UPDATE outbox
                    SET status='uncertain', details_json=?, error=?, updated_at=?
                    WHERE send_id=? AND status='submitted'
                    """,
                    (compact_json(details), error, utc_now(), row["send_id"]),
                ).rowcount
                if changed != 1:
                    continue
                updated = conn.execute("SELECT * FROM outbox WHERE send_id=?", (row["send_id"],)).fetchone()
                self._append_event(
                    conn,
                    str(updated["account_id"]),
                    "send.updated",
                    {
                        "send": self._receipt(updated),
                        "details": details,
                        "error": {"code": "delivery_confirmation_timeout", "message": error},
                    },
                )
                expired += 1
        return expired

    def transition_send(
        self,
        send_id: str,
        status: str,
        *,
        details: dict[str, Any] | None = None,
        error: str = "",
        error_code: str = "sender_failed",
        echo_message_id: str = "",
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
                payload["error"] = {"code": error_code or "sender_failed", "message": error}
            self._append_event(conn, updated["account_id"], "send.updated", payload)
        return receipt

    def close(self) -> None:
        # Connections are deliberately short-lived so a process crash cannot keep a lock.
        return
