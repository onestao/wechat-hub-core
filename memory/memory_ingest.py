#!/usr/bin/env python3
"""Ingest decrypted WeChat 4.x SQLite shards into a local memory database.

This script only reads decrypted database copies and writes our own SQLite
memory store. It does not touch the original WeChat data directory.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

try:
    import zstandard as zstd
except ImportError:  # pragma: no cover - handled in runtime message
    zstd = None


MSG_TYPE_MAP = {
    1: "text",
    3: "image",
    34: "voice",
    42: "contact_card",
    43: "video",
    47: "sticker",
    48: "location",
    49: "link_or_file",
    50: "call",
    10000: "system",
    10002: "recall",
}


def split_msg_type(value: int | None) -> tuple[int, int]:
    try:
        msg_type = int(value or 0)
    except (TypeError, ValueError):
        return 0, 0
    if msg_type > 0xFFFFFFFF:
        return msg_type & 0xFFFFFFFF, msg_type >> 32
    return msg_type, 0


def msg_type_label(value: int | None) -> str:
    base, _ = split_msg_type(value)
    return MSG_TYPE_MAP.get(base, f"type_{value or 0}")


def decompress_content(content, compression_type) -> str | None:
    if content is None:
        return None
    if compression_type == 4 and isinstance(content, bytes):
        if zstd is None:
            return None
        try:
            return zstd.ZstdDecompressor().decompress(content).decode("utf-8", errors="replace")
        except Exception:
            return None
    if isinstance(content, bytes):
        try:
            return content.decode("utf-8", errors="replace")
        except UnicodeDecodeError:
            return None
    return str(content)


def content_hash(*parts) -> str:
    h = hashlib.sha256()
    for part in parts:
        if part is None:
            h.update(b"\x00")
        elif isinstance(part, bytes):
            h.update(part)
        else:
            h.update(str(part).encode("utf-8", errors="replace"))
        h.update(b"\x1f")
    return h.hexdigest()


def source_message_identity(chat_username: str, table_name: str, local_id: int) -> str:
    """Return the stable source identity for one WeChat message row.

    ``server_id`` and ACK/status fields are deliberately excluded because
    WeChat mutates them after the first local insert.  A resolved chat username
    is preferred; the physical message table is only the fallback identity.
    """

    chat = str(chat_username or "").strip()
    table = str(table_name or "").strip()
    scope = f"chat:{chat}" if chat else f"table:{table}"
    return f"{scope}:local:{int(local_id)}"


def _migrate_source_identities(conn: sqlite3.Connection) -> None:
    """Dedupe pre-migration rows while preserving the first exposed UID."""

    previous_factory = conn.row_factory
    conn.row_factory = sqlite3.Row
    try:
        columns = {str(row[1]) for row in conn.execute("PRAGMA table_info(messages)").fetchall()}
        if "source_identity" not in columns:
            conn.execute("ALTER TABLE messages ADD COLUMN source_identity TEXT NOT NULL DEFAULT ''")

        rows = conn.execute(
            "SELECT rowid, message_uid, chat_username, message_table, local_id FROM messages WHERE source_identity=''"
        ).fetchall()
        for row in rows:
            # Older staging rows stored ``message_table`` in chat_username when
            # Name2Id could not resolve a username. Recover that fallback
            # distinction where possible.
            resolved_chat = str(row["chat_username"] or "")
            if resolved_chat == str(row["message_table"] or ""):
                resolved_chat = ""
            identity = source_message_identity(resolved_chat, str(row["message_table"] or ""), int(row["local_id"]))
            conn.execute(
                "UPDATE messages SET source_identity=? WHERE rowid=?",
                (identity, int(row["rowid"])),
            )

        duplicate_groups = conn.execute(
            """
            SELECT source_identity
            FROM messages
            WHERE source_identity<>''
            GROUP BY source_identity
            HAVING COUNT(*)>1
            """
        ).fetchall()
        mutable_columns = (
            "chat_username",
            "chat_display_name",
            "message_table",
            "source_message_db",
            "local_id",
            "server_id",
            "local_type",
            "base_type",
            "app_subtype",
            "type_label",
            "sort_seq",
            "real_sender_id",
            "create_time",
            "status",
            "upload_status",
            "download_status",
            "server_seq",
            "origin_source",
            "source",
            "message_content",
            "compress_content",
            "content_sha256",
            "packed_info_sha256",
            "ingested_at",
        )
        for group in duplicate_groups:
            duplicates = conn.execute(
                "SELECT rowid, * FROM messages WHERE source_identity=? ORDER BY rowid ASC",
                (group["source_identity"],),
            ).fetchall()
            if len(duplicates) < 2:
                continue
            canonical = duplicates[0]

            def score(row: sqlite3.Row) -> tuple[int, int, str, int]:
                acked = int(row["server_id"] not in (None, "", 0, "0"))
                complete = sum(
                    value not in (None, "", 0, "0")
                    for value in (
                        row["server_id"],
                        row["status"],
                        row["upload_status"],
                        row["download_status"],
                        row["server_seq"],
                        row["message_content"],
                        row["compress_content"],
                        row["packed_info_sha256"],
                    )
                )
                return acked, complete, str(row["ingested_at"] or ""), int(row["rowid"])

            best = max(duplicates, key=score)
            assignments = ", ".join(f"{name}=?" for name in mutable_columns)
            conn.execute(
                f"UPDATE messages SET {assignments} WHERE rowid=?",
                tuple(best[name] for name in mutable_columns) + (int(canonical["rowid"]),),
            )

            canonical_uid = str(canonical["message_uid"])
            for duplicate in duplicates[1:]:
                duplicate_uid = str(duplicate["message_uid"])
                canonical_media = conn.execute(
                    "SELECT * FROM message_media WHERE message_uid=?", (canonical_uid,)
                ).fetchone()
                duplicate_media = conn.execute(
                    "SELECT * FROM message_media WHERE message_uid=?", (duplicate_uid,)
                ).fetchone()
                if duplicate_media is not None and canonical_media is None:
                    conn.execute(
                        """
                        UPDATE message_media
                        SET message_uid=?, chat_username=?, local_id=?
                        WHERE message_uid=?
                        """,
                        (
                            canonical_uid,
                            str(canonical["chat_username"]),
                            int(canonical["local_id"]),
                            duplicate_uid,
                        ),
                    )
                elif duplicate_media is not None and canonical_media is not None:
                    use_duplicate = (
                        str(duplicate_media["status"] or "") == "ready"
                        and str(canonical_media["status"] or "") != "ready"
                    ) or str(duplicate_media["updated_at"] or "") > str(canonical_media["updated_at"] or "")
                    if use_duplicate:
                        media_columns = (
                            "media_type",
                            "original_md5",
                            "source_path",
                            "media_path",
                            "thumb_path",
                            "mime_type",
                            "width",
                            "height",
                            "status",
                            "error",
                            "updated_at",
                        )
                        media_assignments = ", ".join(f"{name}=?" for name in media_columns)
                        conn.execute(
                            f"UPDATE message_media SET {media_assignments} WHERE message_uid=?",
                            tuple(duplicate_media[name] for name in media_columns) + (canonical_uid,),
                        )
                    conn.execute("DELETE FROM message_media WHERE message_uid=?", (duplicate_uid,))
                conn.execute("DELETE FROM messages WHERE rowid=?", (int(duplicate["rowid"]),))
    finally:
        conn.row_factory = previous_factory


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def open_readonly(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.execute("PRAGMA query_only=ON")
    return conn


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,),
    ).fetchone()
    return bool(row)


def load_name2id(message_db: Path) -> dict[str, str]:
    with open_readonly(message_db) as conn:
        if not table_exists(conn, "Name2Id"):
            return {}
        return {
            f"Msg_{hashlib.md5(username.encode()).hexdigest()}": username
            for (username,) in conn.execute("SELECT user_name FROM Name2Id")
            if username
        }


def load_contact_names(contact_db: Path | None) -> dict[str, dict[str, str]]:
    if not contact_db or not contact_db.exists():
        return {}
    with open_readonly(contact_db) as conn:
        if not table_exists(conn, "contact"):
            return {}
        rows = conn.execute(
            """
            SELECT username, remark, nick_name, alias, local_type
            FROM contact
            """
        ).fetchall()
    contacts = {}
    for username, remark, nick_name, alias, local_type in rows:
        if not username:
            continue
        display_name = remark or nick_name or alias or username
        contacts[username] = {
            "remark": remark or "",
            "nick_name": nick_name or "",
            "alias": alias or "",
            "display_name": display_name,
            "local_type": local_type,
        }
    return contacts


def load_sessions(session_db: Path | None) -> dict[str, dict]:
    if not session_db or not session_db.exists():
        return {}
    with open_readonly(session_db) as conn:
        if not table_exists(conn, "SessionTable"):
            return {}
        rows = conn.execute(
            """
            SELECT username, type, unread_count, is_hidden, status,
                   last_timestamp, sort_timestamp, last_msg_locald_id,
                   last_msg_type, last_msg_sub_type, last_msg_sender
            FROM SessionTable
            """
        ).fetchall()
    sessions = {}
    for row in rows:
        username = row[0]
        if not username:
            continue
        sessions[username] = {
            "type": row[1],
            "unread_count": row[2],
            "is_hidden": row[3],
            "status": row[4],
            "last_timestamp": row[5],
            "sort_timestamp": row[6],
            "last_msg_local_id": row[7],
            "last_msg_type": row[8],
            "last_msg_sub_type": row[9],
            "last_msg_sender": row[10] or "",
        }
    return sessions


def init_memory_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        PRAGMA journal_mode=DELETE;
        PRAGMA foreign_keys=ON;

        CREATE TABLE IF NOT EXISTS chats (
            username TEXT PRIMARY KEY,
            display_name TEXT,
            is_group INTEGER NOT NULL DEFAULT 0,
            session_type INTEGER,
            last_timestamp INTEGER,
            sort_timestamp INTEGER,
            message_table TEXT,
            source_message_db TEXT,
            updated_at TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS messages (
            message_uid TEXT PRIMARY KEY,
            source_identity TEXT NOT NULL DEFAULT '',
            chat_username TEXT NOT NULL,
            chat_display_name TEXT,
            message_table TEXT NOT NULL,
            source_message_db TEXT NOT NULL,
            local_id INTEGER NOT NULL,
            server_id INTEGER,
            local_type INTEGER,
            base_type INTEGER,
            app_subtype INTEGER,
            type_label TEXT,
            sort_seq INTEGER,
            real_sender_id INTEGER,
            create_time INTEGER,
            status INTEGER,
            upload_status INTEGER,
            download_status INTEGER,
            server_seq INTEGER,
            origin_source INTEGER,
            source TEXT,
            message_content TEXT,
            compress_content TEXT,
            content_sha256 TEXT NOT NULL,
            packed_info_sha256 TEXT,
            ingested_at TEXT NOT NULL,
            FOREIGN KEY(chat_username) REFERENCES chats(username)
        );

        CREATE INDEX IF NOT EXISTS idx_messages_chat_time
            ON messages(chat_username, create_time, local_id);
        CREATE INDEX IF NOT EXISTS idx_messages_type
            ON messages(type_label);
        CREATE INDEX IF NOT EXISTS idx_messages_content_hash
            ON messages(content_sha256);

        CREATE TABLE IF NOT EXISTS message_media (
            message_uid TEXT PRIMARY KEY,
            chat_username TEXT NOT NULL,
            local_id INTEGER NOT NULL,
            media_type TEXT NOT NULL,
            original_md5 TEXT,
            source_path TEXT,
            media_path TEXT,
            thumb_path TEXT,
            mime_type TEXT,
            width INTEGER,
            height INTEGER,
            status TEXT NOT NULL,
            error TEXT,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(message_uid) REFERENCES messages(message_uid)
        );

        CREATE INDEX IF NOT EXISTS idx_message_media_chat
            ON message_media(chat_username, local_id);
        CREATE INDEX IF NOT EXISTS idx_message_media_status
            ON message_media(status);

        CREATE TABLE IF NOT EXISTS ingest_runs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            started_at TEXT NOT NULL,
            finished_at TEXT,
            source_dir TEXT NOT NULL,
            message_count INTEGER NOT NULL DEFAULT 0,
            chat_count INTEGER NOT NULL DEFAULT 0,
            details_json TEXT NOT NULL DEFAULT '{}'
        );
        """
    )
    _migrate_source_identities(conn)
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_messages_source_identity ON messages(source_identity)"
    )


def prune_ingest_runs(conn: sqlite3.Connection, keep: int = 1000) -> int:
    keep = max(100, int(keep or 1000))
    before = conn.execute("SELECT COUNT(*) FROM ingest_runs").fetchone()[0]
    conn.execute(
        """
        DELETE FROM ingest_runs
        WHERE id NOT IN (
            SELECT id FROM ingest_runs
            ORDER BY id DESC
            LIMIT ?
        )
        """,
        (keep,),
    )
    after = conn.execute("SELECT COUNT(*) FROM ingest_runs").fetchone()[0]
    return max(0, int(before or 0) - int(after or 0))


def iter_message_tables(message_db: Path):
    table_to_username = load_name2id(message_db)
    with open_readonly(message_db) as conn:
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'Msg_%' ORDER BY name"
        ).fetchall()
    for (table_name,) in rows:
        yield table_name, table_to_username.get(table_name, "")


def ingest_chat(
    out_conn: sqlite3.Connection,
    chat_username: str,
    table_name: str,
    message_db: Path,
    contacts: dict[str, dict[str, str]],
    sessions: dict[str, dict],
) -> int:
    display_name = contacts.get(chat_username, {}).get("display_name") or chat_username or table_name
    session = sessions.get(chat_username, {})
    is_group = 1 if chat_username.endswith("@chatroom") else 0
    now = utc_now_iso()

    out_conn.execute(
        """
        INSERT INTO chats (
            username, display_name, is_group, session_type, last_timestamp,
            sort_timestamp, message_table, source_message_db, updated_at
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(username) DO UPDATE SET
            display_name=excluded.display_name,
            is_group=excluded.is_group,
            session_type=excluded.session_type,
            last_timestamp=excluded.last_timestamp,
            sort_timestamp=excluded.sort_timestamp,
            message_table=excluded.message_table,
            source_message_db=excluded.source_message_db,
            updated_at=excluded.updated_at
        """,
        (
            chat_username or table_name,
            display_name,
            is_group,
            session.get("type"),
            session.get("last_timestamp"),
            session.get("sort_timestamp"),
            table_name,
            str(message_db),
            now,
        ),
    )

    inserted = 0
    with open_readonly(message_db) as msg_conn:
        rows = msg_conn.execute(
            f"""
            SELECT local_id, server_id, local_type, sort_seq, real_sender_id,
                   create_time, status, upload_status, download_status,
                   server_seq, origin_source, source, message_content,
                   compress_content, packed_info_data, WCDB_CT_message_content
            FROM [{table_name}]
            ORDER BY create_time, local_id
            """
        )
        for row in rows:
            (
                local_id,
                server_id,
                local_type,
                sort_seq,
                real_sender_id,
                create_time,
                status,
                upload_status,
                download_status,
                server_seq,
                origin_source,
                source,
                message_content,
                compress_content,
                packed_info_data,
                content_ct,
            ) = row
            decoded_content = decompress_content(message_content, content_ct)
            decoded_compress = decompress_content(compress_content, None)
            base_type, app_subtype = split_msg_type(local_type)
            identity = source_message_identity(chat_username, table_name, local_id)
            existing = out_conn.execute(
                "SELECT message_uid, content_sha256, packed_info_sha256 FROM messages WHERE source_identity=?",
                (identity,),
            ).fetchone()
            uid = str(existing[0]) if existing is not None else content_hash(identity)
            body_hash = content_hash(decoded_content, decoded_compress, source, local_type)
            packed_hash = content_hash(packed_info_data) if packed_info_data is not None else None

            before = existing[1:3] if existing is not None else None
            out_conn.execute(
                """
                INSERT INTO messages (
                    message_uid, source_identity, chat_username, chat_display_name, message_table,
                    source_message_db, local_id, server_id, local_type, base_type,
                    app_subtype, type_label, sort_seq, real_sender_id, create_time,
                    status, upload_status, download_status, server_seq, origin_source,
                    source, message_content, compress_content, content_sha256,
                    packed_info_sha256, ingested_at
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(message_uid) DO UPDATE SET
                    source_identity=excluded.source_identity,
                    chat_display_name=excluded.chat_display_name,
                    server_id=excluded.server_id,
                    local_type=excluded.local_type,
                    base_type=excluded.base_type,
                    app_subtype=excluded.app_subtype,
                    type_label=excluded.type_label,
                    sort_seq=excluded.sort_seq,
                    real_sender_id=excluded.real_sender_id,
                    create_time=excluded.create_time,
                    status=excluded.status,
                    upload_status=excluded.upload_status,
                    download_status=excluded.download_status,
                    server_seq=excluded.server_seq,
                    origin_source=excluded.origin_source,
                    source=excluded.source,
                    message_content=excluded.message_content,
                    compress_content=excluded.compress_content,
                    content_sha256=excluded.content_sha256,
                    packed_info_sha256=excluded.packed_info_sha256,
                    ingested_at=excluded.ingested_at
                """,
                (
                    uid,
                    identity,
                    chat_username or table_name,
                    display_name,
                    table_name,
                    str(message_db),
                    local_id,
                    server_id,
                    local_type,
                    base_type,
                    app_subtype,
                    msg_type_label(local_type),
                    sort_seq,
                    real_sender_id,
                    create_time,
                    status,
                    upload_status,
                    download_status,
                    server_seq,
                    origin_source,
                    source,
                    decoded_content,
                    decoded_compress,
                    body_hash,
                    packed_hash,
                    now,
                ),
            )
            after_hash = (body_hash, packed_hash)
            if before is None or tuple(before) != after_hash:
                inserted += 1
    return inserted


def ingest_session_chats(
    out_conn: sqlite3.Connection,
    sessions: dict[str, dict],
    contacts: dict[str, dict[str, str]],
    discovered_usernames: set[str],
    session_db: Path | None,
) -> int:
    """Preserve visible sessions even when WeChat has not materialized Msg_* tables yet."""
    inserted = 0
    now = utc_now_iso()
    for username, session in sessions.items():
        if not username or username in discovered_usernames:
            continue
        display_name = contacts.get(username, {}).get("display_name") or username
        out_conn.execute(
            """
            INSERT INTO chats (
                username, display_name, is_group, session_type, last_timestamp,
                sort_timestamp, message_table, source_message_db, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(username) DO UPDATE SET
                display_name=excluded.display_name,
                is_group=excluded.is_group,
                session_type=excluded.session_type,
                last_timestamp=excluded.last_timestamp,
                sort_timestamp=excluded.sort_timestamp,
                updated_at=excluded.updated_at
            """,
            (
                username,
                display_name,
                1 if username.endswith("@chatroom") else 0,
                session.get("type"),
                session.get("last_timestamp"),
                session.get("sort_timestamp"),
                "",
                str(session_db or ""),
                now,
            ),
        )
        inserted += 1
    return inserted


def resolve_paths(decrypted_dir: Path) -> tuple[list[Path], Path | None, Path | None]:
    message_dir = decrypted_dir / "message"
    message_dbs = sorted(message_dir.glob("message_[0-9]*.db"))
    contact_db = decrypted_dir / "contact" / "contact.db"
    session_db = decrypted_dir / "session" / "session.db"
    return message_dbs, contact_db if contact_db.exists() else None, session_db if session_db.exists() else None


def ingest_memory(decrypted_dir: Path, memory_db: Path, dry_run: bool = False) -> dict:
    root = Path.cwd()
    if not decrypted_dir.is_absolute():
        decrypted_dir = root / decrypted_dir
    if not memory_db.is_absolute():
        memory_db = root / memory_db

    message_dbs, contact_db, session_db = resolve_paths(decrypted_dir)
    if not message_dbs:
        raise RuntimeError(f"No message_N.db files found under {decrypted_dir}")

    contacts = load_contact_names(contact_db)
    sessions = load_sessions(session_db)

    discovered = []
    for message_db in message_dbs:
        for table_name, chat_username in iter_message_tables(message_db):
            with open_readonly(message_db) as conn:
                count = conn.execute(f"SELECT COUNT(*) FROM [{table_name}]").fetchone()[0]
            discovered.append(
                {
                    "message_db": str(message_db),
                    "table_name": table_name,
                    "chat_username": chat_username or table_name,
                    "row_count": int(count or 0),
                }
            )

    if dry_run:
        return {"tables": discovered}

    memory_db.parent.mkdir(parents=True, exist_ok=True)
    started_at = utc_now_iso()
    started_mono = time.time()
    with sqlite3.connect(memory_db, timeout=15) as out_conn:
        out_conn.execute("PRAGMA busy_timeout=15000")
        init_memory_db(out_conn)
        cur = out_conn.execute(
            "INSERT INTO ingest_runs (started_at, source_dir, details_json) VALUES (?, ?, ?)",
            (started_at, str(decrypted_dir), json.dumps({"tables": discovered}, ensure_ascii=False)),
        )
        run_id = cur.lastrowid

        total = 0
        discovered_usernames = {str(item["chat_username"]) for item in discovered}
        for item in discovered:
            inserted = ingest_chat(
                out_conn,
                item["chat_username"],
                item["table_name"],
                Path(item["message_db"]),
                contacts,
                sessions,
            )
            total += inserted
        session_only_chats = ingest_session_chats(
            out_conn,
            sessions,
            contacts,
            discovered_usernames,
            session_db,
        )

        chat_count = out_conn.execute("SELECT COUNT(*) FROM chats").fetchone()[0]
        message_count = out_conn.execute("SELECT COUNT(*) FROM messages").fetchone()[0]
        type_rows = out_conn.execute(
            "SELECT type_label, COUNT(*) FROM messages GROUP BY type_label ORDER BY COUNT(*) DESC"
        ).fetchall()
        out_conn.execute(
            """
            UPDATE ingest_runs
            SET finished_at=?, message_count=?, chat_count=?, details_json=?
            WHERE id=?
            """,
            (
                utc_now_iso(),
                message_count,
                chat_count,
                json.dumps(
                    {
                        "tables": discovered,
                        "session_only_chats": session_only_chats,
                        "upserted_rows": total,
                        "elapsed_seconds": round(time.time() - started_mono, 3),
                        "type_counts": dict(type_rows),
                    },
                    ensure_ascii=False,
                ),
                run_id,
            ),
        )
        pruned_runs = prune_ingest_runs(out_conn)

    os.chmod(memory_db, 0o600)
    return {
        "memory_db": str(memory_db),
        "chats": chat_count,
        "messages": message_count,
        "changed_rows": total,
        "session_only_chats": session_only_chats,
        "pruned_ingest_runs": pruned_runs,
        "type_counts": dict(type_rows),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest decrypted WeChat messages into local memory DB")
    parser.add_argument(
        "--decrypted-dir",
        default="runtime/wechat-decrypt/decrypted",
        help="Directory produced by decrypt_db.py",
    )
    parser.add_argument(
        "--memory-db",
        default="runtime/memory/wechat_memory.sqlite",
        help="Output SQLite memory database",
    )
    parser.add_argument("--dry-run", action="store_true", help="Inspect counts without writing")
    args = parser.parse_args(argv)

    try:
        result = ingest_memory(Path(args.decrypted_dir), Path(args.memory_db), dry_run=args.dry_run)
    except RuntimeError as exc:
        print(f"[ERROR] {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
