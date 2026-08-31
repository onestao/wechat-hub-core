"""Import existing per-account linux-wechat-agent staging data into Core V1."""

from __future__ import annotations

import json
import mimetypes
import re
import sqlite3
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from memory.message_parse import message_display_parts

from .registry import AccountConfig
from .store import CoreStore, utc_now


TYPE_MAP = {
    "text": "text",
    "image": "image",
    "sticker": "sticker",
    "voice": "voice",
    "video": "video",
    "link_or_file": "file",
    "link": "link",
    "location": "location",
    "contact_card": "contact_card",
    "system": "system",
    "recall": "recall",
}
SAFE_MEMBER_ID = re.compile(r"[A-Za-z0-9_@.\-]{2,80}\Z")


def _columns(conn: sqlite3.Connection, table: str) -> set[str]:
    return {str(row[1]) for row in conn.execute(f"PRAGMA table_info([{table}])").fetchall()}


@contextmanager
def sqlite_connection(path: Path):
    conn = sqlite3.connect(path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _table(conn: sqlite3.Connection, name: str) -> bool:
    return bool(conn.execute("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)).fetchone())


def _value(row: sqlite3.Row, name: str, default: Any = "") -> Any:
    return row[name] if name in row.keys() and row[name] is not None else default


def _timestamp(value: Any) -> str:
    try:
        seconds = int(value)
        if seconds > 0:
            return datetime.fromtimestamp(seconds, timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
    except (TypeError, ValueError, OSError):
        pass
    return utc_now()


def _safe_text(value: Any, limit: int = 120) -> str:
    text = str(value or "").replace("\x00", " ").strip()
    return re.sub(r"\s+", " ", text)[:limit]


def decode_chatroom_members_buffer(buffer: bytes | None) -> dict[str, str]:
    """Extracted from agent_console.daily_report for Core-owned member import."""
    if not buffer:
        return {}
    data = bytes(buffer)
    members: dict[str, str] = {}
    index = 0
    for _ in range(600):
        start = data.find(b"\n", index)
        if start < 0 or start + 2 >= len(data):
            break
        length = data[start + 1]
        name_start = start + 2
        name_end = name_start + length
        if length <= 0 or name_end > len(data):
            index = start + 1
            continue
        try:
            member_id = data[name_start:name_end].decode("utf-8")
        except UnicodeDecodeError:
            index = start + 1
            continue
        if not SAFE_MEMBER_ID.fullmatch(member_id):
            index = start + 1
            continue
        alias = ""
        if name_end + 2 <= len(data) and data[name_end] == 0x12:
            alias_end = name_end + 2 + data[name_end + 1]
            if data[name_end + 1] and alias_end <= len(data):
                try:
                    alias = _safe_text(data[name_end + 2:alias_end].decode("utf-8"), 80)
                except UnicodeDecodeError:
                    pass
        members[member_id] = alias
        index = name_end
    return members


def _load_contacts(contact_db: Path) -> dict[str, dict[str, str]]:
    if not contact_db.exists():
        return {}
    try:
        with sqlite_connection(contact_db) as conn:
            conn.row_factory = sqlite3.Row
            if not _table(conn, "contact"):
                return {}
            rows = conn.execute("SELECT username, remark, nick_name, alias FROM contact").fetchall()
    except sqlite3.Error:
        return {}
    return {
        str(row["username"]): {
            "member_id": str(row["username"]),
            "display_name": _safe_text(row["remark"] or row["nick_name"] or row["alias"] or row["username"]),
            "alias": _safe_text(row["alias"], 80),
            "remark": _safe_text(row["remark"], 80),
        }
        for row in rows
        if row["username"]
    }


def _load_group_members(contact_db: Path, chat_id: str, contacts: dict[str, dict[str, str]]) -> list[dict[str, Any]]:
    if not contact_db.exists():
        return []
    aliases: dict[str, str] = {}
    rows: list[sqlite3.Row] = []
    try:
        with sqlite_connection(contact_db) as conn:
            conn.row_factory = sqlite3.Row
            if _table(conn, "chat_room"):
                room = conn.execute("SELECT ext_buffer FROM chat_room WHERE username=?", (chat_id,)).fetchone()
                if room:
                    aliases = decode_chatroom_members_buffer(room["ext_buffer"])
            if _table(conn, "chatroom_member") and _table(conn, "chat_room") and _table(conn, "contact"):
                rows = conn.execute(
                    """
                    SELECT c.username, c.remark, c.nick_name, c.alias
                    FROM chatroom_member cm
                    JOIN chat_room r ON r.id=cm.room_id
                    JOIN contact c ON c.id=cm.member_id
                    WHERE r.username=?
                    """,
                    (chat_id,),
                ).fetchall()
    except sqlite3.Error:
        return []
    members: dict[str, dict[str, Any]] = {}
    for row in rows:
        member_id = str(row["username"] or "")
        if member_id:
            members[member_id] = {
                "member_id": member_id,
                "display_name": _safe_text(aliases.get(member_id) or row["remark"] or row["nick_name"] or row["alias"] or member_id),
                "alias": _safe_text(row["alias"], 80),
                "is_self": False,
            }
    for member_id, group_alias in aliases.items():
        contact = contacts.get(member_id, {})
        members.setdefault(
            member_id,
            {
                "member_id": member_id,
                "display_name": _safe_text(group_alias or contact.get("display_name") or member_id),
                "alias": _safe_text(contact.get("alias"), 80),
                "is_self": False,
            },
        )
    return list(members.values())


def _normalized_message(
    account_id: str,
    row: sqlite3.Row,
    contacts: dict[str, dict[str, str]],
    media: dict[str, str] | None = None,
) -> dict[str, Any]:
    chat_id = str(_value(row, "chat_username") or _value(row, "message_table"))
    raw_type = str(_value(row, "type_label", "unsupported"))
    type_name = TYPE_MAP.get(raw_type, "unsupported")
    content = str(_value(row, "message_content"))
    compressed = str(_value(row, "compress_content"))
    parts = message_display_parts(content, compressed, raw_type, str(_value(row, "source")))
    if raw_type == "link_or_file" and parts.get("app_url"):
        type_name = "link"
    sender_hint = str(parts.get("sender_hint") or "")
    try:
        origin_source = int(_value(row, "origin_source", 0) or 0)
    except (TypeError, ValueError):
        origin_source = 0
    is_self = origin_source == 1 and not sender_hint
    direction = "outgoing" if is_self else "incoming"
    # Group incoming messages carry a sender prefix.  Private incoming messages
    # do not, so the chat username is the best stable member identity available
    # from the upstream staging database.
    member_id = "self" if is_self else (sender_hint or chat_id)
    contact = contacts.get(member_id, {})
    author = {
        "member_id": member_id,
        "display_name": _safe_text(contact.get("display_name") or member_id),
        "is_self": is_self,
    }
    if contact.get("alias"):
        author["alias"] = contact["alias"]
    text = _safe_text(parts.get("semantic_text"), 3000) if type_name not in {"image", "sticker", "video", "voice"} else ""
    message_id = str(_value(row, "message_uid") or _value(row, "server_id") or f"local-{_value(row, 'local_id')}")
    vendor_specific = {
        "source_type_label": raw_type,
        "source_local_id": _value(row, "local_id", None),
        "source_server_id": _value(row, "server_id", None),
        "source_message_table": _value(row, "message_table", ""),
        "source_origin_source": origin_source,
        "source_real_sender_id": _value(row, "real_sender_id", None),
        "direction_inferred_from": "group_sender_prefix" if sender_hint else "origin_source",
    }
    attributes = {
        key: value
        for key, value in {
            "semantic_type": parts.get("semantic_type"),
            "app_type": parts.get("app_type"),
            "app_url": parts.get("app_url"),
            "app_id": parts.get("app_id"),
            "app_source_name": parts.get("app_source_name"),
        }.items()
        if value
    }
    normalized = {
        "account_id": account_id,
        "message_id": message_id,
        "chat_id": chat_id,
        "type": type_name,
        "direction": direction,
        "created_at": _timestamp(_value(row, "create_time", 0)),
        "author": author,
        "text": text,
        "attributes": attributes,
        "vendor_specific": vendor_specific,
    }
    if media:
        normalized.update(
            {
                "media_id": media["media_id"],
                "filename": media["filename"],
                "mime_type": media["mime_type"],
            }
        )
    return normalized


def _resolve_media_path(account: AccountConfig, raw_path: str) -> Path | None:
    if not raw_path:
        return None
    candidate = Path(raw_path)
    if not candidate.is_absolute():
        candidate = account.runtime_dir / candidate
    try:
        resolved = candidate.resolve()
        resolved.relative_to(account.runtime_dir.resolve())
    except (OSError, ValueError):
        return None
    return resolved if resolved.exists() and resolved.is_file() else None


def import_account(account: AccountConfig, store: CoreStore) -> dict[str, int]:
    """Normalize the upstream per-account staging SQLite output into Core tables."""
    summary = {"chats": 0, "messages": 0, "message_changes": 0, "contacts": 0, "members": 0, "media": 0}
    if not account.memory_db.exists():
        raise RuntimeError(f"staging memory database does not exist: {account.memory_db}")
    contact_db = account.decrypted_dir / "contact" / "contact.db"
    contacts = _load_contacts(contact_db)
    for contact in contacts.values():
        store.upsert_contact(account.account_id, contact)
        summary["contacts"] += 1
    with sqlite_connection(account.memory_db) as conn:
        conn.row_factory = sqlite3.Row
        if not _table(conn, "chats") or not _table(conn, "messages"):
            raise RuntimeError("upstream staging database lacks chats or messages tables")
        chat_rows = conn.execute("SELECT * FROM chats ORDER BY username").fetchall()
        for row in chat_rows:
            chat_id = str(_value(row, "username") or _value(row, "message_table"))
            type_name = "group" if int(_value(row, "is_group", 0) or 0) else "private"
            store.upsert_chat(
                {
                    "account_id": account.account_id,
                    "chat_id": chat_id,
                    "type": type_name,
                    "display_name": _safe_text(_value(row, "display_name") or chat_id),
                    "updated_at": str(_value(row, "updated_at") or utc_now()),
                    "vendor_specific": {"source_message_table": _value(row, "message_table", "")},
                }
            )
            summary["chats"] += 1
            if type_name == "group":
                for member in _load_group_members(contact_db, chat_id, contacts):
                    store.upsert_member(account.account_id, chat_id, member)
                    summary["members"] += 1
                refreshed = {
                    "account_id": account.account_id,
                    "chat_id": chat_id,
                    "type": type_name,
                    "display_name": _safe_text(_value(row, "display_name") or chat_id),
                    "member_count": store.member_count(account.account_id, chat_id),
                    "updated_at": str(_value(row, "updated_at") or utc_now()),
                    "vendor_specific": {"source_message_table": _value(row, "message_table", "")},
                }
                store.upsert_chat(refreshed)
        media_by_message: dict[str, dict[str, str]] = {}
        if _table(conn, "message_media"):
            media_rows = conn.execute("SELECT * FROM message_media WHERE status='ready'").fetchall()
            for row in media_rows:
                path = _resolve_media_path(account, str(_value(row, "media_path") or _value(row, "thumb_path")))
                if not path:
                    continue
                media_id = str(_value(row, "message_uid"))
                if not media_id:
                    continue
                mime_type = str(
                    _value(row, "mime_type")
                    or mimetypes.guess_type(path.name)[0]
                    or "application/octet-stream"
                )
                store.upsert_media(
                    {
                        "account_id": account.account_id,
                        "media_id": media_id,
                        "filename": path.name,
                        "mime_type": mime_type,
                        "local_path": str(path),
                        "disposition": "inline",
                        "status": "ready",
                    }
                )
                media_by_message[media_id] = {
                    "media_id": media_id,
                    "filename": path.name,
                    "mime_type": mime_type,
                }
                summary["media"] += 1
        message_rows = conn.execute("SELECT * FROM messages ORDER BY create_time, local_id").fetchall()
        for row in message_rows:
            message_id = str(_value(row, "message_uid") or _value(row, "server_id") or f"local-{_value(row, 'local_id')}")
            result = store.upsert_message(
                _normalized_message(
                    account.account_id,
                    row,
                    contacts,
                    media=media_by_message.get(message_id),
                )
            )
            summary["messages"] += 1
            summary["message_changes"] += int(result != "unchanged")
    return summary
