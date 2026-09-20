"""AgentWechat client and canonical normalization for WeChat Hub Core.

Authoritative boundary:
- agent-wechat HTTP API is the sole real-time message and media source for agent_wechat provider.
- Core handles canonicalization, ownership, durable events, outbox, and media cache.
"""

from __future__ import annotations

import base64
import hashlib
import json
import logging
import mimetypes
import re
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

CANONICAL_KINDS = {
    "text",
    "image",
    "sticker",
    "voice",
    "video",
    "file",
    "link",
    "reply",
    "system",
    "unknown",
}


class AgentWechatError(Exception):
    """Base typed exception for AgentWechat client operations."""

    def __init__(
        self,
        message: str,
        status_code: int = 500,
        code: str = "agent_wechat_error",
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.details = details or {}


def content_hash(*parts: Any) -> str:
    """Compute content SHA-256 hash strictly matching legacy memory_ingest.content_hash."""
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


def canonical_message_id(chat_id: str, local_id: int | str) -> str:
    """Generate deterministic message ID strictly identical to legacy source_message_identity hash.

    Identity string: f"chat:{chat_id}:local:{local_id}"
    Result: content_hash(identity)
    """
    scope = f"chat:{str(chat_id).strip()}"
    identity = f"{scope}:local:{int(local_id)}"
    return content_hash(identity)


def _extract_xml_tag(xml: str, tag: str) -> str | None:
    open_tag = f"<{tag}>"
    close_tag = f"</{tag}>"
    start = xml.find(open_tag)
    if start == -1:
        return None
    start += len(open_tag)
    end = xml.find(close_tag, start)
    if end == -1:
        return None
    val = xml[start:end].strip()
    if val.startswith("<![CDATA[") and val.endswith("]]>"):
        val = val[9:-3].strip()
    return val if val else None


def _extract_xml_attr(xml: str, attr: str) -> str | None:
    pattern = rf'{attr}="([^"]*)"'
    m = re.search(pattern, xml)
    return m.group(1).strip() if m else None


def map_agent_message_kind(
    raw_type: int,
    content: str,
    reply: Any = None,
    explicit_kind: str = "",
    explicit_subtype: int | None = None,
) -> tuple[str, int | None, str]:
    """Map agent-wechat raw message fields to canonical (kind, subtype, filename).

    kind strictly in: text, image, sticker, voice, video, file, link, reply, system, unknown.
    """
    if explicit_kind and explicit_kind in CANONICAL_KINDS:
        return explicit_kind, explicit_subtype, ""

    base = raw_type & 0x7FFFFFFF
    subtype: int | None = explicit_subtype

    # Reply check
    if reply or (base == 49 and ("<refermsg>" in content or "<type>57</type>" in content)):
        return "reply", 57, ""

    if base == 1:
        return "text", None, ""
    if base == 3:
        return "image", None, ""
    if base == 34:
        return "voice", None, ""
    if base == 43:
        return "video", None, ""
    if base == 47:
        return "sticker", None, ""
    if base in (10000, 10002):
        return "system", None, ""

    if base == 49:
        # App message
        appmsg_type = _extract_xml_tag(content, "type")
        parsed_sub = None
        if appmsg_type:
            try:
                parsed_sub = int(appmsg_type)
            except (ValueError, TypeError):
                pass
        subtype = parsed_sub if parsed_sub is not None else explicit_subtype

        if subtype == 6 or "<type>6</type>" in content:
            # File
            title = _extract_xml_tag(content, "title") or ""
            return "file", 6, title
        if subtype in (3, 4, 5) or any(f"<type>{t}</type>" in content for t in (3, 4, 5)) or content.startswith("[Link]"):
            return "link", subtype or 5, ""
        if subtype == 57 or "<refermsg>" in content:
            return "reply", 57, ""

        return "unknown", subtype, ""

    return "unknown", None, ""


def parse_timestamp_iso(ts: Any) -> str:
    """Parse various timestamp formats (RFC3339 string, unix epoch int/float) into ISO 8601 UTC."""
    if isinstance(ts, (int, float)):
        # Handle seconds or milliseconds
        val = float(ts)
        if val > 1e11:  # milliseconds
            val /= 1000.0
        return datetime.fromtimestamp(val, tz=timezone.utc).isoformat()
    if isinstance(ts, str):
        ts_str = ts.strip()
        if not ts_str:
            return datetime.now(timezone.utc).isoformat()
        try:
            # Check if numeric string
            val = float(ts_str)
            if val > 1e11:
                val /= 1000.0
            return datetime.fromtimestamp(val, tz=timezone.utc).isoformat()
        except ValueError:
            pass
        # ISO string
        if ts_str.endswith("Z"):
            ts_str = ts_str[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(ts_str)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            return dt.astimezone(timezone.utc).isoformat()
        except Exception:
            return datetime.now(timezone.utc).isoformat()
    return datetime.now(timezone.utc).isoformat()


def normalize_agent_message(
    account_id: str,
    msg: dict[str, Any],
    bound_wxid: str = "",
    contacts: dict[str, dict[str, Any]] | None = None,
) -> dict[str, Any]:
    """Convert an agent-wechat API message into Core's canonical message structure."""
    local_id = int(msg["localId"])
    server_id = str(msg.get("serverId") or "")
    chat_id = str(msg.get("chatId") or "").strip()
    raw_type = int(msg.get("type") or 1)
    content = str(msg.get("content") or "")
    reply = msg.get("reply")
    explicit_kind = str(msg.get("kind") or "")
    explicit_subtype = msg.get("subtype")
    explicit_filename = str(msg.get("filename") or "")

    kind, subtype, detected_filename = map_agent_message_kind(
        raw_type,
        content,
        reply=reply,
        explicit_kind=explicit_kind,
        explicit_subtype=explicit_subtype,
    )

    filename = explicit_filename or detected_filename

    # Determine message ID via canonical SHA-256 hash (strict parity with legacy)
    message_id = canonical_message_id(chat_id, local_id)

    # Determine sender & direction
    sender = str(msg.get("sender") or "").strip()
    is_self_flag = bool(msg.get("isSelf"))
    if bound_wxid and sender and sender == bound_wxid:
        is_self = True
    elif is_self_flag:
        is_self = True
    else:
        is_self = False

    direction = "outgoing" if is_self else "incoming"
    member_id = "self" if is_self else (sender or chat_id)

    # Resolve display name
    sender_name = str(msg.get("senderName") or "").strip()
    if contacts and member_id in contacts:
        c = contacts[member_id]
        display_name = c.get("remark") or c.get("nickname") or c.get("display_name") or sender_name or member_id
    else:
        display_name = sender_name or member_id

    author = {
        "member_id": member_id,
        "display_name": display_name,
        "is_self": is_self,
    }

    # Text presentation: for pure media types, semantic text is empty
    if kind in {"image", "sticker", "video", "voice"}:
        text = ""
    else:
        text = content

    # Media handling: lazy resolution
    media_id = ""
    media_role = ""
    media_status = ""
    mime_type = ""
    if kind in {"image", "sticker", "video", "voice", "file"}:
        media_id = message_id
        media_role = "original"
        media_status = "pending"
        if filename:
            mime_type = mimetypes.guess_type(filename)[0] or "application/octet-stream"
        elif kind == "image":
            mime_type = "image/jpeg"
        elif kind == "sticker":
            mime_type = "image/gif"
        elif kind == "voice":
            mime_type = "audio/silk"
        elif kind == "video":
            mime_type = "video/mp4"

    created_at = parse_timestamp_iso(msg.get("timestamp"))

    vendor_specific: dict[str, Any] = {
        "provider": "agent_wechat",
        "source_local_id": local_id,
        "source_server_id": server_id if server_id else None,
        "source_type": raw_type,
        "source_subtype": subtype,
    }
    if media_id:
        vendor_specific["media"] = {
            "role": media_role,
            "status": media_status,
            "original_media_id": media_id,
            "thumbnail_media_id": "",
        }

    attributes: dict[str, Any] = {}
    if reply and isinstance(reply, dict):
        attributes["reply"] = {
            "sender": reply.get("sender") or "",
            "content": reply.get("content") or "",
        }
    if kind == "link":
        attributes["semantic_type"] = "link"
    elif kind == "file":
        attributes["semantic_type"] = "file"

    normalized: dict[str, Any] = {
        "account_id": account_id,
        "message_id": message_id,
        "chat_id": chat_id,
        "type": kind,
        "direction": direction,
        "created_at": created_at,
        "author": author,
        "text": text,
        "attributes": attributes,
        "vendor_specific": vendor_specific,
    }
    if media_id:
        normalized.update(
            {
                "media_id": media_id,
                "filename": filename,
                "mime_type": mime_type,
                "media_role": media_role,
                "media_status": media_status,
            }
        )
    return normalized


class AgentWechatClient:
    """Sole authoritative HTTP client for communication with agent-wechat server."""

    def __init__(self, base_url: str, token: str, timeout: float = 10.0) -> None:
        self.base_url = str(base_url or "").rstrip("/")
        self.token = str(token or "").strip()
        self.timeout = max(1.0, float(timeout))
        if not self.base_url:
            raise AgentWechatError("agent-wechat base_url is required", 400, "invalid_config")
        if not self.token:
            raise AgentWechatError("agent-wechat token is required", 400, "invalid_config")

    @classmethod
    def from_account(cls, account: Any, timeout: float = 10.0) -> AgentWechatClient:
        """Instantiate client from an AccountConfig instance."""
        runtime = getattr(account, "runtime", {}) or {}
        base_url = str(runtime.get("agent_wechat_base_url") or "").strip()
        token_file_path = str(runtime.get("agent_wechat_token_file") or "").strip()

        token = ""
        if token_file_path:
            token_path = Path(token_file_path)
            if token_path.is_file():
                try:
                    token = token_path.read_text(encoding="utf-8").strip()
                except OSError as exc:
                    raise AgentWechatError(
                        f"Failed reading agent-wechat token file {token_path}: {exc}",
                        500,
                        "token_file_error",
                    ) from exc

        if not base_url:
            raise AgentWechatError(
                f"agent_wechat_base_url is not configured for account {getattr(account, 'account_id', '')}",
                400,
                "missing_base_url",
            )
        if not token:
            raise AgentWechatError(
                f"agent_wechat token is missing or file empty for account {getattr(account, 'account_id', '')}",
                400,
                "missing_token",
            )
        return cls(base_url, token, timeout=timeout)

    def _request(
        self,
        path: str,
        method: str = "GET",
        data: bytes | None = None,
        query: dict[str, Any] | None = None,
        timeout: float | None = None,
    ) -> Any:
        url = f"{self.base_url}{path}"
        if query:
            clean_query = {k: v for k, v in query.items() if v is not None and v != ""}
            if clean_query:
                url = f"{url}?{urllib.parse.urlencode(clean_query)}"

        headers = {
            "Authorization": f"Bearer {self.token}",
            "Accept": "application/json",
        }
        if data is not None:
            headers["Content-Type"] = "application/json"

        req = urllib.request.Request(url, data=data, headers=headers, method=method)
        req_timeout = timeout if timeout is not None else self.timeout

        try:
            with urllib.request.urlopen(req, timeout=req_timeout) as resp:
                raw = resp.read(10 * 1024 * 1024 + 1)  # 10MB safety cap
                status_code = resp.status
        except urllib.error.HTTPError as exc:
            err_body = exc.read(64 * 1024).decode("utf-8", errors="replace").strip()
            if exc.code == 401:
                raise AgentWechatError(
                    f"agent-wechat unauthorized (HTTP 401): {err_body}",
                    401,
                    "unauthorized",
                ) from exc
            if exc.code == 404:
                raise AgentWechatError(
                    f"agent-wechat endpoint not found (HTTP 404): {url}",
                    404,
                    "not_found",
                ) from exc
            raise AgentWechatError(
                f"agent-wechat returned HTTP {exc.code}: {err_body}",
                exc.code,
                "upstream_http_error",
                details={"body": err_body},
            ) from exc
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            raise AgentWechatError(
                f"agent-wechat connection failed ({type(exc).__name__}): {exc}",
                504,
                "connection_failed",
            ) from exc

        if len(raw) > 10 * 1024 * 1024:
            raise AgentWechatError("agent-wechat response exceeded 10MB limit", 502, "payload_too_large")

        if not raw:
            return {}

        try:
            return json.loads(raw.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            raise AgentWechatError(
                f"agent-wechat returned non-JSON response: {exc}",
                502,
                "invalid_json",
            ) from exc

    def health(self) -> dict[str, Any]:
        """Check agent-server health."""
        res = self._request("/health", method="GET", timeout=min(5.0, self.timeout))
        if isinstance(res, dict):
            return res
        return {"status": "ok"}

    def list_chats(self) -> list[dict[str, Any]]:
        """Fetch list of all active chats with their lightweight state."""
        res = self._request("/api/chats", method="GET")
        if isinstance(res, list):
            return res
        if isinstance(res, dict) and isinstance(res.get("chats"), list):
            return res["chats"]
        return []

    def list_messages(
        self,
        chat_id: str,
        limit: int = 50,
        offset: int = 0,
    ) -> list[dict[str, Any]]:
        """Fetch message history for a specific chat."""
        path = f"/api/messages/{urllib.parse.quote(str(chat_id), safe='')}"
        res = self._request(
            path,
            method="GET",
            query={"limit": limit, "offset": offset},
        )
        if isinstance(res, list):
            return res
        if isinstance(res, dict) and isinstance(res.get("messages"), list):
            return res["messages"]
        return []

    def list_contacts(self, limit: int = 200, offset: int = 0) -> list[dict[str, Any]]:
        """Fetch contact list."""
        res = self._request("/api/contacts", method="GET", query={"limit": limit, "offset": offset})
        if isinstance(res, list):
            return res
        if isinstance(res, dict) and isinstance(res.get("contacts"), list):
            return res["contacts"]
        return []

    def get_media(self, chat_id: str, local_id: int | str) -> dict[str, Any]:
        """Fetch media data for a specific message.

        Returns dict: {"type": ..., "format": ..., "filename": ..., "data": base64_str}
        """
        path = f"/api/messages/{urllib.parse.quote(str(chat_id), safe='')}/media/{int(local_id)}"
        res = self._request(path, method="GET", timeout=max(15.0, self.timeout))
        if isinstance(res, dict):
            return res
        return {}

    def send_message(self, payload: dict[str, Any], timeout: float = 45.0) -> dict[str, Any]:
        """Send message via agent-wechat API."""
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        res = self._request("/api/messages/send", method="POST", data=body, timeout=timeout)
        if isinstance(res, dict):
            return res
        return {}
