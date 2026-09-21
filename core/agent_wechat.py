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
import shutil
import tempfile
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from memory.memory_ingest import content_hash, source_message_identity

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


def canonical_message_id(chat_id: str, local_id: int | str) -> str:
    """Generate deterministic message ID strictly identical to legacy source_message_identity hash."""
    return content_hash(source_message_identity(str(chat_id).strip(), "", int(local_id)))


def map_agent_message_kind(raw_msg: dict[str, Any]) -> tuple[str, int | None, str]:
    """Map canonical provider message fields to canonical (kind, subtype, filename).

    Core accepts authoritative provider fields: kind, subtype, filename.
    Unknown types default to 'unknown'; Core does not guess or parse XML.
    """
    kind = str(raw_msg.get("kind") or "").strip().lower()
    subtype = raw_msg.get("subtype")
    if subtype is not None:
        try:
            subtype = int(subtype)
        except (ValueError, TypeError):
            subtype = None
    filename = str(raw_msg.get("filename") or "").strip()

    if kind in CANONICAL_KINDS:
        return kind, subtype, filename
    return "unknown", subtype, filename


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
    kind, subtype, filename = map_agent_message_kind(msg)

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

    def fetch_media_to_file(
        self,
        chat_id: str,
        local_id: int | str,
        target_path: Path,
        timeout: float = 45.0,
    ) -> dict[str, str]:
        """Fetch media directly to target file path using streaming copy.

        Handles:
        1. Direct streaming from agent-wechat.
        2. URL-backed CDN stickers: downloads from CDN URL without AgentWechat Authorization header.
        3. Pending / unsupported status propagation.
        """
        path = f"/api/messages/{urllib.parse.quote(str(chat_id), safe='')}/media/{int(local_id)}?raw=true"
        url = f"{self.base_url}{path}"
        req = urllib.request.Request(
            url,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Accept": "*/*",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                headers = {k.lower(): str(v) for k, v in resp.headers.items()}
                status = str(headers.get("x-media-status") or "ready").lower()

                if status == "unsupported":
                    return {"status": "unsupported", **headers}
                if status == "pending":
                    return {"status": "pending", **headers}

                cdn_url = headers.get("x-media-url")
                if cdn_url:
                    # Fetch from CDN: NEVER include AgentWechat Authorization header to CDN
                    target_path.parent.mkdir(parents=True, exist_ok=True)
                    cdn_req = urllib.request.Request(cdn_url, headers={"User-Agent": "WeChatHub/0.1.0"})
                    try:
                        with urllib.request.urlopen(cdn_req, timeout=15.0) as cdn_resp:
                            with open(target_path, "wb") as f_out:
                                shutil.copyfileobj(cdn_resp, f_out, length=64 * 1024)
                    except Exception as exc:
                        raise AgentWechatError(f"CDN download failed: {exc}", status_code=504) from exc
                    return {"status": "ready", **headers}

                # Direct stream copy to target file
                target_path.parent.mkdir(parents=True, exist_ok=True)
                with open(target_path, "wb") as f_out:
                    shutil.copyfileobj(resp, f_out, length=64 * 1024)

                if target_path.stat().st_size == 0:
                    target_path.unlink(missing_ok=True)
                    return {"status": "pending", **headers}

                return {"status": "ready", **headers}
        except urllib.error.HTTPError as exc:
            if exc.code == 404:
                return {"status": "unsupported", "x-media-status": "unsupported"}
            if exc.code == 202:
                return {"status": "pending", "x-media-status": "pending"}
            raise AgentWechatError(f"HTTP {exc.code}: {exc.reason}", status_code=exc.code) from exc
        except AgentWechatError:
            raise
        except Exception as exc:
            raise AgentWechatError(f"Connection failed: {exc}", status_code=502) from exc

    def get_media_raw(self, chat_id: str, local_id: int | str, timeout: float = 45.0) -> tuple[bytes, dict[str, str]]:
        """Fetch raw binary media directly from agent-wechat API with streaming/raw support.

        Bypasses JSON/base64 size limitations and streams data directly.
        Returns: (bytes, headers_dict)
        """
        with tempfile.NamedTemporaryFile(delete=False) as tmp:
            tmp_path = Path(tmp.name)
        try:
            headers = self.fetch_media_to_file(chat_id, local_id, tmp_path, timeout=timeout)
            if headers.get("status") == "ready" and tmp_path.is_file():
                data = tmp_path.read_bytes()
            else:
                data = b""
            return data, headers
        finally:
            tmp_path.unlink(missing_ok=True)

    def send_message(self, payload: dict[str, Any], timeout: float = 45.0) -> dict[str, Any]:
        """Send message via agent-wechat API."""
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        res = self._request("/api/messages/send", method="POST", data=body, timeout=timeout)
        if isinstance(res, dict):
            return res
        return {}
