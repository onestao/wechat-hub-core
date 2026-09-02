#!/usr/bin/env python3
"""HTTP service for the account-aware WeChat Core Interface Contract V1."""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import os
import re
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, unquote, urlparse

from . import CONTRACT_VERSION
from .account_worker import AccountSyncLoop, AccountWorker
from .registry import AccountRegistry, RegistryError, legacy_registry, load_registry
from .runtime_control import RuntimeControlClient, RuntimeControlError
from .sender import AccountSender, OutboxLoop, sender_capabilities
from .store import CoreStore, StoreError, utc_now


ACCOUNT_STATES = {"offline", "starting", "login_required", "online", "degraded", "stopped", "error"}


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message
        self.details = details or {}


def required_text(payload: dict[str, Any], field: str) -> str:
    value = payload.get(field)
    if not isinstance(value, str) or not value.strip():
        raise ApiError(400, "invalid_request", f"{field} must be a non-empty string", {"field": field})
    return value.strip()


def bounded_int(value: str, field: str, *, default: int, low: int, high: int) -> int:
    if value == "":
        return default
    try:
        result = int(value)
    except ValueError as exc:
        raise ApiError(400, "invalid_request", f"{field} must be an integer", {"field": field}) from exc
    if not low <= result <= high:
        raise ApiError(400, "invalid_request", f"{field} must be between {low} and {high}", {"field": field})
    return result


class CoreService:
    def __init__(
        self,
        *,
        root: Path,
        registry: AccountRegistry,
        store: CoreStore,
        runtime_control: RuntimeControlClient | None = None,
    ) -> None:
        self.root = root
        self.registry = registry
        self.store = store
        self.runtime_control = runtime_control
        self.media_root = root / "runtime" / "core-media"
        self._registry_reload_lock = threading.RLock()
        self._registry_fingerprint_value = ""
        self.last_registry_reload: dict[str, Any] = {
            "ok": True,
            "changed": False,
            "error": "",
            "added": [],
            "removed": [],
            "updated": [],
        }
        self.apply_registry()
        # Re-read an on-disk registry once at service construction. This closes
        # the small load->service-start race where Runtime could rewrite the
        # file after main() parsed it but before the watcher recorded a
        # fingerprint. Tests that construct an in-memory registry without a
        # source file keep their supplied snapshot unchanged.
        if self.registry.source_path.is_file():
            self.reload_registry(force=True)
        else:
            self._registry_fingerprint_value = self._registry_fingerprint()

    def apply_registry(self) -> None:
        for account in self.registry.all():
            existing = self.store.account(account.account_id)
            existing_runtime = (existing or {}).get("runtime") or {}
            state = str((existing or {}).get("state") or account.runtime.get("state") or "offline")
            if existing_runtime.get("registered") is False and state == "stopped":
                state = "offline"
            if state not in ACCOUNT_STATES:
                state = "offline"
            runtime = account.public_runtime()
            runtime["registered"] = True
            self.store.upsert_account(
                account.account_id,
                account.display_name,
                state=state,
                runtime=runtime,
                sync=(existing or {}).get("sync") or {},
            )

    def accounts(self) -> list[dict[str, Any]]:
        # Keep the public account list fresh enough to surface a degraded
        # AgentWechat child instead of serving an indefinitely stale healthy
        # projection. Runtime list probes are bounded/parallelized on the
        # Runtime side, so one frozen agent-server cannot serialize all peers.
        # If Runtime management itself is unavailable, preserve the existing
        # projection rather than turning account discovery into a 5xx.
        if self.runtime_control is not None:
            try:
                runtime_list = self._runtime_request("list")
            except ApiError:
                runtime_list = {}
            for status in runtime_list.get("accounts") or []:
                if isinstance(status, dict):
                    self._apply_runtime_status(status)
        output: list[dict[str, Any]] = []
        for config in self.registry.all():
            account = self.store.account(config.account_id)
            if account is not None:
                output.append(account)
        return sorted(output, key=lambda item: str(item.get("account_id") or ""))

    def _registry_fingerprint(self) -> str:
        try:
            content = self.registry.source_path.read_bytes()
        except FileNotFoundError:
            return "missing"
        return hashlib.sha256(content).hexdigest()

    def registry_status(self) -> dict[str, Any]:
        return {
            "source": str(self.registry.source_path),
            "hot_reload": True,
            "accounts": len(self.registry.all()),
            **self.last_registry_reload,
        }

    def reload_registry(self, *, force: bool = False) -> dict[str, Any]:
        with self._registry_reload_lock:
            fingerprint = self._registry_fingerprint()
            if not force and fingerprint == self._registry_fingerprint_value:
                return {**self.last_registry_reload, "changed": False}
            old = {account.account_id: account for account in self.registry.all()}
            try:
                replacement = load_registry(self.registry.source_path, root=self.root)
            except RegistryError as exc:
                self.last_registry_reload = {
                    "ok": False,
                    "changed": False,
                    "error": str(exc),
                    "added": [],
                    "removed": [],
                    "updated": [],
                }
                raise
            new = {account.account_id: account for account in replacement.all()}
            added = sorted(set(new) - set(old))
            removed = sorted(set(old) - set(new))
            updated = sorted(account_id for account_id in set(old) & set(new) if old[account_id] != new[account_id])
            self.registry.replace_from(replacement)
            self.apply_registry()
            for account_id in removed:
                existing = self.store.account(account_id)
                if existing is not None:
                    runtime = dict(existing.get("runtime") or {})
                    runtime["registered"] = False
                    runtime["enabled"] = False
                    self.store.upsert_account(
                        account_id,
                        str(existing.get("display_name") or account_id),
                        state="stopped",
                        runtime=runtime,
                        sync=existing.get("sync") or {},
                    )
                self.store.fail_pending_sends_for_account(
                    account_id,
                    reason="WeChat account was removed from the Runtime registry before dispatch",
                )
            self._registry_fingerprint_value = fingerprint
            self.last_registry_reload = {
                "ok": True,
                "changed": bool(added or removed or updated),
                "error": "",
                "added": added,
                "removed": removed,
                "updated": updated,
            }
            return dict(self.last_registry_reload)

    def runtime_management_status(self) -> dict[str, Any]:
        if self.runtime_control is None:
            return {"configured": False, "available": False, "registry_hot_reload": True}
        return {
            "configured": True,
            "available": self.runtime_control.available,
            "registry_hot_reload": True,
        }

    @staticmethod
    def _runtime_error(error: RuntimeControlError) -> ApiError:
        status = {
            "invalid_request": 400,
            "account_not_found": 404,
            "account_exists": 409,
            "runtime_management_unavailable": 503,
            "invalid_runtime_response": 502,
        }.get(error.code, 500)
        return ApiError(status, error.code, error.message)

    def _runtime_request(self, action: str, **payload: Any) -> dict[str, Any]:
        if self.runtime_control is None:
            raise ApiError(503, "runtime_management_unavailable", "Runtime management control socket is not configured")
        try:
            return self.runtime_control.request(action, **payload)
        except RuntimeControlError as exc:
            raise self._runtime_error(exc) from exc

    def runtime_accounts(self) -> dict[str, Any]:
        output = self._runtime_request("list")
        try:
            output["registry_reload"] = self.reload_registry()
        except RegistryError as exc:
            raise ApiError(500, "registry_reload_failed", str(exc)) from exc
        for status in output.get("accounts") or []:
            if isinstance(status, dict):
                self._apply_runtime_status(status)
        return output

    def _apply_runtime_status(self, status: dict[str, Any]) -> None:
        account_id = str(status.get("account_id") or "").strip()
        config = self.registry.get(account_id) if account_id else None
        if config is None:
            return
        existing = self.store.account(account_id)
        runtime = config.public_runtime()
        runtime["registered"] = True
        runtime.update(
            {
                "running": bool(status.get("running")),
                "container_running": bool(status.get("container_running", status.get("running"))),
                "agent_server_healthy": status.get("agent_server_healthy"),
                "runtime_health": status.get("runtime_health"),
                "health_error": status.get("health_error"),
                "wechat_login_status": status.get("wechat_login_status"),
                "logged_in_user": status.get("logged_in_user"),
                "pids": list(status.get("pids") or []),
                "windows": list(status.get("windows") or []),
                "window_error": status.get("window_error"),
                "username": status.get("username") or runtime.get("username"),
                "uid": status.get("uid", runtime.get("uid")),
                "home": status.get("home") or runtime.get("source_home"),
                "autostart": bool(status.get("autostart", True)),
            }
        )
        for key in (
            "runtime_provider",
            "container_name",
            "container_id",
            "image",
            "current_image",
            "image_update_pending",
            "capabilities",
        ):
            if key in status:
                runtime[key] = status[key]
        if not runtime["running"]:
            state = "stopped"
        elif str(runtime.get("runtime_provider") or config.runtime_provider) == "agent_wechat" and status.get("agent_server_healthy") is False:
            state = "degraded"
        elif str(runtime.get("runtime_provider") or config.runtime_provider) == "agent_wechat" and status.get("agent_server_healthy") is True:
            login_status = str(status.get("wechat_login_status") or "unknown")
            if login_status == "logged_in":
                state = "online"
            elif login_status == "logged_out":
                state = "login_required"
            else:
                current_state = str((existing or {}).get("state") or "offline")
                state = current_state if current_state not in {"offline", "stopped", "degraded"} else "starting"
        elif str(status.get("action") or "") in {"started", "restarted"}:
            state = "starting"
        else:
            current_state = str((existing or {}).get("state") or "offline")
            state = current_state if current_state not in {"offline", "stopped"} else "starting"
        self.store.upsert_account(
            account_id,
            str(status.get("display_name") or config.display_name),
            state=state,
            runtime=runtime,
            sync=(existing or {}).get("sync") or {},
        )

    def create_runtime_account(self, payload: dict[str, Any]) -> dict[str, Any]:
        provider = str(payload.get("runtime_provider") or payload.get("provider") or "legacy").strip().lower()
        if provider not in {"legacy", "agent_wechat"}:
            raise ApiError(400, "invalid_request", "runtime_provider must be legacy or agent_wechat")
        result = self._runtime_request(
            "register",
            account_id=required_text(payload, "account_id"),
            display_name=str(payload.get("display_name") or "").strip(),
            display=str(payload.get("display") or "").strip(),
            runtime_provider=provider,
            autostart=bool(payload.get("autostart", True)),
            start=bool(payload.get("start", True)),
        )
        try:
            result["registry_reload"] = self.reload_registry(force=True)
        except RegistryError as exc:
            raise ApiError(500, "registry_reload_failed", str(exc)) from exc
        if isinstance(result.get("status"), dict):
            self._apply_runtime_status(result["status"])
        return result

    def runtime_account_action(self, account_id: str, action: str) -> dict[str, Any]:
        if action not in {"start", "stop", "restart"}:
            raise ApiError(404, "not_found", f"Unknown Runtime account action: {action}")
        self.require_account(account_id)
        result = self._runtime_request(action, account_id=account_id)
        result["registry_reload"] = self.reload_registry()
        if isinstance(result.get("status"), dict):
            self._apply_runtime_status(result["status"])
        return result

    def runtime_login_status(self, account_id: str) -> dict[str, Any]:
        account = self.require_account(account_id)
        result = self._runtime_request("login_status", account_id=account_id)
        login = result.get("login") if isinstance(result.get("login"), dict) else {}
        running = bool(login.get("running"))
        snapshot_available = bool(login.get("snapshot_available"))
        auth_status = str(login.get("auth_status") or "")
        login_flow_state = str(login.get("login_flow_state") or "idle")
        login_flow_status = str(login.get("login_flow_status") or "")
        login_flow_error = str(login.get("login_flow_error") or "")
        agent_server_healthy = login.get("agent_server_healthy")
        core_state = str(account.get("state") or "offline")
        if agent_server_healthy is False:
            state = "attention"
        elif login_flow_state in {"error", "timeout"}:
            state = "attention"
        elif auth_status == "logged_in" or core_state == "online":
            state = "online"
        elif not running:
            state = "stopped"
        elif not snapshot_available:
            state = "starting"
        elif core_state in {"error", "degraded"}:
            state = "attention"
        else:
            state = "waiting"
        return {
            "account_id": account_id,
            "display_name": str(login.get("display_name") or account.get("display_name") or account_id),
            "state": state,
            "core_state": core_state,
            "running": running,
            "container_running": bool(login.get("container_running", running)),
            "agent_server_healthy": agent_server_healthy,
            "runtime_health": str(login.get("runtime_health") or ""),
            "snapshot_available": snapshot_available,
            "auth_status": auth_status,
            "logged_in_user": str(login.get("logged_in_user") or ""),
            "window_title": str(login.get("window_title") or ""),
            "window_count": len(login.get("windows") or []),
            "login_flow_state": login_flow_state,
            "login_flow_status": login_flow_status,
            "login_flow_error": login_flow_error,
        }

    def runtime_login_start(self, account_id: str) -> dict[str, Any]:
        self.require_account(account_id)
        result = self._runtime_request("start_login", account_id=account_id)
        login = result.get("login") if isinstance(result.get("login"), dict) else {}
        return {
            "account_id": account_id,
            "running": bool(login.get("running")),
            "snapshot_available": bool(login.get("snapshot_available")),
            "login_flow_state": str(login.get("login_flow_state") or "starting"),
            "login_flow_status": str(login.get("login_flow_status") or ""),
            "login_flow_error": str(login.get("login_flow_error") or ""),
        }

    def runtime_login_snapshot(self, account_id: str) -> tuple[bytes, str]:
        self.require_account(account_id)
        result = self._runtime_request("capture_login", account_id=account_id)
        if str(result.get("status") or "") == "qr_not_ready":
            raise ApiError(
                409,
                "qr_not_ready",
                "WeChat login QR is not ready yet",
                {
                    "login_flow_state": str(result.get("login_flow_state") or "idle"),
                    "login_flow_status": str(result.get("login_flow_status") or ""),
                    "login_flow_error": str(result.get("login_flow_error") or ""),
                },
            )
        encoded = str(result.get("content_base64") or "")
        try:
            content = base64.b64decode(encoded, validate=True)
        except (ValueError, binascii.Error) as exc:
            raise ApiError(502, "invalid_runtime_response", "Runtime returned invalid login snapshot data") from exc
        if not content.startswith(b"\x89PNG\r\n\x1a\n"):
            raise ApiError(502, "invalid_runtime_response", "Runtime login snapshot is not a PNG")
        if len(content) > 2 * 1024 * 1024:
            raise ApiError(502, "invalid_runtime_response", "Runtime login snapshot exceeds the safe response limit")
        return content, str(result.get("content_type") or "image/png")

    def runtime_desktop(self, account_id: str) -> dict[str, Any]:
        self.require_account(account_id)
        result = self._runtime_request("desktop", account_id=account_id)
        desktop = result.get("desktop") if isinstance(result.get("desktop"), dict) else {}
        return {
            "account_id": account_id,
            "runtime_provider": str(desktop.get("runtime_provider") or "legacy"),
            "desktop_provider": str(desktop.get("desktop_provider") or ""),
            "scheme": str(desktop.get("scheme") or ""),
            "host": str(desktop.get("host") or ""),
            "port": desktop.get("port"),
            "path": str(desktop.get("path") or ""),
            "gateway_session_expires_at": desktop.get("gateway_session_expires_at"),
            "features": dict(desktop.get("features") or {}) if isinstance(desktop.get("features"), dict) else {},
            "fallback_reason": str(desktop.get("fallback_reason") or ""),
            "file_exchange_path": str(desktop.get("file_exchange_path") or ""),
        }

    def remove_runtime_account(self, account_id: str, *, purge_data: bool = False) -> dict[str, Any]:
        self.require_account(account_id)
        result = self._runtime_request("unregister", account_id=account_id, purge_data=bool(purge_data))
        try:
            result["registry_reload"] = self.reload_registry(force=True)
        except RegistryError as exc:
            raise ApiError(500, "registry_reload_failed", str(exc)) from exc
        return result

    def require_account(self, account_id: str) -> dict[str, Any]:
        account = self.store.account(account_id)
        if account is None or self.registry.get(account_id) is None:
            raise ApiError(404, "account_not_found", f"Unknown account_id: {account_id}")
        return account

    def require_chat(self, account_id: str, chat_id: str) -> dict[str, Any]:
        self.require_account(account_id)
        chat = self.store.chat(account_id, chat_id)
        if chat is None:
            raise ApiError(404, "chat_not_found", f"Unknown chat_id for {account_id}: {chat_id}")
        return chat

    def validate_send(self, kind: str, payload: dict[str, Any]) -> dict[str, Any]:
        account_id = required_text(payload, "account_id")
        chat_id = required_text(payload, "chat_id")
        self.require_chat(account_id, chat_id)
        normalized = dict(payload)
        normalized["account_id"] = account_id
        normalized["chat_id"] = chat_id
        if kind == "text":
            normalized["text"] = required_text(payload, "text")
            mentions = payload.get("mention_member_ids", [])
            if mentions is not None and (not isinstance(mentions, list) or not all(isinstance(item, str) and item.strip() for item in mentions)):
                raise ApiError(400, "invalid_request", "mention_member_ids must be an array of non-empty strings", {"field": "mention_member_ids"})
            return normalized
        media_id = str(payload.get("media_id") or "").strip()
        content_base64 = str(payload.get("content_base64") or "").strip()
        if bool(media_id) == bool(content_base64):
            raise ApiError(400, "invalid_media_source", "Provide exactly one of media_id or content_base64")
        if media_id:
            if self.store.media(account_id, media_id) is None:
                raise ApiError(404, "media_not_found", f"Unknown media_id for {account_id}: {media_id}")
            normalized["media_id"] = media_id
            return normalized
        filename = str(payload.get("filename") or ("image.bin" if kind == "image" else "file.bin"))
        mime_type = str(payload.get("mime_type") or "application/octet-stream")
        try:
            generated_media_id = self.store.put_inline_media(
                account_id,
                content_base64,
                filename=filename,
                mime_type=mime_type,
                media_root=self.media_root,
            )
        except StoreError as exc:
            raise ApiError(exc.status, exc.code, str(exc), exc.details) from exc
        normalized.pop("content_base64", None)
        normalized["media_id"] = generated_media_id
        normalized["filename"] = filename
        normalized["mime_type"] = mime_type
        return normalized


class CoreHandler(BaseHTTPRequestHandler):
    server_version = "WeChatCore/1"
    service: CoreService

    def log_message(self, fmt: str, *args: Any) -> None:
        return

    def _json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _error(self, error: ApiError) -> None:
        self._json(error.status, {"error": {"code": error.code, "message": error.message, "details": error.details}})

    def _body_json(self) -> dict[str, Any]:
        content_type = self.headers.get("Content-Type", "").split(";", 1)[0].strip().lower()
        if content_type != "application/json":
            raise ApiError(415, "unsupported_media_type", "Content-Type must be application/json")
        try:
            length = int(self.headers.get("Content-Length", "0"))
        except ValueError as exc:
            raise ApiError(400, "invalid_content_length", "Invalid Content-Length") from exc
        if length <= 0:
            raise ApiError(400, "empty_body", "Request body is required")
        try:
            payload = json.loads(self.rfile.read(length).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ApiError(400, "invalid_json", "Request body must be valid UTF-8 JSON") from exc
        if not isinstance(payload, dict):
            raise ApiError(400, "invalid_json", "Top-level JSON value must be an object")
        return payload

    def do_GET(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            query = parse_qs(parsed.query, keep_blank_values=True)
            if path == "/health":
                self._json(
                    200,
                    {
                        "ok": True,
                        "service": "wechat-core",
                        "contract_version": CONTRACT_VERSION,
                        "time": utc_now(),
                        "accounts": len(self.service.accounts()),
                        "sender_capabilities": sender_capabilities(),
                        "registry": self.service.registry_status(),
                        "runtime_management": self.service.runtime_management_status(),
                    },
                )
                return
            if path == "/v1/accounts":
                self._json(200, {"accounts": self.service.accounts()})
                return
            if path == "/v1/runtime/accounts":
                self._json(200, self.service.runtime_accounts())
                return
            runtime_prefix = "/v1/runtime/accounts/"
            if path.startswith(runtime_prefix):
                runtime_suffix = path[len(runtime_prefix):]
                if runtime_suffix.endswith("/login/snapshot"):
                    account_id = unquote(runtime_suffix[: -len("/login/snapshot")].strip("/"))
                    if not account_id or "/" in account_id:
                        raise ApiError(404, "not_found", f"Unknown endpoint: {path}")
                    content, content_type = self.service.runtime_login_snapshot(account_id)
                    self.send_response(200)
                    self.send_header("Content-Type", content_type)
                    self.send_header("Content-Length", str(len(content)))
                    self.send_header("Cache-Control", "no-store, max-age=0")
                    self.send_header("Pragma", "no-cache")
                    self.send_header("X-Content-Type-Options", "nosniff")
                    self.end_headers()
                    self.wfile.write(content)
                    return
                if runtime_suffix.endswith("/login"):
                    account_id = unquote(runtime_suffix[: -len("/login")].strip("/"))
                    if account_id and "/" not in account_id:
                        self._json(200, self.service.runtime_login_status(account_id))
                        return
                if runtime_suffix.endswith("/desktop"):
                    account_id = unquote(runtime_suffix[: -len("/desktop")].strip("/"))
                    if account_id and "/" not in account_id:
                        self._json(200, self.service.runtime_desktop(account_id))
                        return
            prefix = "/v1/accounts/"
            suffix = "/chats"
            if path.startswith(prefix) and path.endswith(suffix):
                account_id = unquote(path[len(prefix):-len(suffix)].strip("/"))
                self.service.require_account(account_id)
                limit = bounded_int(query.get("limit", [""])[0], "limit", default=100, low=1, high=200)
                try:
                    output = self.service.store.list_chats(
                        account_id,
                        cursor=query.get("cursor", [""])[0],
                        limit=limit,
                        query=query.get("query", [""])[0].strip(),
                    )
                except StoreError as exc:
                    raise ApiError(exc.status, exc.code, str(exc), exc.details) from exc
                self._json(200, output)
                return
            if path == "/v1/events/poll":
                account_id = query.get("account_id", [""])[0].strip()
                if account_id:
                    self.service.require_account(account_id)
                limit = bounded_int(query.get("limit", [""])[0], "limit", default=50, low=1, high=200)
                timeout = bounded_int(query.get("timeout", [""])[0], "timeout", default=0, low=0, high=30)
                after = query.get("after", ["0"])[0] or "0"
                deadline = time.monotonic() + timeout
                while True:
                    try:
                        page = self.service.store.poll_events(after=after, limit=limit, account_id=account_id)
                    except StoreError as exc:
                        raise ApiError(exc.status, exc.code, str(exc), exc.details) from exc
                    if page["events"] or timeout == 0 or time.monotonic() >= deadline:
                        self._json(200, page)
                        return
                    time.sleep(min(0.25, max(0.0, deadline - time.monotonic())))
            media_prefix = "/v1/media/"
            if path.startswith(media_prefix):
                media_id = unquote(path[len(media_prefix):])
                account_id = query.get("account_id", [""])[0].strip()
                if not account_id:
                    raise ApiError(400, "invalid_request", "account_id query parameter is required", {"field": "account_id"})
                self.service.require_account(account_id)
                media = self.service.store.media(account_id, media_id)
                if media is None:
                    raise ApiError(404, "media_not_found", f"Unknown media_id for {account_id}: {media_id}")
                local_path = Path(str(media["local_path"]))
                if not local_path.exists() or not local_path.is_file():
                    raise ApiError(404, "media_not_found", f"Media bytes are unavailable for {media_id}")
                content = local_path.read_bytes()
                filename = re.sub(r"[\r\n\"]", "_", str(media["filename"]))
                self.send_response(200)
                self.send_header("Content-Type", str(media["mime_type"]))
                self.send_header("Content-Length", str(len(content)))
                self.send_header("Content-Disposition", f'{media["disposition"]}; filename="{filename}"')
                self.send_header("X-Media-Id", media_id)
                self.end_headers()
                self.wfile.write(content)
                return
            raise ApiError(404, "not_found", f"Unknown endpoint: {path}")
        except ApiError as error:
            self._error(error)

    def do_POST(self) -> None:  # noqa: N802
        try:
            path = urlparse(self.path).path.rstrip("/") or "/"
            payload = self._body_json()
            if path == "/v1/events/ack":
                event_ids = payload.get("event_ids")
                if not isinstance(event_ids, list):
                    raise ApiError(400, "invalid_event_ids", "event_ids must be a non-empty list of strings", {"field": "event_ids"})
                try:
                    output = self.service.store.ack_events(required_text(payload, "consumer_id"), event_ids)
                except StoreError as exc:
                    raise ApiError(exc.status, exc.code, str(exc), exc.details) from exc
                self._json(200, output)
                return
            if path == "/v1/runtime/accounts":
                self._json(201, self.service.create_runtime_account(payload))
                return
            runtime_prefix = "/v1/runtime/accounts/"
            if path.startswith(runtime_prefix):
                suffix = path[len(runtime_prefix):]
                parts = suffix.split("/")
                if len(parts) == 2 and parts[0] and parts[1]:
                    account_id = unquote(parts[0])
                    if parts[1] == "login":
                        self._json(202, self.service.runtime_login_start(account_id))
                        return
                    self._json(200, self.service.runtime_account_action(account_id, parts[1]))
                    return
            send_prefix = "/v1/send/"
            if path.startswith(send_prefix):
                kind = path[len(send_prefix):]
                if kind not in {"text", "image", "file"}:
                    raise ApiError(404, "not_found", f"Unknown send operation: {kind}")
                idempotency_key = self.headers.get("Idempotency-Key", "").strip() or str(payload.get("client_request_id") or "").strip()
                account_id = required_text(payload, "account_id")
                chat_id = required_text(payload, "chat_id")
                self.service.require_chat(account_id, chat_id)
                if len(idempotency_key) > 200:
                    raise ApiError(400, "invalid_request", "Idempotency-Key must be at most 200 characters", {"field": "Idempotency-Key"})
                request_digest = self.service.store.send_request_digest(kind, payload)
                if idempotency_key:
                    try:
                        existing = self.service.store.receipt_by_idempotency_key(
                            idempotency_key,
                            kind=kind,
                            account_id=account_id,
                            chat_id=chat_id,
                            request_digest=request_digest,
                        )
                    except StoreError as exc:
                        raise ApiError(exc.status, exc.code, str(exc), exc.details) from exc
                    if existing:
                        self._json(202, existing)
                        return
                normalized = self.service.validate_send(kind, payload)
                try:
                    receipt = self.service.store.queue_send(
                        kind,
                        normalized,
                        idempotency_key,
                        request_digest=request_digest,
                    )
                except StoreError as exc:
                    raise ApiError(exc.status, exc.code, str(exc), exc.details) from exc
                self._json(202, receipt)
                return
            raise ApiError(404, "not_found", f"Unknown endpoint: {path}")
        except ApiError as error:
            self._error(error)


    def do_DELETE(self) -> None:  # noqa: N802
        try:
            parsed = urlparse(self.path)
            path = parsed.path.rstrip("/") or "/"
            query = parse_qs(parsed.query)
            prefix = "/v1/runtime/accounts/"
            if path.startswith(prefix):
                suffix = path[len(prefix):]
                if suffix and "/" not in suffix:
                    purge = query.get("purge_data", [""])[0].strip().lower() in {"1", "true", "yes", "on"}
                    self._json(200, self.service.remove_runtime_account(unquote(suffix), purge_data=purge))
                    return
            raise ApiError(404, "not_found", f"Unknown endpoint: {path}")
        except ApiError as error:
            self._error(error)


class RegistryReloadLoop:
    def __init__(self, service: CoreService, interval_seconds: float) -> None:
        self.service = service
        self.interval_seconds = max(0.25, float(interval_seconds))
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="wechat-core-registry", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=max(2.0, self.interval_seconds + 1.0))

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self.service.reload_registry()
            except RegistryError:
                pass
            self._stop.wait(self.interval_seconds)


def create_server(host: str, port: int, service: CoreService) -> ThreadingHTTPServer:
    handler = type("BoundCoreHandler", (CoreHandler,), {"service": service})
    return ThreadingHTTPServer((host, port), handler)


def main(argv: list[str] | None = None) -> int:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument("--root", type=Path, default=root)
    parser.add_argument("--registry", type=Path, default=Path("runtime/core/accounts.json"))
    parser.add_argument("--database", type=Path, default=Path("runtime/core/wechat_core.sqlite"))
    parser.add_argument("--require-registry", action="store_true", help="Exit if the configured registry is missing or empty")
    parser.add_argument("--legacy-bootstrap", action="store_true", help="Use WECHAT_ACCOUNT_DIR_NAME when no registry is present")
    parser.add_argument("--sync-interval", type=float, default=0, help="Run account sync loop every N seconds; 0 disables it")
    parser.add_argument("--send-interval", type=float, default=0, help="Run enabled account senders every N seconds; 0 disables it")
    parser.add_argument(
        "--registry-reload-interval",
        type=float,
        default=float(os.environ.get("CORE_REGISTRY_RELOAD_INTERVAL", "1")),
        help="Reload the Runtime account registry every N seconds when it changes; 0 disables watching",
    )
    parser.add_argument(
        "--runtime-control-socket",
        default=os.environ.get("WECHAT_RUNTIME_CONTROL_SOCKET", ""),
        help="Private Runtime Unix control socket used for account lifecycle management",
    )
    args = parser.parse_args(argv)
    workspace_root = args.root.resolve()
    registry_path = args.registry if args.registry.is_absolute() else workspace_root / args.registry
    if args.require_registry and not registry_path.is_file():
        parser.error(f"required registry does not exist: {registry_path}")
    try:
        registry = load_registry(args.registry, root=workspace_root)
        if not registry.all() and args.legacy_bootstrap:
            registry = legacy_registry(root=workspace_root)
    except RegistryError as exc:
        parser.error(str(exc))
    if args.require_registry and not registry.all():
        parser.error(f"required registry contains no accounts: {registry_path}")
    database = args.database if args.database.is_absolute() else workspace_root / args.database
    runtime_control = RuntimeControlClient(args.runtime_control_socket) if str(args.runtime_control_socket).strip() else None
    service = CoreService(
        root=workspace_root,
        registry=registry,
        store=CoreStore(database),
        runtime_control=runtime_control,
    )
    sync_loop = None
    sender_loop = None
    registry_loop = None
    if args.registry_reload_interval > 0:
        registry_loop = RegistryReloadLoop(service, args.registry_reload_interval)
        registry_loop.start()
    if args.sync_interval > 0:
        sync_loop = AccountSyncLoop(AccountWorker(registry, service.store), args.sync_interval)
        sync_loop.start()
    if args.send_interval > 0:
        sender_loop = OutboxLoop(AccountSender(registry, service.store, root=workspace_root), args.send_interval)
        sender_loop.start()
    server = create_server(args.host, args.port, service)
    print(f"WeChat Core V{CONTRACT_VERSION} listening on http://{args.host}:{server.server_port} ({len(registry.all())} accounts)", flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        if sync_loop:
            sync_loop.stop()
        if sender_loop:
            sender_loop.stop()
        if registry_loop:
            registry_loop.stop()
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
