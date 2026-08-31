#!/usr/bin/env python3
"""HTTP service for the account-aware WeChat Core Interface Contract V1."""

from __future__ import annotations

import argparse
import json
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
from .sender import AccountSender, OutboxLoop
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
    def __init__(self, *, root: Path, registry: AccountRegistry, store: CoreStore) -> None:
        self.root = root
        self.registry = registry
        self.store = store
        self.media_root = root / "runtime" / "core-media"
        self.apply_registry()

    def apply_registry(self) -> None:
        for account in self.registry.all():
            existing = self.store.account(account.account_id)
            state = str((existing or {}).get("state") or account.runtime.get("state") or "offline")
            if state not in ACCOUNT_STATES:
                state = "offline"
            self.store.upsert_account(
                account.account_id,
                account.display_name,
                state=state,
                runtime=account.public_runtime(),
                sync=(existing or {}).get("sync") or {},
            )

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
                    {"ok": True, "service": "wechat-core", "contract_version": CONTRACT_VERSION, "time": utc_now(), "accounts": len(self.service.store.list_accounts())},
                )
                return
            if path == "/v1/accounts":
                self._json(200, {"accounts": self.service.store.list_accounts()})
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
    service = CoreService(root=workspace_root, registry=registry, store=CoreStore(database))
    sync_loop = None
    sender_loop = None
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
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
