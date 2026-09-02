"""Account-aware adapter around the upstream X11 WeChat controller."""

from __future__ import annotations

import base64
import hashlib
import json
import mimetypes
import os
import subprocess
import sys
import threading
import urllib.error
import urllib.parse
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from .registry import AccountConfig, AccountRegistry
from .runtime_bridge import resolve_runtime_account
from .store import CoreStore, parse_json

try:  # Linux production path; Windows host tests keep the in-process lock only.
    import fcntl
except ImportError:  # pragma: no cover - platform-specific fallback.
    fcntl = None


_DISPLAY_LOCKS: dict[str, threading.RLock] = {}
_DISPLAY_LOCKS_GUARD = threading.Lock()
_GUI_LEASE_LOCKS: dict[str, threading.Lock] = {}
_GUI_LEASE_LOCKS_GUARD = threading.Lock()


# Optional V1 capability discovery. These values describe the verified
# primitives in the currently reused X11 controller, not theoretical Core API
# request shapes. Consumers can use them to avoid queuing requests that this
# concrete sender would later fail asynchronously.
LEGACY_SEND_CAPABILITIES: dict[str, Any] = {
    "text": False,
    "image": False,
    "file": False,
    "native_reply": False,
    "media_caption": False,
    "max_mentions": 0,
    "echo_confirmation": False,
    "verified_chat_target": False,
}

AGENT_WECHAT_SEND_CAPABILITIES: dict[str, Any] = {
    "text": True,
    "image": True,
    "file": True,
    "native_reply": False,
    "media_caption": False,
    "max_mentions": 0,
    "echo_confirmation": False,
    "verified_chat_target": True,
}

NATIVE_SEND_CAPABILITIES: dict[str, Any] = {
    "text": False,
    "image": False,
    "file": False,
    "native_reply": False,
    "media_caption": False,
    "max_mentions": 0,
    "echo_confirmation": False,
    "verified_chat_target": False,
    "available": False,
    "configured": False,
    "bridge_detected": False,
    "transport": "unix_socket",
    "reason": "native bridge is reserved but disabled until an upstream send API exists",
}


def detect_native_sender_capabilities() -> dict[str, Any]:
    """Report whether a future native bridge endpoint is present, fail-closed.

    Presence alone never enables native sending.  A future driver must add an
    explicit, versioned capability handshake before `available` can become
    true.  This lets wechat-shot-bridge coexist today without guessing internal
    WeChat send symbols or turning injection on by default.
    """

    result = dict(NATIVE_SEND_CAPABILITIES)
    socket_path = os.environ.get("WECHAT_NATIVE_DRIVER_SOCKET", "").strip()
    result["configured"] = bool(socket_path)
    result["bridge_detected"] = bool(
        socket_path and os.name == "posix" and Path(socket_path).is_socket()
    )
    if result["bridge_detected"]:
        result["reason"] = (
            "native bridge endpoint detected, but no versioned send capability "
            "handshake is implemented; native sending remains disabled"
        )
    return result


def sender_capabilities() -> dict[str, Any]:
    return {
        **LEGACY_SEND_CAPABILITIES,
        "drivers": {
            "legacy": dict(LEGACY_SEND_CAPABILITIES),
            "agent_wechat": dict(AGENT_WECHAT_SEND_CAPABILITIES),
            "native": detect_native_sender_capabilities(),
        },
    }


SEND_CAPABILITIES: dict[str, Any] = sender_capabilities()


class DeliveryUncertainError(RuntimeError):
    """A send may have reached upstream, but no authoritative response arrived."""

    code = "agent_wechat_delivery_unknown"

    def __init__(self, message: str, *, details: dict[str, Any] | None = None) -> None:
        super().__init__(message)
        self.details = dict(details or {})
        self.details.setdefault("delivery_certainty", "unknown")
        self.details.setdefault("automatic_retry", False)


def display_lock(display: str) -> threading.RLock:
    with _DISPLAY_LOCKS_GUARD:
        return _DISPLAY_LOCKS.setdefault(display, threading.RLock())


def _account_gui_lease_path(account_id: str) -> Path:
    root = Path(os.environ.get("WECHAT_GUI_LEASE_DIR", "/run/wechat-runtime/locks"))
    digest = hashlib.sha256(str(account_id).encode("utf-8")).hexdigest()[:32]
    return root / f"account-gui-{digest}.lock"


@contextmanager
def account_gui_lease(account_id: str):
    """Try to reserve one account's GUI without racing an interactive desktop.

    Runtime's Desktop Gateway takes the same non-blocking flock while a real
    browser control WebSocket is connected.  A busy lease therefore means the
    operator is manually driving that exact WeChat GUI.  Sender must defer the
    row without incrementing attempt_count or touching upstream.

    Windows unit tests do not provide fcntl; the process-local fallback keeps
    the state machine testable while Linux production uses the shared file
    lock across Runtime and Core containers.
    """

    path = _account_gui_lease_path(account_id)
    if fcntl is None:  # pragma: no cover - exercised on Windows test hosts.
        with _GUI_LEASE_LOCKS_GUARD:
            lock = _GUI_LEASE_LOCKS.setdefault(str(path), threading.Lock())
        acquired = lock.acquire(blocking=False)
        try:
            yield acquired
        finally:
            if acquired:
                lock.release()
        return

    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        handle = path.open("a+", encoding="utf-8")
    except OSError:
        # Fail closed. A broken shared lease path must never allow automated
        # GUI input to race an operator; leave the outbox row pending instead.
        yield False
        return
    acquired = False
    try:
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            acquired = True
        except BlockingIOError:
            acquired = False
        except OSError:
            acquired = False
        yield acquired
    finally:
        if acquired:
            fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


@contextmanager
def account_display_lock(account: AccountConfig):
    """Serialize display-global input, optionally sharing Runtime's flock file."""
    with display_lock(account.display):
        lock_path = str(account.runtime.get("display_lock") or "").strip()
        if not lock_path or fcntl is None:
            yield
            return
        path = Path(lock_path)
        if not path.parent.exists():
            raise RuntimeError(
                f"Runtime display lock directory is not visible to Core: {path.parent}; "
                "mount the Runtime lock directory into Core before enabling the sender"
            )
        with path.open("a+", encoding="utf-8") as handle:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
            try:
                yield
            finally:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)


def b64(value: str) -> str:
    return base64.b64encode(value.encode("utf-8")).decode("ascii")


class NativeSenderDriver:
    """Reserved interface for a future upstream-native sender implementation."""

    capabilities = NATIVE_SEND_CAPABILITIES

    def send(self, kind: str, account_id: str, chat_id: str, request: dict[str, Any]) -> dict[str, Any]:
        del kind, account_id, chat_id, request
        raise RuntimeError(
            "native sender is disabled: wechat-shot-bridge currently provides screenshot injection only, not a stable send API"
        )


class AgentWechatSenderDriver:
    """Thin HTTP adapter around upstream agent-wechat's verified send endpoint."""

    capabilities = AGENT_WECHAT_SEND_CAPABILITIES

    def __init__(self, registry: AccountRegistry, store: CoreStore) -> None:
        self.registry = registry
        self.store = store

    @staticmethod
    def _token(account: AccountConfig) -> str:
        token_file = Path(str(account.runtime.get("agent_wechat_token_file") or ""))
        if not token_file.is_file():
            raise RuntimeError(f"agent-wechat token file is unavailable for {account.account_id}")
        token = token_file.read_text(encoding="utf-8").strip()
        if not token:
            raise RuntimeError(f"agent-wechat token file is empty for {account.account_id}")
        return token

    def _request(self, account: AccountConfig, payload: dict[str, Any]) -> dict[str, Any]:
        base_url = str(account.runtime.get("agent_wechat_base_url") or "").rstrip("/")
        if not base_url:
            raise RuntimeError(f"agent-wechat base URL is missing for {account.account_id}")
        body = json.dumps(payload, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        req = urllib.request.Request(
            f"{base_url}/api/messages/send",
            data=body,
            method="POST",
            headers={
                "Authorization": f"Bearer {self._token(account)}",
                "Content-Type": "application/json",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=45) as response:
                raw = response.read(2 * 1024 * 1024 + 1)
        except urllib.error.HTTPError as exc:
            detail = exc.read(64 * 1024).decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"agent-wechat send returned HTTP {exc.code}: {detail}") from exc
        except (urllib.error.URLError, OSError, TimeoutError) as exc:
            raise DeliveryUncertainError(
                "agent-wechat send response was not received; delivery may already have occurred",
                details={
                    "driver": "agent_wechat",
                    "phase": "awaiting_upstream_response",
                    "transport_error": type(exc).__name__,
                },
            ) from exc
        if len(raw) > 2 * 1024 * 1024:
            raise RuntimeError("agent-wechat send response exceeded safety limit")
        try:
            result = json.loads(raw.decode("utf-8")) if raw else {}
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("agent-wechat send returned invalid JSON") from exc
        if not isinstance(result, dict):
            raise RuntimeError("agent-wechat send response must be an object")
        if result.get("success") is False or result.get("ok") is False:
            raise RuntimeError(str(result.get("error") or "agent-wechat send failed"))
        return result

    def _preopen_chat(self, account: AccountConfig, chat_id: str) -> dict[str, Any]:
        """Stabilize the target chat before entering upstream's send plan.

        agent-wechat's send plan can open a previously unopened chat and then
        immediately advance into focus/input actions.  On a freshly logged-in
        account that UI transition can race the input action while the upstream
        plan still reaches its disabled-Send-button success condition.  Opening
        the chat through the upstream's dedicated endpoint first separates the
        non-delivery UI transition from the delivery attempt and gives us one
        more exact-target check before any message can be submitted.

        Failure here is deterministic (no send endpoint has been called yet),
        so it must fail closed rather than become delivery uncertainty.
        """

        base_url = str(account.runtime.get("agent_wechat_base_url") or "").rstrip("/")
        if not base_url:
            raise RuntimeError(f"agent-wechat base URL is missing for {account.account_id}")
        encoded_chat = urllib.parse.quote(str(chat_id), safe="")
        req = urllib.request.Request(
            f"{base_url}/api/chats/{encoded_chat}/open?clearUnreads=false",
            data=b"",
            method="POST",
            headers={"Authorization": f"Bearer {self._token(account)}"},
        )
        try:
            with urllib.request.urlopen(req, timeout=30) as response:
                raw = response.read(256 * 1024 + 1)
        except urllib.error.HTTPError as exc:
            detail = exc.read(64 * 1024).decode("utf-8", errors="replace").strip()
            raise RuntimeError(f"agent-wechat chat pre-open returned HTTP {exc.code}: {detail}") from exc
        except (urllib.error.URLError, OSError, TimeoutError) as exc:
            raise RuntimeError(
                f"agent-wechat chat pre-open failed before submission: {type(exc).__name__}"
            ) from exc
        if len(raw) > 256 * 1024:
            raise RuntimeError("agent-wechat chat pre-open response exceeded safety limit")
        try:
            result = json.loads(raw.decode("utf-8")) if raw else {}
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeError("agent-wechat chat pre-open returned invalid JSON") from exc
        if not isinstance(result, dict):
            raise RuntimeError("agent-wechat chat pre-open response must be an object")
        if result.get("ok") is False or result.get("success") is False:
            raise RuntimeError(str(result.get("error") or "agent-wechat chat pre-open failed"))
        opened_username = str(result.get("username") or "").strip()
        if opened_username and opened_username != str(chat_id):
            raise RuntimeError(
                f"agent-wechat chat pre-open target mismatch: expected {chat_id}, got {opened_username}"
            )
        return result

    @staticmethod
    def _reject_unverified_semantics(request: dict[str, Any]) -> None:
        if str(request.get("target_message_id") or "").strip():
            raise RuntimeError("agent-wechat does not expose a verified native reply primitive; request was not sent")
        mention_ids = request.get("mention_member_ids") or []
        if mention_ids:
            raise RuntimeError("agent-wechat send API does not expose verified mention semantics; request was not sent")

    def send(self, kind: str, account_id: str, chat_id: str, request: dict[str, Any]) -> dict[str, Any]:
        account = resolve_runtime_account(self.registry.require(account_id))
        if account.runtime.get("agent_server_healthy") is False:
            raise RuntimeError(
                str(account.runtime.get("health_error") or "agent-wechat agent-server is unhealthy")
            )
        chat = self.store.chat(account_id, chat_id)
        if chat is None:
            raise RuntimeError("target chat is no longer present in normalized Core data")
        self._reject_unverified_semantics(request)
        payload: dict[str, Any] = {"chatId": str(chat["chat_id"])}
        if kind == "text":
            text = str(request.get("text") or "").strip()
            if not text:
                raise RuntimeError("text is empty")
            payload["text"] = text
        elif kind in {"image", "file"}:
            media_id = str(request.get("media_id") or "").strip()
            media = self.store.media(account_id, media_id)
            if media is None:
                raise RuntimeError("Core media is no longer present")
            path = Path(str(media.get("local_path") or ""))
            if not path.is_file():
                raise RuntimeError("Core media file is unavailable")
            data = base64.b64encode(path.read_bytes()).decode("ascii")
            filename = str(media.get("filename") or path.name or "attachment.bin")
            mime_type = str(media.get("mime_type") or mimetypes.guess_type(filename)[0] or "application/octet-stream")
            if kind == "image":
                payload["image"] = {"data": data, "mimeType": mime_type}
            else:
                payload["file"] = {"data": data, "filename": filename}
        else:
            raise RuntimeError(f"unsupported send kind for agent-wechat: {kind}")
        preopen = self._preopen_chat(account, str(chat["chat_id"]))
        upstream = self._request(account, payload)
        return {
            "driver": "agent_wechat",
            "preopen": preopen,
            "upstream": upstream,
            "confirmed": False,
            "note": "agent-wechat FSM accepted the send; Core has not observed a matching DB echo yet.",
        }


class LegacySenderDriver:
    capabilities = LEGACY_SEND_CAPABILITIES

    def __init__(self, owner: "AccountSender") -> None:
        self.owner = owner

    def send(self, kind: str, account_id: str, chat_id: str, request: dict[str, Any]) -> dict[str, Any]:
        if kind == "text":
            return self.owner._send_text(account_id, chat_id, request)
        if kind == "image":
            return self.owner._send_image(account_id, chat_id, request)
        raise RuntimeError("upstream X11 controller has no verified file-paste primitive; request was not sent")


class AccountSender:
    """Uses existing controller actions and serializes clipboard/window access per display."""

    def __init__(self, registry: AccountRegistry, store: CoreStore, *, root: Path) -> None:
        self.registry = registry
        self.store = store
        self.root = root
        self._legacy_driver = LegacySenderDriver(self)
        self._agent_wechat_driver = AgentWechatSenderDriver(registry, store)
        self._native_driver = NativeSenderDriver()
        self._account_locks: dict[str, threading.RLock] = {}
        self._account_locks_guard = threading.Lock()

    def _account_lock(self, account_id: str) -> threading.RLock:
        with self._account_locks_guard:
            return self._account_locks.setdefault(account_id, threading.RLock())

    def _driver_for(self, account: AccountConfig):
        driver = str(account.runtime.get("sender_driver") or account.runtime_provider or "legacy")
        if driver == "agent_wechat":
            return self._agent_wechat_driver
        if driver == "native":
            return self._native_driver
        if driver == "legacy":
            return self._legacy_driver
        raise RuntimeError(f"unsupported sender driver for {account.account_id}: {driver}")

    def _controller_command(self, account_id: str) -> tuple[list[str], dict[str, str]]:
        account = resolve_runtime_account(self.registry.require(account_id))
        if (
            len(self.registry.all()) > 1
            and not account.window_id
            and not bool(account.runtime.get("controller_resolves_account", False))
        ):
            raise RuntimeError(
                "multi-account sender requires runtime.window_id or a controller that resolves the account window; "
                "global WeChat window discovery is unsafe"
            )
        configured = account.runtime.get("controller_command")
        if configured:
            if not isinstance(configured, list) or not configured or not all(isinstance(item, str) and item for item in configured):
                raise RuntimeError("runtime.controller_command must be a non-empty command array")
            command = list(configured)
        else:
            command = [sys.executable, str(self.root / "agent_console" / "wechat_controller.py")]
        env = dict(os.environ)
        env["WECHAT_DISPLAY"] = account.display
        if account.window_id:
            env["WECHAT_WINDOW_ID"] = account.window_id
        xauthority = str(account.runtime.get("xauthority") or "").strip()
        if xauthority and Path(xauthority).exists():
            env["XAUTHORITY"] = xauthority
        return command, env

    def _run_controller(self, account_id: str, args: list[str], *, timeout: int = 45) -> dict[str, Any]:
        command, env = self._controller_command(account_id)
        process = subprocess.run(
            [*command, *args],
            cwd=self.root,
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout,
        )
        output = (process.stdout or "").strip().splitlines()
        try:
            payload = json.loads(output[-1]) if output else {}
        except json.JSONDecodeError:
            payload = {}
        if process.returncode != 0 or not payload.get("ok"):
            detail = str(payload.get("error") or process.stderr or process.stdout or "controller failed").strip()
            raise RuntimeError(detail)
        return payload

    @staticmethod
    def _require_verified_chat_target(account: AccountConfig) -> None:
        if not bool(account.runtime.get("controller_verifies_chat_target", False)):
            raise RuntimeError(
                "X11 controller cannot verify the exact target chat; request was not sent"
            )

    def _send_text(self, account_id: str, chat_id: str, request: dict[str, Any]) -> dict[str, Any]:
        chat = self.store.chat(account_id, chat_id)
        if chat is None:
            raise RuntimeError("target chat is no longer present in normalized Core data")
        text = str(request.get("text") or "").strip()
        if not text:
            raise RuntimeError("text is empty")
        target_message_id = str(request.get("target_message_id") or "").strip()
        if target_message_id:
            # The reused upstream X11 controller has no verified native
            # quote/reply primitive.  Never silently drop reply semantics and
            # send an unthreaded message to the wrong context.
            raise RuntimeError(
                "native target_message_id reply is not verified by the upstream X11 controller; request was not sent"
            )
        mention_ids = request.get("mention_member_ids") or []
        if not isinstance(mention_ids, list):
            raise RuntimeError("mention_member_ids is not an array")
        if len(mention_ids) > 1:
            raise RuntimeError("the upstream X11 controller currently verifies one blue mention per send; request was not sent")
        paste_args = ["paste", "--text-b64", b64(text), "--send-delay", "0"]
        paste_label = "paste"
        if mention_ids:
            member_id = str(mention_ids[0]).strip()
            member = self.store.member(account_id, chat_id, member_id)
            if member is None:
                raise RuntimeError(f"mention member is not present in normalized chat membership: {member_id}")
            alias = str(member.get("alias") or "").strip().lstrip("@")
            display_name = str(member.get("display_name") or member_id).strip()
            if not alias:
                raise RuntimeError(
                    f"mention member has no verified alias for blue mention: {member_id}; request was not sent"
                )
            paste_args = [
                "mention-paste",
                "--text-b64",
                b64(text),
                "--mention-alias-b64",
                b64(alias),
                "--mention-display-b64",
                b64(display_name),
                "--send-delay",
                "0",
            ]
            paste_label = "mention"
        account = self.registry.require(account_id)
        self._require_verified_chat_target(account)
        with account_display_lock(account):
            opened = self._run_controller(
                account_id,
                ["open", "--chat-name-b64", b64(str(chat["display_name"])), "--switch-delay", "0.5"],
            )
            pasted = self._run_controller(account_id, paste_args)
            submitted = self._run_controller(account_id, ["submit", "--send-delay", "0"])
        return {
            "controller": {"open": opened, paste_label: pasted, "submit": submitted},
            "confirmed": False,
            "note": "X11 submit completed; Core has not observed a WeChat echo message.",
        }

    def _send_image(self, account_id: str, chat_id: str, request: dict[str, Any]) -> dict[str, Any]:
        chat = self.store.chat(account_id, chat_id)
        media_id = str(request.get("media_id") or "")
        media = self.store.media(account_id, media_id)
        if chat is None or media is None:
            raise RuntimeError("target chat or media is no longer present in Core")
        path = Path(str(media["local_path"]))
        if not path.exists() or not path.is_file():
            raise RuntimeError("Core media file is unavailable")
        account = self.registry.require(account_id)
        self._require_verified_chat_target(account)
        with account_display_lock(account):
            opened = self._run_controller(
                account_id,
                ["open", "--chat-name-b64", b64(str(chat["display_name"])), "--switch-delay", "0.5"],
            )
            pasted = self._run_controller(account_id, ["image", "--image-path-b64", b64(str(path)), "--send-delay", "0"])
            submitted = self._run_controller(account_id, ["submit", "--send-delay", "0"])
        return {"controller": {"open": opened, "image": pasted, "submit": submitted}, "confirmed": False, "note": "X11 image submit completed; Core has not observed a WeChat echo message."}

    def _process_account_rows(self, account_id: str, rows: list[Any]) -> dict[str, int]:
        result = {"processed": 0, "submitted": 0, "sent": 0, "failed": 0, "uncertain": 0, "deferred": 0}
        with self._account_lock(account_id):
            for row in rows:
                account = self.registry.get(account_id)
                if account is None or not account.sender_enabled:
                    result["deferred"] += 1
                    continue
                with account_gui_lease(account_id) as gui_available:
                    if not gui_available:
                        # A live browser desktop is manually controlling this
                        # account. Keep accepted/queued untouched so the next
                        # outbox cycle can retry after the operator disconnects.
                        result["deferred"] += 1
                        continue
                    result["processed"] += 1
                    driver_name = str(account.runtime.get("sender_driver") or account.runtime_provider or "legacy")
                    transition_details = {
                        "runtime": {
                            "display": account.display,
                            "runtime_provider": account.runtime_provider,
                            "sender_driver": driver_name,
                        }
                    }
                    if str(row.get("status") or "") == "accepted":
                        self.store.transition_send(row["send_id"], "queued", details=transition_details)
                    self.store.transition_send(row["send_id"], "sending", details=transition_details)
                    request = parse_json(row["request_json"], {})
                    try:
                        driver = self._driver_for(account)
                        details = {
                            **transition_details,
                            **driver.send(str(row["kind"]), account_id, str(row["chat_id"]), request),
                            "delivery_certainty": "pending_confirmation",
                            "automatic_retry": False,
                        }
                        self.store.transition_send(row["send_id"], "submitted", details=details)
                        result["submitted"] += 1
                    except DeliveryUncertainError as exc:
                        details = {
                            **transition_details,
                            **exc.details,
                            "error_code": exc.code,
                        }
                        self.store.transition_send(
                            row["send_id"],
                            "uncertain",
                            details=details,
                            error=str(exc),
                            error_code=exc.code,
                        )
                        result["uncertain"] += 1
                    except Exception as exc:
                        self.store.transition_send(
                            row["send_id"],
                            "failed",
                            details=transition_details,
                            error=str(exc),
                        )
                        result["failed"] += 1
        return result

    def process_pending(self, *, limit: int = 20) -> dict[str, int]:
        try:
            lease_seconds = max(30.0, float(os.environ.get("WECHAT_SENDING_LEASE_SECONDS", "120")))
        except ValueError:
            lease_seconds = 120.0
        self.store.recover_stale_sends(max_age_seconds=lease_seconds)
        try:
            confirmation_seconds = max(
                5.0, float(os.environ.get("WECHAT_SEND_CONFIRMATION_SECONDS", "120"))
            )
        except ValueError:
            confirmation_seconds = 120.0
        self.store.expire_submitted_sends(max_age_seconds=confirmation_seconds)
        result = {"processed": 0, "submitted": 0, "sent": 0, "failed": 0, "uncertain": 0, "deferred": 0}
        grouped: dict[str, list[Any]] = {}
        for row in self.store.pending_sends(limit=limit):
            account_id = str(row["account_id"])
            account = self.registry.get(account_id)
            if account is None or not account.sender_enabled:
                result["deferred"] += 1
                continue
            grouped.setdefault(account_id, []).append(row)
        if not grouped:
            return result

        # A single account remains strictly serial. Distinct accounts use
        # independent Runtime/driver locks and can send concurrently.
        try:
            configured_workers = max(1, int(os.environ.get("WECHAT_SENDER_ACCOUNT_WORKERS", "8")))
        except ValueError:
            configured_workers = 8
        workers = min(len(grouped), configured_workers)
        if workers == 1:
            partials = [self._process_account_rows(account_id, rows) for account_id, rows in grouped.items()]
        else:
            partials = []
            with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="wechat-send") as pool:
                futures = {
                    pool.submit(self._process_account_rows, account_id, rows): account_id
                    for account_id, rows in grouped.items()
                }
                for future in as_completed(futures):
                    partials.append(future.result())
        for partial in partials:
            for key in result:
                result[key] += partial[key]
        return result


class OutboxLoop:
    def __init__(self, sender: AccountSender, interval_seconds: float) -> None:
        self.sender = sender
        self.interval_seconds = max(0.5, float(interval_seconds))
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="wechat-core-outbox", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=max(2.0, self.interval_seconds + 1.0))

    def _run(self) -> None:
        while not self._stop.is_set():
            self.sender.process_pending()
            self._stop.wait(self.interval_seconds)
