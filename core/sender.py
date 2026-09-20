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
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from .identity import IdentityError
from .registry import AccountConfig, AccountRegistry, provider_sender_capabilities
from .runtime_bridge import resolve_runtime_account
from .store import CoreStore, parse_json, parse_rfc3339, utc_now

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


# Product-facing reason vocabulary.  A send must never surface a raw traceback
# or an internal exception class to the Console; it surfaces one of these.
SEND_BLOCK_MESSAGES: dict[str, str] = {
    "sender_disabled": "该账号的发送功能已关闭。",
    "driver_not_send_capable": "该账号的运行驱动不支持发送。",
    "wechat_client_unavailable": "微信客户端当前不可用，请稍后重试。",
    "wechat_not_logged_in": "微信正在完成登录，请稍候。",
    "wechat_ready_unknown": "正在确认微信登录状态，请稍候。",
    "account_unknown": "账号不存在或已移除。",
}


def send_block_message(code: str) -> str:
    return SEND_BLOCK_MESSAGES.get(str(code or ""), "当前无法发送，请稍后重试。")


# Readiness blocks that must reject at enqueue time instead of parking the
# message in the outbox.  The account advertises a send-capable driver, but the
# live WeChat client is not usable yet, so queueing would only produce an
# indefinitely "queued" message from the user's point of view.
ENQUEUE_BLOCK_CODES: frozenset[str] = frozenset(
    {"wechat_client_unavailable", "wechat_not_logged_in", "wechat_ready_unknown"}
)


def is_enqueue_block(code: str) -> bool:
    return str(code or "") in ENQUEUE_BLOCK_CODES


# Product-facing vocabulary for a *dispatch* failure (as opposed to an enqueue
# rejection).  The Console renders ``user_message``; the raw internal message is
# retained on the receipt for operators.
SEND_FAILURE_MESSAGES: dict[str, str] = {
    "wechat_not_ready": "微信正在完成登录，请稍候。",
    "wechat_unavailable": "微信客户端当前不可用，请稍后重试。",
    "sender_unavailable": "发送服务暂不可用，请稍后重试。",
    "send_timeout": "发送超时，请稍后重试。",
    "target_unavailable": "目标会话已不可用，请刷新后重试。",
    "operator_gui_busy": "微信界面正在被手动操作，请稍后重试。",
    "sender_failed": "发送失败，请稍后重试。",
}


def send_failure_message(code: str) -> str:
    return SEND_FAILURE_MESSAGES.get(str(code or ""), SEND_FAILURE_MESSAGES["sender_failed"])


# ``uncertain`` is not a plain failure: upstream reported success but Core never
# observed a matching outgoing echo, so the message may or may not have been
# delivered.  Never invite an automatic retry from this state.
UNCERTAIN_DELIVERY_MESSAGE = "未能确认微信是否已接收，请核对后决定是否重发。"


def classify_send_failure(exc: BaseException) -> str:
    """Map an internal dispatch failure to a stable, product-level code.

    Never leaks the exception class or a traceback to the Console; the raw text
    stays on the receipt for operators.
    """

    explicit = str(getattr(exc, "code", "") or "")
    if explicit in SEND_FAILURE_MESSAGES:
        return explicit

    text = f"{type(exc).__name__}: {exc}".lower()
    if isinstance(exc, TimeoutError) or "timed out" in text or "timeout" in text:
        return "send_timeout"
    if "token file" in text or "base url is missing" in text or "sending service" in text:
        return "sender_unavailable"
    if "no longer present" in text or "no longer in normalized" in text or "target chat" in text:
        return "target_unavailable"
    if "not logged in" in text or "not ready" in text or "logged_out" in text:
        return "wechat_not_ready"
    if (
        "http 5" in text
        or "http 4" in text
        or "connection refused" in text
        or "connection reset" in text
        or "name or service not known" in text
        or "temporary failure in name resolution" in text
        or "unreachable" in text
    ):
        return "wechat_unavailable"
    return "sender_failed"


def account_send_readiness(account: AccountConfig) -> dict[str, Any]:
    """Authoritative, product-level send readiness for one account.

    Readiness is derived from the *same* authoritative runtime observation that
    drives the account state machine (``wechat_login_status`` / ``running`` /
    ``agent_server_healthy``), never from a cached "logged in once" flag and
    never from the account's own ``sender_capabilities`` claim.  A console that
    queues a message for an account which is not ready would otherwise hold it
    in ``queued`` indefinitely.
    """

    provider = account.runtime_provider
    capabilities = provider_sender_capabilities(provider)
    row: dict[str, Any] = {
        "account_id": account.account_id,
        "runtime_provider": provider,
        "sender_enabled": bool(account.sender_enabled),
        "send_ready": False,
        "send_blocked_reason": "",
        "send_blocked_message": "",
        "capabilities": capabilities,
    }

    def block(code: str) -> dict[str, Any]:
        row["send_blocked_reason"] = code
        row["send_blocked_message"] = send_block_message(code)
        return row

    if not account.sender_enabled or not bool(account.runtime.get("enabled", True)):
        return block("sender_disabled")
    if not any(bool(value) for value in capabilities.values() if isinstance(value, bool)):
        return block("driver_not_send_capable")
    if provider != "agent_wechat":
        return block("driver_not_send_capable")

    running = bool(account.runtime.get("running", False))
    container_running = bool(account.runtime.get("container_running", running))
    if not running or not container_running:
        return block("wechat_client_unavailable")
    if account.runtime.get("agent_server_healthy") is False:
        return block("wechat_client_unavailable")
    login_status = str(account.runtime.get("wechat_login_status") or "").strip()
    if login_status == "logged_out":
        return block("wechat_not_logged_in")
    if login_status != "logged_in":
        # ``unknown`` / empty telemetry is not proof of readiness.  Report it as
        # "still settling" rather than "not logged in" so the UI can say
        # "微信正在完成登录，请稍候" without lying about a logout.
        return block("wechat_ready_unknown")

    row["send_ready"] = True
    return row


def effective_sender_capabilities(accounts: Iterable[AccountConfig]) -> dict[str, Any]:
    """Aggregate the *effective* send capability of the live account set.

    The top level answers "what can this deployment send right now", i.e. the
    union over accounts that are actually ready to send.  The per-driver
    breakdown stays under ``drivers`` for compatibility.

    Regression guard (P0-0B): a deployment whose only account is an online,
    logged-in, sender-enabled AgentWechat account must report ``text: true``.
    It must never be downgraded to ``false`` merely because the legacy/default
    driver is unused or because no legacy account exists.
    """

    rows = [account_send_readiness(account) for account in accounts]
    aggregate: dict[str, Any] = {**LEGACY_SEND_CAPABILITIES}
    contributing: list[str] = []
    for row in rows:
        if not row["send_ready"]:
            continue
        contributing.append(row["account_id"])
        for key, value in row["capabilities"].items():
            if isinstance(value, bool):
                aggregate[key] = bool(aggregate.get(key)) or value
            elif isinstance(value, int):
                aggregate[key] = max(int(aggregate.get(key) or 0), value)
    return {
        **aggregate,
        "drivers": {
            "legacy": dict(LEGACY_SEND_CAPABILITIES),
            "agent_wechat": dict(AGENT_WECHAT_SEND_CAPABILITIES),
            "native": detect_native_sender_capabilities(),
        },
        "effective_from_accounts": sorted(contributing),
        "accounts_total": len(rows),
        "accounts_send_ready": len(contributing),
        "account_readiness": rows,
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


class SenderError(RuntimeError):
    """A send failed deterministically, with an explicit product-level code.

    Raising with an explicit ``code`` keeps the product-facing reason stable
    instead of relying on substring matching against an internal message.
    """

    def __init__(self, message: str, *, code: str = "sender_failed") -> None:
        super().__init__(message)
        self.code = str(code or "sender_failed")


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
        return result
        if not isinstance(result, dict):
            raise RuntimeError("agent-wechat send response must be an object")
        if result.get("success") is False or result.get("ok") is False:
            raise RuntimeError(str(result.get("error") or "agent-wechat send failed"))
        return result

    def _preopen_chat(self, account: AccountConfig, chat_id: str) -> dict[str, Any]:
        """Open one chat through the upstream endpoint.

        DO NOT call this on the send path -- see ``send()``.  It is retained as
        a documented primitive (and for the lifecycle/debug surface), but
        pre-opening a chat that is not already the active one makes the
        immediately following send silently no-op upstream.
        """

        base_url = str(account.runtime.get("agent_wechat_base_url") or "").rstrip("/")
        if not base_url:
            raise SenderError(
                f"agent-wechat base URL is missing for {account.account_id}",
                code="sender_unavailable",
            )
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
            raise SenderError(
                f"agent-wechat chat pre-open returned HTTP {exc.code}: {detail}",
                code="wechat_unavailable",
            ) from exc
        except (urllib.error.URLError, OSError, TimeoutError) as exc:
            raise SenderError(
                f"agent-wechat chat pre-open failed before submission: {type(exc).__name__}",
                code="wechat_unavailable",
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
        """Dispatch one send through upstream agent-wechat.

        The upstream send plan owns the whole verified sequence: open the exact
        target, positively verify the open chat header, resolve the active
        composer in the active frame, type, submit, then confirm.

        Core deliberately does **not** pre-open the chat first.  Reproduced on
        the Factory Fresh deployment (A/B/D probes, 2026-09-19): pre-opening a
        chat that is not already the active one transitions the UI, the send
        plan then reports "target already verified open" and skips its own
        Opening/VerifyingPrimaryOpen phases, and it finally declares success
        from its ``Send button DISABLED`` condition -- which an *empty*
        composer also satisfies.  The message is never delivered, yet the plan
        returns ``{"success": true}``.  Letting the plan perform its own open
        delivers correctly (verified for both a never-opened chat and a
        previously-opened one).

        A false upstream success is still caught by Core: without a unique
        outgoing DB echo inside the confirmation window the send converges to
        ``uncertain`` instead of ``sent``.
        """

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
        upstream = self._request(account, payload)
        return {
            "driver": "agent_wechat",
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
                try:
                    # Send Gate re-check at dispatch time (B6): the queue-time
                    # binding may have changed (mismatch or confirmed switch).
                    # A row whose intended identity no longer matches the slot
                    # must fail closed instead of reaching upstream WeChat.
                    self.store.identity_send_gate(
                        account_id,
                        expected_wechat_identity_uuid=str(row["wechat_identity_uuid"] or ""),
                    )
                except IdentityError as exc:
                    self.store.transition_send(
                        row["send_id"],
                        "failed",
                        details={"identity": {"code": exc.code, **dict(exc.details)}},
                        error=str(exc),
                        error_code=exc.code,
                    )
                    result["failed"] += 1
                    continue
                with account_gui_lease(account_id) as gui_available:
                    if not gui_available:
                        # A live browser desktop is manually controlling this
                        # account. Deferral is intentional, but it must be
                        # bounded: an indefinitely held lease (a leaked desktop
                        # session) must not pin a message in ``queued`` forever.
                        if self._defer_window_exceeded(row):
                            code = "operator_gui_busy"
                            self.store.transition_send(
                                row["send_id"],
                                "failed",
                                details={
                                    "failure": {
                                        "code": code,
                                        "reason": "operator_gui_lease_held",
                                        "user_message": send_failure_message(code),
                                    }
                                },
                                error="interactive desktop lease was held past the deferral window",
                                error_code=code,
                            )
                            result["failed"] += 1
                        else:
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
                        code = classify_send_failure(exc)
                        self.store.transition_send(
                            row["send_id"],
                            "failed",
                            details={
                                **transition_details,
                                "failure": {
                                    "code": code,
                                    "reason": str(exc),
                                    "user_message": send_failure_message(code),
                                },
                            },
                            error=str(exc),
                            error_code=code,
                        )
                        result["failed"] += 1
        return result

    def _defer_window_exceeded(self, row: Any) -> bool:
        """True once an accepted/queued row has waited past the deferral window."""

        try:
            limit = max(30.0, float(os.environ.get("WECHAT_GUI_LEASE_DEFER_MAX_SECONDS", "600")))
        except ValueError:
            limit = 600.0
        try:
            accepted = parse_rfc3339(str(row["accepted_at"] or ""))
        except (KeyError, IndexError, TypeError):
            return False
        if accepted is None:
            return False
        return (datetime.now(timezone.utc) - accepted).total_seconds() > limit

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
    """Background outbox dispatcher.

    The loop must never die silently: a raised exception inside
    ``process_pending`` (store contention, a driver bug, a registry reload race)
    used to terminate the thread permanently, which left every accepted message
    queued forever with no observable error anywhere.  Failures are now
    recorded and the next cycle continues.
    """

    def __init__(self, sender: AccountSender, interval_seconds: float) -> None:
        self.sender = sender
        self.interval_seconds = max(0.5, float(interval_seconds))
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="wechat-core-outbox", daemon=True)
        self._liveness_guard = threading.Lock()
        self._started_at = ""
        self._cycle_count = 0
        self._last_cycle_at = ""
        self._last_cycle_result: dict[str, int] = {}
        self._last_error = ""
        self._last_error_at = ""
        self._consecutive_failures = 0

    def start(self) -> None:
        with self._liveness_guard:
            self._started_at = utc_now()
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=max(2.0, self.interval_seconds + 1.0))

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def liveness(self) -> dict[str, Any]:
        with self._liveness_guard:
            return {
                "worker": "wechat-core-outbox",
                "started_at": self._started_at,
                "interval_seconds": self.interval_seconds,
                "alive": self._thread.is_alive(),
                "cycle_count": self._cycle_count,
                "last_cycle_at": self._last_cycle_at,
                "last_cycle_result": dict(self._last_cycle_result),
                "last_error": self._last_error,
                "last_error_at": self._last_error_at,
                "consecutive_failures": self._consecutive_failures,
            }

    def _record_cycle(self, result: dict[str, int]) -> None:
        with self._liveness_guard:
            self._cycle_count += 1
            self._last_cycle_at = utc_now()
            self._last_cycle_result = dict(result or {})
            self._last_error = ""
            self._consecutive_failures = 0

    def _record_error(self, exc: BaseException) -> None:
        with self._liveness_guard:
            self._cycle_count += 1
            self._last_cycle_at = utc_now()
            self._last_error = f"{type(exc).__name__}: {exc}"
            self._last_error_at = self._last_cycle_at
            self._consecutive_failures += 1

    def _run(self) -> None:
        while not self._stop.is_set():
            try:
                self._record_cycle(self.sender.process_pending())
            except Exception as exc:  # noqa: BLE001 - the loop must survive.
                self._record_error(exc)
            self._stop.wait(self.interval_seconds)
