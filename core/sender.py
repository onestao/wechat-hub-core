"""Account-aware adapter around the upstream X11 WeChat controller."""

from __future__ import annotations

import base64
import json
import os
import subprocess
import sys
import threading
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


def display_lock(display: str) -> threading.RLock:
    with _DISPLAY_LOCKS_GUARD:
        return _DISPLAY_LOCKS.setdefault(display, threading.RLock())


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


class AccountSender:
    """Uses existing controller actions and serializes clipboard/window access per display."""

    def __init__(self, registry: AccountRegistry, store: CoreStore, *, root: Path) -> None:
        self.registry = registry
        self.store = store
        self.root = root

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
        with account_display_lock(account):
            opened = self._run_controller(
                account_id,
                ["open", "--chat-name-b64", b64(str(chat["display_name"])), "--switch-delay", "0.5"],
            )
            pasted = self._run_controller(account_id, ["image", "--image-path-b64", b64(str(path)), "--send-delay", "0"])
            submitted = self._run_controller(account_id, ["submit", "--send-delay", "0"])
        return {"controller": {"open": opened, "image": pasted, "submit": submitted}, "confirmed": False, "note": "X11 image submit completed; Core has not observed a WeChat echo message."}

    def process_pending(self, *, limit: int = 20) -> dict[str, int]:
        try:
            lease_seconds = max(30.0, float(os.environ.get("WECHAT_SENDING_LEASE_SECONDS", "120")))
        except ValueError:
            lease_seconds = 120.0
        self.store.recover_stale_sends(max_age_seconds=lease_seconds)
        result = {"processed": 0, "sent": 0, "failed": 0, "deferred": 0}
        for row in self.store.pending_sends(limit=limit):
            account_id = str(row["account_id"])
            account = self.registry.get(account_id)
            if account is None or not account.sender_enabled:
                result["deferred"] += 1
                continue
            result["processed"] += 1
            self.store.transition_send(row["send_id"], "sending", details={"runtime": {"display": account.display}})
            request = parse_json(row["request_json"], {})
            try:
                if row["kind"] == "text":
                    details = self._send_text(account_id, str(row["chat_id"]), request)
                elif row["kind"] == "image":
                    details = self._send_image(account_id, str(row["chat_id"]), request)
                else:
                    raise RuntimeError("upstream X11 controller has no verified file-paste primitive; request was not sent")
                self.store.transition_send(row["send_id"], "sent", details=details)
                result["sent"] += 1
            except Exception as exc:
                self.store.transition_send(row["send_id"], "failed", details={"runtime": {"display": account.display}}, error=str(exc))
                result["failed"] += 1
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
