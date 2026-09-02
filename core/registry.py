"""Validated account registry and account-scoped path derivation for Core."""

from __future__ import annotations

import json
import os
import re
import threading
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any


ACCOUNT_ID_RE = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}\Z")
LEGACY_PLACEHOLDER = "PLEASE_SET_WECHAT_ACCOUNT_DIR"


def provider_sender_capabilities(provider: str) -> dict[str, Any]:
    """Return conservative public sender capabilities for one runtime provider."""

    if provider == "agent_wechat":
        return {
            "text": True,
            "image": True,
            "file": True,
            "native_reply": False,
            "media_caption": False,
            "max_mentions": 0,
            "echo_confirmation": False,
            "verified_chat_target": True,
            "driver": "agent_wechat",
        }
    return {
        "text": False,
        "image": False,
        "file": False,
        "native_reply": False,
        "media_caption": False,
        "max_mentions": 0,
        "echo_confirmation": False,
        "verified_chat_target": False,
        "driver": "legacy",
    }


class RegistryError(ValueError):
    """A registry entry cannot safely be used by the Core worker."""


def _path(value: str | Path | None, *, root: Path, default: Path) -> Path:
    if not value:
        return default
    candidate = Path(value)
    return candidate if candidate.is_absolute() else root / candidate


def _text(value: object, field_name: str, *, required: bool = False) -> str:
    text = str(value or "").strip()
    if required and not text:
        raise RegistryError(f"{field_name} must be a non-empty string")
    return text


@dataclass(frozen=True)
class AccountConfig:
    account_id: str
    display_name: str
    source_db_dir: Path
    wechat_base_dir: Path
    keys_file: Path
    runtime_dir: Path
    decrypted_dir: Path
    decrypt_state_file: Path
    memory_db: Path
    media_dir: Path
    sync_status_file: Path
    config_file: Path
    runtime: dict[str, Any] = field(default_factory=dict)

    @property
    def display(self) -> str:
        return str(self.runtime.get("display") or ":1")

    @property
    def window_id(self) -> str:
        return str(self.runtime.get("window_id") or "").strip()

    @property
    def sender_enabled(self) -> bool:
        return bool(self.runtime.get("sender_enabled", False))

    @property
    def runtime_provider(self) -> str:
        return str(self.runtime.get("runtime_provider") or "legacy")

    def public_runtime(self) -> dict[str, Any]:
        runtime = dict(self.runtime)
        runtime.pop("controller_command", None)
        runtime.pop("key_file", None)
        runtime.pop("agent_wechat_token_file", None)
        runtime.setdefault("sender_capabilities", provider_sender_capabilities(self.runtime_provider))
        runtime["display"] = self.display
        if self.window_id:
            runtime["window_id"] = self.window_id
        return runtime


class AccountRegistry:
    def __init__(self, accounts: list[AccountConfig], source_path: Path) -> None:
        self._accounts = {account.account_id: account for account in accounts}
        self.source_path = source_path
        self._lock = threading.RLock()

    def all(self) -> list[AccountConfig]:
        with self._lock:
            return list(self._accounts.values())

    def get(self, account_id: str) -> AccountConfig | None:
        with self._lock:
            return self._accounts.get(account_id)

    def require(self, account_id: str) -> AccountConfig:
        account = self.get(account_id)
        if account is None:
            raise RegistryError(f"Unknown account_id: {account_id}")
        return account

    def replace_from(self, other: "AccountRegistry") -> None:
        """Atomically replace the live account snapshot while keeping callers' reference stable."""
        with self._lock:
            self._accounts = {account.account_id: account for account in other.all()}
            self.source_path = other.source_path


def parse_account(item: object, *, root: Path) -> AccountConfig:
    if not isinstance(item, dict):
        raise RegistryError("each accounts entry must be an object")
    account_id = _text(item.get("account_id"), "account_id", required=True)
    if not ACCOUNT_ID_RE.fullmatch(account_id):
        raise RegistryError("account_id may contain only letters, digits, '_', '.' and '-'")
    display_name = _text(item.get("display_name"), "display_name") or account_id
    runtime = item.get("runtime") or {}
    if not isinstance(runtime, dict):
        raise RegistryError(f"runtime for {account_id} must be an object")
    account_root = _path(item.get("runtime_dir"), root=root, default=root / "runtime" / "accounts" / account_id)
    source_db_dir = _path(
        item.get("source_db_dir"), root=root, default=account_root / "unconfigured-source-db"
    )
    wechat_base_dir = _path(item.get("wechat_base_dir"), root=root, default=source_db_dir.parent)
    return AccountConfig(
        account_id=account_id,
        display_name=display_name,
        source_db_dir=source_db_dir,
        wechat_base_dir=wechat_base_dir,
        keys_file=_path(item.get("keys_file"), root=root, default=account_root / "wechat-decrypt" / "keys" / "all_keys.json"),
        runtime_dir=account_root,
        decrypted_dir=_path(item.get("decrypted_dir"), root=root, default=account_root / "wechat-decrypt" / "decrypted"),
        decrypt_state_file=_path(item.get("decrypt_state_file"), root=root, default=account_root / "wechat-decrypt" / "sync_state.json"),
        memory_db=_path(item.get("memory_db"), root=root, default=account_root / "memory" / "wechat_memory.sqlite"),
        media_dir=_path(item.get("media_dir"), root=root, default=account_root / "media"),
        sync_status_file=_path(item.get("sync_status_file"), root=root, default=account_root / "memory" / "sync_status.json"),
        config_file=_path(item.get("config_file"), root=root, default=account_root / "wechat-decrypt" / "config.json"),
        runtime=dict(runtime),
    )


def _runtime_config_root(registry_path: Path) -> Path:
    configured = os.environ.get("WECHAT_RUNTIME_CONFIG_ROOT", "").strip()
    if configured:
        return Path(configured)
    return registry_path.parent.parent


def _translate_runtime_home(home: str, *, config_root: Path) -> Path:
    if home == "/config":
        return config_root
    prefix = "/config/"
    if home.startswith(prefix):
        return config_root / home[len(prefix):]
    return Path(home)


def _display_lock_path(display: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_.-]", "_", display or ":1") or "display"
    return f"/run/wechat-runtime/locks/display-{safe}.lock"


def parse_runtime_account(item: object, *, root: Path, registry_path: Path) -> AccountConfig:
    """Translate package A's stable registry record into a Core account."""
    if not isinstance(item, dict):
        raise RegistryError("each Runtime accounts entry must be an object")
    account_id = _text(item.get("id"), "id", required=True)
    if not ACCOUNT_ID_RE.fullmatch(account_id):
        raise RegistryError("Runtime account id may contain only letters, digits, '_', '.' and '-'")
    display = _text(item.get("display"), "display") or ":1"
    provider = _text(item.get("runtime_provider"), "runtime_provider") or "legacy"
    if provider not in {"legacy", "agent_wechat"}:
        raise RegistryError(f"unsupported Runtime provider for {account_id}: {provider}")
    config_root = _runtime_config_root(registry_path)
    home = _translate_runtime_home(_text(item.get("home"), "home", required=True), config_root=config_root)
    unresolved_base = home / "Documents" / "xwechat_files" / "__runtime_unresolved__"
    runtime: dict[str, Any] = {
        "runtime_bridge": "agent-wechat-v1" if provider == "agent_wechat" else "wechat-selkies-v1",
        "runtime_provider": provider,
        "display": "isolated" if provider == "agent_wechat" else display,
        "uid": item.get("uid"),
        "legacy": bool(item.get("legacy", False)),
        "enabled": bool(item.get("enabled", True)),
        "sender_enabled": bool(item.get("enabled", True)),
        "source_home": str(home),
    }
    if provider == "agent_wechat":
        agent_wechat = item.get("agent_wechat") if isinstance(item.get("agent_wechat"), dict) else {}
        container_name = _text(agent_wechat.get("container_name"), "agent_wechat.container_name", required=True)
        token_file = _text(agent_wechat.get("token_file"), "agent_wechat.token_file", required=True)
        runtime.update(
            {
                "sender_driver": "agent_wechat",
                "agent_wechat_container": container_name,
                "agent_wechat_base_url": f"http://{container_name}:6174",
                "agent_wechat_token_file": str(_translate_runtime_home(token_file, config_root=config_root)),
                "agent_wechat_image": _text(agent_wechat.get("image"), "agent_wechat.image"),
                "runtime_status_file": f"/run/wechat-runtime/accounts/{account_id}/agent-status.json",
                "native_driver_enabled": False,
            }
        )
    else:
        runtime.update(
            {
                "sender_driver": "legacy",
                "display_lock": _display_lock_path(display),
                "xauthority": str(config_root / ".Xauthority"),
                "native_driver_enabled": False,
            }
        )
    return parse_account(
        {
            "account_id": account_id,
            "display_name": _text(item.get("display_name"), "display_name") or account_id,
            "source_db_dir": str(unresolved_base / "db_storage"),
            "wechat_base_dir": str(unresolved_base),
            "runtime_dir": str(root / "runtime" / "accounts" / account_id),
            "runtime": runtime,
        },
        root=root,
    )


def load_registry(path: Path, *, root: Path) -> AccountRegistry:
    """Load an account registry. A missing file means no accounts, not an error."""
    path = path if path.is_absolute() else root / path
    if not path.exists():
        return AccountRegistry([], path)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise RegistryError(f"Invalid account registry JSON: {path}") from exc
    entries = payload.get("accounts") if isinstance(payload, dict) else None
    if not isinstance(entries, list):
        raise RegistryError("registry must be an object containing an accounts array")
    runtime_shape = bool(entries) and all(
        isinstance(item, dict) and "id" in item and "account_id" not in item
        for item in entries
    )
    if runtime_shape:
        accounts = [parse_runtime_account(item, root=root, registry_path=path) for item in entries]
    else:
        accounts = [parse_account(item, root=root) for item in entries]
    ids = [account.account_id for account in accounts]
    if len(ids) != len(set(ids)):
        raise RegistryError("account_id values must be unique")
    return AccountRegistry(accounts, path)


def legacy_registry(*, root: Path) -> AccountRegistry:
    """Expose the upstream single-account environment as an opt-in bootstrap."""
    account_dir = os.environ.get("WECHAT_ACCOUNT_DIR_NAME", "").strip()
    if not account_dir or account_dir == LEGACY_PLACEHOLDER:
        return AccountRegistry([], root / "runtime" / "core" / "accounts.json")
    item = {
        "account_id": os.environ.get("WECHAT_LEGACY_ACCOUNT_ID", "legacy"),
        "display_name": os.environ.get("WECHAT_LEGACY_ACCOUNT_NAME", account_dir),
        "source_db_dir": os.environ.get("WECHAT_SOURCE_DB_DIR", f"config/xwechat_files/{account_dir}/db_storage"),
        "wechat_base_dir": os.environ.get("WECHAT_BASE_DIR", f"config/xwechat_files/{account_dir}"),
        "runtime_dir": os.environ.get("WECHAT_LEGACY_RUNTIME_DIR", "runtime/accounts/legacy"),
        "runtime": {"display": os.environ.get("WECHAT_DISPLAY", ":1")},
    }
    return AccountRegistry([parse_account(item, root=root)], root / "runtime" / "core" / "accounts.json")
