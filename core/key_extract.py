#!/usr/bin/env python3
"""Account-scoped entrypoint for the upstream Linux WeChat key scanner."""

from __future__ import annotations

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path
from typing import Any

from .registry import AccountConfig, RegistryError, load_registry
from .runtime_bridge import resolve_runtime_account
from .runtime_control import RuntimeControlClient, RuntimeControlError


HEX_KEY_RE = re.compile(r"^[0-9a-fA-F]{64}$")
IMAGE_AES_RE = re.compile(r"^[0-9a-fA-F]{32}$")
IMAGE_XOR_RE = re.compile(r"^[0-9a-fA-F]{2}$")


def _source_account_dir(source_db_dir: Path) -> str:
    if source_db_dir.name != "db_storage":
        return ""
    parent = source_db_dir.parent.name
    return parent if parent not in {"data", "xwechat_files", "Documents"} else ""


def import_agent_wechat_keys(
    account: AccountConfig,
    *,
    credentials: list[dict[str, Any]] | None = None,
) -> dict[str, object]:
    """Convert upstream credentials into Core's existing key JSON.

    This keeps each agent-wechat instance in its own PID namespace.  We reuse
    credentials already extracted and verified by upstream. Runtime owns the
    upstream state DB and exports only account-scoped records over the private
    Unix control socket, so upstream storage details do not leak into Core.
    """

    if not account.source_db_dir.is_dir():
        return {
            "account_id": account.account_id,
            "returncode": 2,
            "keys_file": str(account.keys_file),
            "source_db_dir": str(account.source_db_dir),
            "source": "agent_wechat_runtime_driver",
            "error": "WeChat db_storage is unavailable",
        }

    if credentials is None:
        socket_path = os.environ.get("WECHAT_RUNTIME_CONTROL_SOCKET", "/run/wechat-runtime/control.sock")
        try:
            exported = RuntimeControlClient(socket_path, timeout=10.0).request(
                "db_keys", account_id=account.account_id
            )
        except RuntimeControlError as exc:
            return {
                "account_id": account.account_id,
                "returncode": 2,
                "keys_file": str(account.keys_file),
                "source_db_dir": str(account.source_db_dir),
                "source": "agent_wechat_runtime_driver",
                "error": f"Runtime key provider failed: {exc.message}",
            }
        raw_credentials = exported.get("credentials")
        if not isinstance(raw_credentials, list):
            return {
                "account_id": account.account_id,
                "returncode": 2,
                "keys_file": str(account.keys_file),
                "source_db_dir": str(account.source_db_dir),
                "source": "agent_wechat_runtime_driver",
                "error": "Runtime key provider returned invalid credentials",
            }
        credentials = [item for item in raw_credentials if isinstance(item, dict)]

    rows = sorted(
        credentials,
        key=lambda item: str(item.get("verified_at") or ""),
        reverse=True,
    )

    account_dir = _source_account_dir(account.source_db_dir)
    available_accounts = {
        str(row.get("account_dir") or "").strip()
        for row in rows
        if str(row.get("account_dir") or "").strip()
    }
    if account_dir and account_dir in available_accounts:
        selected_account = account_dir
    elif len(available_accounts) == 1:
        selected_account = next(iter(available_accounts))
    else:
        return {
            "account_id": account.account_id,
            "returncode": 2,
            "keys_file": str(account.keys_file),
            "source_db_dir": str(account.source_db_dir),
            "source": "agent_wechat_runtime_driver",
            "error": (
                f"cannot safely select agent-wechat account_dir {account_dir!r}; "
                f"stored account dirs: {sorted(available_accounts)}"
            ),
        }

    # Rows are newest-first.  A re-login can refresh a credential, so retain
    # the newest verified value for each database basename.
    by_name: dict[str, str] = {}
    # Upstream stores the media (.dat) key in the same table under an
    # underscore-prefixed pseudo db_name.  It is not a SQLCipher credential,
    # so it never enters ``by_name``; it only feeds Core's media config.
    media_meta: dict[str, str] = {}
    for row in rows:
        if str(row.get("account_dir") or "") != selected_account:
            continue
        name = str(row.get("db_name") or "").strip()
        key = str(row.get("hex_key") or "").strip().lower()
        if name.startswith("_"):
            if not bool(str(row.get("verified_at") or "").strip()):
                continue
            if name == "_image_aes" and IMAGE_AES_RE.fullmatch(key):
                media_meta.setdefault(name, key)
            elif name == "_image_xor" and IMAGE_XOR_RE.fullmatch(key):
                media_meta.setdefault(name, key)
            continue
        if not name or not HEX_KEY_RE.fullmatch(key):
            continue
        by_name.setdefault(name, key)

    converted: dict[str, object] = {}
    missing: list[str] = []
    for db_path in sorted(account.source_db_dir.rglob("*.db")):
        rel = db_path.relative_to(account.source_db_dir).as_posix()
        key = by_name.get(db_path.name)
        if not key:
            missing.append(rel)
            continue
        try:
            with db_path.open("rb") as handle:
                salt = handle.read(16).hex()
            size_mb = round(db_path.stat().st_size / 1024 / 1024, 1)
        except OSError:
            missing.append(rel)
            continue
        converted[rel] = {"enc_key": key, "salt": salt, "size_mb": size_mb}

    if not converted:
        return {
            "account_id": account.account_id,
            "returncode": 1,
            "keys_file": str(account.keys_file),
            "source_db_dir": str(account.source_db_dir),
            "source": "agent_wechat_runtime_driver",
            "account_dir": selected_account,
            "error": "agent-wechat has not stored any usable DB credentials yet",
        }

    converted["_db_dir"] = str(account.source_db_dir)
    account.keys_file.parent.mkdir(parents=True, exist_ok=True)
    temp = account.keys_file.with_suffix(account.keys_file.suffix + ".tmp")
    temp.write_text(json.dumps(converted, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temp, account.keys_file)
    try:
        account.keys_file.chmod(0o600)
    except OSError:
        pass
    media_config_written = _write_media_config(account, media_meta)
    return {
        "account_id": account.account_id,
        "returncode": 0,
        "keys_file": str(account.keys_file),
        "source_db_dir": str(account.source_db_dir),
        "source": "agent_wechat_runtime_driver",
        "account_dir": selected_account,
        "imported": len(converted) - 1,
        "missing": missing,
        "media_key": media_config_written,
        "pids": [],
    }


def _write_media_config(account: AccountConfig, media_meta: dict[str, str]) -> dict[str, Any]:
    """Persist the upstream media (.dat) key into Core's per-account config.

    ``memory/media_sync.py`` reads ``image_aes_key`` from ``account.config_file``
    and applies it as ``key.encode("ascii")[:16]``, which matches upstream
    ``wechat_media.rs`` taking the first 16 ASCII characters of the 32-char
    hex credential.  Absent credentials never overwrite an existing config.
    """

    aes_key = media_meta.get("_image_aes")
    if not aes_key:
        return {"present": False, "written": False, "path": str(account.config_file)}
    existing: dict[str, Any] = {}
    if account.config_file.is_file():
        try:
            loaded = json.loads(account.config_file.read_text(encoding="utf-8"))
            if isinstance(loaded, dict):
                existing = loaded
        except (OSError, json.JSONDecodeError):
            existing = {}
    updated = dict(existing)
    updated["image_aes_key"] = aes_key
    xor_key = media_meta.get("_image_xor")
    if xor_key:
        updated["image_xor_key"] = "0x" + xor_key
    if updated == existing:
        return {"present": True, "written": False, "path": str(account.config_file)}
    account.config_file.parent.mkdir(parents=True, exist_ok=True)
    temp = account.config_file.with_suffix(account.config_file.suffix + ".tmp")
    temp.write_text(json.dumps(updated, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    os.replace(temp, account.config_file)
    try:
        account.config_file.chmod(0o600)
    except OSError:
        pass
    return {"present": True, "written": True, "path": str(account.config_file)}


def scanner_command(account: AccountConfig, *, root: Path, scanner: Path | None = None) -> list[str]:
    account = resolve_runtime_account(account)
    raw_pids = account.runtime.get("pids")
    if raw_pids is None:
        raw_pids = [account.runtime.get("pid")]
    if not isinstance(raw_pids, list):
        raise RegistryError(f"account {account.account_id} runtime.pids must be an array")
    pids: list[int] = []
    for raw_pid in raw_pids:
        if raw_pid in (None, ""):
            continue
        try:
            pid = int(raw_pid)
        except (TypeError, ValueError) as exc:
            raise RegistryError(f"account {account.account_id} has an invalid runtime pid: {raw_pid!r}") from exc
        if pid <= 0:
            raise RegistryError(f"account {account.account_id} has an invalid runtime pid: {pid}")
        if pid not in pids:
            pids.append(pid)
    if not pids:
        raise RegistryError(
            f"account {account.account_id} requires runtime.pid or runtime.pids before key extraction"
        )
    scanner_path = scanner or Path(os.environ.get("WECHAT_KEY_SCANNER") or root / "tools" / "wechat-decrypt" / "find_all_keys_linux.py")
    command = [
        sys.executable,
        str(scanner_path),
        "--db-dir",
        str(account.source_db_dir),
        "--out-file",
        str(account.keys_file),
    ]
    for pid in pids:
        command.extend(["--pid", str(pid)])
    return command


def extract_account_keys(
    account: AccountConfig,
    *,
    root: Path,
    scanner: Path | None = None,
) -> dict[str, object]:
    """Run the reused scanner for one account after refreshing Runtime PIDs."""
    account = resolve_runtime_account(account)
    if account.runtime_provider == "agent_wechat":
        return import_agent_wechat_keys(account)
    command = scanner_command(account, root=root, scanner=scanner)
    account.keys_file.parent.mkdir(parents=True, exist_ok=True)
    result = subprocess.run(command, cwd=root, text=True)
    if result.returncode == 0:
        try:
            account.keys_file.chmod(0o600)
        except OSError:
            pass
    return {
        "account_id": account.account_id,
        "returncode": result.returncode,
        "keys_file": str(account.keys_file),
        "source_db_dir": str(account.source_db_dir),
        "pids": list(account.runtime.get("pids") or []),
    }


def main(argv: list[str] | None = None) -> int:
    root = Path(__file__).resolve().parents[1]
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=root)
    parser.add_argument("--registry", type=Path, default=Path("runtime/core/accounts.json"))
    parser.add_argument("--scanner", type=Path, help="path to the upstream find_all_keys_linux.py in the same PID namespace as WeChat")
    parser.add_argument("--account", action="append", required=True, help="registered account_id; may be repeated")
    args = parser.parse_args(argv)
    workspace_root = args.root.resolve()
    scanner = args.scanner
    if scanner and not scanner.is_absolute():
        scanner = workspace_root / scanner
    try:
        registry = load_registry(args.registry, root=workspace_root)
        accounts = [registry.require(account_id) for account_id in args.account]
    except RegistryError as exc:
        parser.error(str(exc))
    exit_code = 0
    for account in accounts:
        result = extract_account_keys(account, root=workspace_root, scanner=scanner)
        returncode = int(result["returncode"])
        if returncode != 0:
            exit_code = returncode or 1
        print(json.dumps(result, ensure_ascii=False), flush=True)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
