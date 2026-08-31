#!/usr/bin/env python3
"""Account-scoped entrypoint for the upstream Linux WeChat key scanner."""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

from .registry import AccountConfig, RegistryError, load_registry
from .runtime_bridge import resolve_runtime_account


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
