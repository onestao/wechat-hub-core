"""Live handoff helpers for the package-A multi-account WeChat Runtime.

The Runtime persists stable account metadata in ``/config`` while PID and X11
window identifiers are intentionally ephemeral. Core shares Runtime's PID
namespace/X11 socket in the integrated Compose stack and resolves those live
identifiers when sync, key scanning, or sending needs them.
"""

from __future__ import annotations

import json
import os
import re
import subprocess
from dataclasses import replace
from pathlib import Path
from typing import Any

from .registry import AccountConfig


_WINDOW_RE = re.compile(r"0x[0-9a-fA-F]+")
_PID_RE = re.compile(r"_NET_WM_PID\([^)]*\)\s*=\s*(\d+)")


def _proc_text(proc_root: Path, pid: int, name: str) -> str:
    try:
        data = (proc_root / str(pid) / name).read_bytes()
    except (FileNotFoundError, PermissionError, OSError):
        return ""
    if name == "cmdline":
        return data.replace(b"\x00", b" ").decode("utf-8", errors="replace").strip()
    return data.decode("utf-8", errors="replace").strip()


def is_wechat_process(proc_root: Path, pid: int) -> bool:
    """Mirror Runtime's conservative WeChat process predicate."""
    cmdline = _proc_text(proc_root, pid, "cmdline")
    comm = _proc_text(proc_root, pid, "comm").lower()
    lower = cmdline.lower()
    if "/scripts/wechat/" in lower or "wechat_runtime.py" in lower:
        return False
    if "/usr/bin/wechat" in lower or "/opt/wechat/" in lower or "/usr/lib/wechat" in lower:
        return True
    return comm.startswith("wechat") or comm.startswith("weixin")


def account_processes(uid: int, *, proc_root: Path = Path("/proc")) -> list[int]:
    """Find WeChat PIDs owned by one Runtime account UID."""
    if not proc_root.exists():
        return []
    result: list[int] = []
    for entry in proc_root.iterdir():
        if not entry.name.isdigit():
            continue
        pid = int(entry.name)
        try:
            owner = entry.stat().st_uid
        except (FileNotFoundError, PermissionError, OSError):
            continue
        if owner != uid:
            continue
        if is_wechat_process(proc_root, pid):
            result.append(pid)
    return sorted(result)


def _xprop(args: list[str], *, display: str, xauthority: str = "") -> str:
    env = {**os.environ, "DISPLAY": display}
    if xauthority and Path(xauthority).is_file():
        env["XAUTHORITY"] = xauthority
    elif "XAUTHORITY" in env and not Path(env["XAUTHORITY"]).is_file():
        env.pop("XAUTHORITY", None)
    try:
        result = subprocess.run(
            ["xprop", *args],
            capture_output=True,
            text=True,
            timeout=5,
            env=env,
            check=False,
        )
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return ""
    return result.stdout if result.returncode == 0 else ""


def _proc_uid(pid: int, *, proc_root: Path = Path("/proc")) -> int | None:
    try:
        return int((proc_root / str(pid)).stat().st_uid)
    except (FileNotFoundError, PermissionError, OSError):
        return None


def account_window(
    uid: int,
    display: str,
    *,
    legacy: bool = False,
    xauthority: str = "",
    proc_root: Path = Path("/proc"),
) -> str:
    """Resolve one account-owned visible WeChat window from the shared X server."""
    root_props = _xprop(["-root", "_NET_CLIENT_LIST"], display=display, xauthority=xauthority)
    candidates: list[tuple[str, str]] = []
    for window_id in _WINDOW_RE.findall(root_props):
        props = _xprop(
            ["-id", window_id, "_NET_WM_PID", "WM_CLASS", "_NET_WM_NAME", "WM_NAME"],
            display=display,
            xauthority=xauthority,
        )
        pid_match = _PID_RE.search(props)
        if pid_match is None:
            continue
        pid = int(pid_match.group(1))
        if _proc_uid(pid, proc_root=proc_root) != uid:
            continue
        lower = props.lower()
        process_match = is_wechat_process(proc_root, pid)
        if legacy and not process_match:
            continue
        if not process_match and not any(marker in lower for marker in ("wechat", "weixin", "微信")):
            continue
        preferred = "weixin" in lower or "微信" in props
        candidates.append(("0" if preferred else "1", str(int(window_id, 16))))
    if not candidates:
        return ""
    candidates.sort()
    return candidates[0][1]


def discover_source_db(home: Path) -> tuple[Path, Path] | None:
    """Locate the account-local Linux WeChat db_storage beneath Runtime HOME."""
    candidates: list[Path] = []
    for base in (home / "Documents" / "xwechat_files", home / "xwechat_files"):
        if base.exists():
            candidates.extend(path for path in base.glob("*/db_storage") if path.is_dir())
    legacy = home / ".local" / "share" / "weixin" / "data" / "db_storage"
    if legacy.is_dir():
        candidates.append(legacy)
    if not candidates:
        return None

    def modified(path: Path) -> float:
        try:
            return path.stat().st_mtime
        except OSError:
            return 0.0

    source_db = max(candidates, key=modified)
    return source_db, source_db.parent


def _agent_runtime_status(account: AccountConfig) -> dict[str, Any]:
    status_file = str(account.runtime.get("runtime_status_file") or "").strip()
    if not status_file:
        return {}
    try:
        value = json.loads(Path(status_file).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def resolve_runtime_account(account: AccountConfig) -> AccountConfig:
    """Refresh PID/window/source paths for an account derived from package A."""
    if not account.runtime.get("runtime_bridge"):
        return account
    runtime: dict[str, Any] = dict(account.runtime)

    if account.runtime_provider == "agent_wechat":
        status = _agent_runtime_status(account)
        container_id = str(status.get("container_id") or "").strip()
        runtime["container_id"] = container_id
        # AgentWechat containers intentionally keep isolated PID namespaces so
        # each upstream instance can only discover its own WeChat process.
        # Core imports stored DB credentials from that account's agent.db
        # instead of ptracing the child process.
        runtime["pids"] = []
        runtime["running"] = bool(status.get("running"))
        for key in (
            "container_running",
            "agent_server_healthy",
            "runtime_health",
            "health_error",
            "wechat_login_status",
            "logged_in_user",
        ):
            if key in status:
                runtime[key] = status[key]
        source_db_dir = account.source_db_dir
        wechat_base_dir = account.wechat_base_dir
        source_home = str(runtime.get("source_home") or "").strip()
        if source_home:
            discovered = discover_source_db(Path(source_home))
            if discovered:
                source_db_dir, wechat_base_dir = discovered
        return replace(
            account,
            source_db_dir=source_db_dir,
            wechat_base_dir=wechat_base_dir,
            runtime=runtime,
        )

    uid_raw = runtime.get("uid")
    try:
        uid = int(uid_raw)
    except (TypeError, ValueError):
        uid = -1
    pids = account_processes(uid) if uid >= 0 else []
    runtime["pids"] = pids
    runtime["running"] = bool(pids)

    window_id = ""
    if uid >= 0:
        window_id = account_window(
            uid,
            account.display,
            legacy=bool(runtime.get("legacy", False)),
            xauthority=str(runtime.get("xauthority") or ""),
        )
    if window_id:
        runtime["window_id"] = window_id
    else:
        runtime.pop("window_id", None)

    source_db_dir = account.source_db_dir
    wechat_base_dir = account.wechat_base_dir
    source_home = str(runtime.get("source_home") or "").strip()
    if source_home:
        discovered = discover_source_db(Path(source_home))
        if discovered:
            source_db_dir, wechat_base_dir = discovered

    return replace(
        account,
        source_db_dir=source_db_dir,
        wechat_base_dir=wechat_base_dir,
        runtime=runtime,
    )
