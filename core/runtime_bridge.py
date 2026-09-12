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

from .registry import AccountConfig, provider_sender_capabilities
from .source_provenance import DATA_DIR_PLACEHOLDERS, SourceIdentityError, valid_wxid


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


def merge_authoritative_login_observation(
    projected_runtime: dict[str, Any],
    status: dict[str, Any] | None,
    *,
    previous_runtime: dict[str, Any] | None = None,
    bound_wxid: str = "",
    binding_state: str = "",
) -> tuple[str, str]:
    """Merge fresh login telemetry without turning a healthy gap into logout.

    Runtime occasionally publishes a healthy ``unknown`` / empty-user sample
    between two verified samples for the same bound WeChat identity.  Such a
    sample is not an authoritative negative observation.  Preserve the last
    verified bound login only while the runtime is still running and no health
    signal explicitly failed.  Explicit logout and any fresh valid identity
    (including a conflicting one) always win.

    ``status is None`` means there is no fresh Runtime observation (for
    example ``AccountConfig.public_runtime()`` on an already-resolved account),
    so the account's existing projected login fields are returned unchanged.
    """
    if status is None:
        return (
            str(projected_runtime.get("wechat_login_status") or "").strip(),
            str(projected_runtime.get("logged_in_user") or "").strip(),
        )

    incoming_status = str(status.get("wechat_login_status") or "").strip()
    incoming_user = str(status.get("logged_in_user") or "").strip()

    if incoming_status == "logged_out":
        return "logged_out", ""

    # A fresh valid identity must never be replaced by remembered state.  This
    # deliberately preserves conflicting wxids so Identity-v2 / RB-003 can
    # fail closed downstream.
    if valid_wxid(incoming_user):
        return incoming_status or "unknown", incoming_user

    running = bool(projected_runtime.get("running", False))
    container_running = bool(projected_runtime.get("container_running", running))
    agent_health = projected_runtime.get("agent_server_healthy")
    runtime_health = str(projected_runtime.get("runtime_health") or "").strip().lower()
    explicitly_unhealthy = agent_health is False or runtime_health in {
        "degraded",
        "failed",
        "unhealthy",
        "error",
        "offline",
        "stopped",
    }

    previous = previous_runtime if isinstance(previous_runtime, dict) else {}
    previous_status = str(previous.get("wechat_login_status") or "").strip()
    previous_user = str(previous.get("logged_in_user") or "").strip()
    # ``logged_in`` without a valid identity is not a complete authoritative
    # positive observation.  Treat it like the other transient/partial status
    # samples so a previously verified bound login is not erased merely
    # because Runtime published the status and identity fields non-atomically.
    observation_is_gap = incoming_status in {
        "",
        "unknown",
        "checking",
        "starting",
        "pending",
        "unavailable",
        "logged_in",
    }

    if (
        observation_is_gap
        and running
        and container_running
        and not explicitly_unhealthy
        and str(binding_state or "").strip().lower() == "bound"
        and valid_wxid(bound_wxid)
        and previous_status == "logged_in"
        and valid_wxid(previous_user)
        and previous_user == bound_wxid
    ):
        return "logged_in", previous_user

    # ``logged_in`` without a valid user is incomplete positive telemetry.  If
    # it cannot be safely merged with a previously verified bound login above,
    # do not let the status word alone manufacture an ``online`` transition
    # after a real logout/degradation.  Surface it as unknown until a valid
    # identity arrives.
    if incoming_status == "logged_in":
        return "unknown", ""

    # Invalid/empty identity telemetry is not persisted as an identity claim.
    return incoming_status, ""


def canonical_runtime_projection(
    account: AccountConfig,
    status: dict[str, Any] | None = None,
    *,
    previous_runtime: dict[str, Any] | None = None,
    bound_wxid: str = "",
    binding_state: str = "",
) -> dict[str, Any]:
    """Produce a deterministic, canonical runtime projection dictionary.

    Ensures both CoreService._apply_runtime_status and AccountWorker.run_account
    persist the exact same shape for identical registry + Runtime inputs.
    """
    runtime = dict(account.runtime)
    previous = previous_runtime if isinstance(previous_runtime, dict) else {}
    runtime.pop("controller_command", None)
    runtime.pop("key_file", None)
    runtime.pop("agent_wechat_token_file", None)
    runtime["registered"] = True
    s = status or {}

    def observed_or_previous(key: str, fallback: Any = None) -> Any:
        """Prefer an explicit fresh value, then the latest persisted runtime.

        Worker projections can be built from an ``AccountConfig`` resolved at
        the start of a comparatively long sync cycle.  If the final Runtime
        status read is partial/missing, falling back to that stale config can
        overwrite a newer explicit stopped/unhealthy observation already
        persisted by the Runtime/API writer.  The latest persisted projection
        is therefore the safe fallback for dynamic telemetry.  ``False`` is a
        meaningful fresh value; only an absent/``None`` observation falls
        through.
        """
        if key in s and s.get(key) is not None:
            return s.get(key)
        if key in previous:
            return previous.get(key)
        return runtime.get(key, fallback)

    provider = str(s.get("runtime_provider") or runtime.get("runtime_provider") or account.runtime_provider or "legacy")
    runtime["runtime_provider"] = provider
    runtime.setdefault("sender_capabilities", provider_sender_capabilities(provider))
    runtime["display"] = "isolated" if provider == "agent_wechat" else (account.display or ":1")
    if account.window_id:
        runtime["window_id"] = account.window_id
    instance_uuid = str(s.get("instance_uuid") or account.instance_uuid or "").strip()
    if instance_uuid:
        runtime["instance_uuid"] = instance_uuid
    runtime_alias = str(s.get("runtime_alias") or account.runtime_alias or account.account_id).strip()
    if runtime_alias:
        runtime["runtime_alias"] = runtime_alias
    resource_key = str(s.get("resource_key") or account.resource_key or account.account_id).strip()
    if resource_key:
        runtime["resource_key"] = resource_key
    runtime["running"] = bool(observed_or_previous("running", False))
    if "container_running" in s and s.get("container_running") is not None:
        runtime["container_running"] = bool(s.get("container_running"))
    elif "running" in s and s.get("running") is not None:
        runtime["container_running"] = bool(s.get("running"))
    elif "container_running" in previous:
        runtime["container_running"] = bool(previous.get("container_running"))
    elif "running" in previous:
        runtime["container_running"] = bool(previous.get("running"))
    else:
        runtime["container_running"] = bool(runtime.get("container_running", runtime["running"]))
    runtime["agent_server_healthy"] = observed_or_previous("agent_server_healthy")
    runtime["runtime_health"] = observed_or_previous("runtime_health")
    if "health_error" in s:
        runtime["health_error"] = str(s.get("health_error") or "")
    elif "health_error" in previous:
        runtime["health_error"] = str(previous.get("health_error") or "")
    else:
        runtime["health_error"] = str(runtime.get("health_error") or "")
    login_status, logged_in_user = merge_authoritative_login_observation(
        runtime,
        status,
        previous_runtime=previous_runtime,
        bound_wxid=bound_wxid,
        binding_state=binding_state,
    )
    runtime["wechat_login_status"] = login_status
    runtime["logged_in_user"] = logged_in_user
    runtime["username"] = str(
        s.get("username")
        or runtime.get("username")
        or f"agent_{account.resource_key or account.account_id}"
    )
    runtime["uid"] = s.get("uid", runtime.get("uid"))
    runtime["home"] = s.get("home") or runtime.get("source_home") or ""
    runtime["autostart"] = bool(s.get("autostart", runtime.get("autostart", True)))
    runtime["container_id"] = str(s.get("container_id") or runtime.get("container_id") or "").strip()
    runtime["container_name"] = str(s.get("container_name") or runtime.get("container_name") or "").strip()
    runtime["image"] = s.get("image", runtime.get("image"))
    runtime["current_image"] = s.get("current_image", runtime.get("current_image"))
    runtime["image_update_pending"] = s.get("image_update_pending", runtime.get("image_update_pending"))
    runtime["capabilities"] = s.get("capabilities", runtime.get("capabilities"))
    runtime["pids"] = list(s.get("pids") or runtime.get("pids") or [])
    runtime["windows"] = list(s.get("windows") or runtime.get("windows") or [])
    runtime["window_error"] = s.get("window_error", runtime.get("window_error"))
    return runtime


def discover_agent_wechat_source_db(
    home: Path,
    logged_in_user: str,
    account_id: str = "",
    bound_wxid: str = "",
) -> tuple[Path, Path] | None:
    """Locate the AgentWechat db_storage for exactly the logged-in WeChat identity.

    RB-003 and Retry2 invariants:
    1. If fresh valid logged_in_user is present:
       - If bound_wxid is also present and logged_in_user != bound_wxid -> FAIL CLOSED.
       - Check for <home>/.../<logged_in_user>/db_storage.
       - If found, select it.
       - If not found but other valid wxid directories exist, FAIL CLOSED with SourceIdentityError.
       - If no directories exist, return None.
    2. If logged_in_user is missing/invalid:
       - If valid bound_wxid is present (account already bound):
         - Check for <home>/.../<bound_wxid>/db_storage.
         - If found, select it (never select historical wxid by mtime).
         - If not found but other directories exist, FAIL CLOSED with SourceIdentityError.
         - If no directories exist, return None.
       - If bound_wxid is also not present:
         - Keep legacy discover_source_db behavior for unconstrained/legacy caller.
    """
    def modified(path: Path) -> float:
        try:
            return path.stat().st_mtime
        except OSError:
            return 0.0

    bases = [home / "Documents" / "xwechat_files", home / "xwechat_files"]
    all_wxid_dirs: dict[str, list[Path]] = {}

    for base in bases:
        if not base.is_dir():
            continue
        try:
            children = list(base.iterdir())
        except OSError:
            continue
        for child in children:
            if not child.is_dir():
                continue
            child_name = child.name
            db_storage = child / "db_storage"
            if (
                db_storage.is_dir()
                and valid_wxid(child_name)
                and child_name not in DATA_DIR_PLACEHOLDERS
            ):
                all_wxid_dirs.setdefault(child_name, []).append(db_storage)

    found_wxids = sorted(all_wxid_dirs.keys())

    # Case 1: Fresh valid logged_in_user observed
    if valid_wxid(logged_in_user):
        if valid_wxid(bound_wxid) and logged_in_user != bound_wxid:
            raise SourceIdentityError(
                "source_identity_mismatch",
                409,
                f"source db identity mismatch for account {account_id or 'unknown'}: "
                f"fresh observed user {logged_in_user!r} conflicts with bound identity {bound_wxid!r}",
                details={
                    "account_id": account_id,
                    "expected_wxid": logged_in_user,
                    "bound_wxid": bound_wxid,
                    "found_wxids": found_wxids,
                    "source_home": str(home),
                },
            )
        if logged_in_user in all_wxid_dirs:
            chosen = max(all_wxid_dirs[logged_in_user], key=modified)
            return chosen, chosen.parent
        if found_wxids:
            raise SourceIdentityError(
                "source_identity_mismatch",
                409,
                f"source db identity mismatch for account {account_id or 'unknown'}: "
                f"expected {logged_in_user} beneath {home}, but found other wxid directories: {found_wxids}",
                details={
                    "account_id": account_id,
                    "expected_wxid": logged_in_user,
                    "found_wxids": found_wxids,
                    "source_home": str(home),
                },
            )
        return None

    # Case 2: Transient missing/invalid logged_in_user, but account is bound
    if valid_wxid(bound_wxid):
        if bound_wxid in all_wxid_dirs:
            chosen = max(all_wxid_dirs[bound_wxid], key=modified)
            return chosen, chosen.parent
        if found_wxids:
            raise SourceIdentityError(
                "source_identity_mismatch",
                409,
                f"source db identity mismatch for account {account_id or 'unknown'}: "
                f"bound identity {bound_wxid!r} not found beneath {home}; found other wxid directories: {found_wxids}",
                details={
                    "account_id": account_id,
                    "bound_wxid": bound_wxid,
                    "found_wxids": found_wxids,
                    "source_home": str(home),
                },
            )
        return None

    # Case 3: Neither logged_in_user nor bound_wxid is valid -> legacy fallback
    return discover_source_db(home)


def _agent_runtime_status(account: AccountConfig) -> dict[str, Any]:
    status_file = str(account.runtime.get("runtime_status_file") or "").strip()
    if not status_file:
        return {}
    try:
        value = json.loads(Path(status_file).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return value if isinstance(value, dict) else {}


def resolve_runtime_account(
    account: AccountConfig,
    bound_wxid: str = "",
    *,
    previous_runtime: dict[str, Any] | None = None,
    binding_state: str = "",
) -> AccountConfig:
    """Refresh PID/window/source paths for an account derived from package A."""
    if not account.runtime.get("runtime_bridge"):
        return account
    runtime: dict[str, Any] = dict(account.runtime)

    if account.runtime_provider == "agent_wechat":
        status = _agent_runtime_status(account)
        effective_bound = bound_wxid or str(account.runtime.get("bound_wxid") or "").strip()
        runtime = canonical_runtime_projection(
            account,
            status,
            previous_runtime=previous_runtime,
            bound_wxid=effective_bound,
            binding_state=binding_state,
        )
        if effective_bound:
            runtime["bound_wxid"] = effective_bound

        source_db_dir = account.source_db_dir
        wechat_base_dir = account.wechat_base_dir
        source_home = str(runtime.get("source_home") or "").strip()
        logged_in_user = str(runtime.get("logged_in_user") or "").strip()
        if source_home:
            discovered = discover_agent_wechat_source_db(
                Path(source_home),
                logged_in_user,
                account_id=account.account_id,
                bound_wxid=effective_bound,
            )
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
