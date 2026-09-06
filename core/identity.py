"""Identity v2 binding state machine for the Core data layer.

Implements the frozen contract in ``docs/IDENTITY_V2_CONTRACT.md`` (§3-§5):

* ``runtime_instances`` own runtime slots, ``wechat_identities`` own business
  data, and ``instance_identity_bindings`` records which identity a slot is
  currently proxying.  One instance has at most one active binding.
* The Sync Gate refuses business-data writes while a slot is ``mismatch`` or
  ``unresolved``; the Send Gate refuses dispatch with HTTP-409-shaped errors
  and never lets a message leave with the wrong identity intent.
* Legacy ``account_id`` rows keep working: while an instance has never engaged
  the identity system (no binding, no observation, no backfill record) writes
  fall through unstamped, and any stamped row keeps its original attribution
  on later re-imports so history can never silently change owner.

All functions operate on an open ``sqlite3.Connection`` (``row_factory`` may be
``sqlite3.Row``); connection and transaction lifecycle stay owned by
``core.store.CoreStore``.
"""

from __future__ import annotations

import re
import sqlite3
import uuid
from datetime import datetime, timezone
from typing import Any, Iterable


WXID_RE = re.compile(r"[A-Za-z][A-Za-z0-9_-]{4,79}\Z")
INSTANCE_UUID_RE = re.compile(
    r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$"
)
UNRESOLVED_NICKNAME = "未确认历史微信"
DATA_DIR_PLACEHOLDERS = {
    "__runtime_unresolved__",
    "unconfigured-source-db",
    "db_storage",
    "xwechat_files",
}

VERIFIED_SOURCE_RUNTIME_STATUS = "runtime_status"
VERIFIED_SOURCE_AGENT_AUTH = "agent_wechat_auth_status"
VERIFIED_SOURCE_MANUAL_SWITCH = "manual_confirmed_switch"
VERIFIED_SOURCE_LEGACY_BACKFILL = "legacy_backfill"

STATE_UNBOUND = "unbound"
STATE_BOUND = "bound"
STATE_MISMATCH = "mismatch"
STATE_UNRESOLVED = "unresolved"

BACKFILL_TABLES = ("chats", "contacts", "chat_members", "messages", "media", "events", "outbox")

# Tables that constitute real business history for the unresolved-backfill
# decision.  ``events`` is observability and is deliberately excluded.
HISTORY_TABLES = ("chats", "contacts", "chat_members", "messages", "media", "outbox")

MISMATCH_ACTION_REQUIRED = "请在管理面板确认换号操作或重新登录原账号"


class IdentityError(RuntimeError):
    """Fail-closed identity gate error carrying an HTTP status and code."""

    def __init__(self, code: str, status: int, message: str, *, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.status = status
        self.message = message
        self.details = details or {}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def new_uuid() -> str:
    return str(uuid.uuid4())


def valid_wxid(value: object) -> bool:
    text = str(value or "").strip()
    return bool(text) and bool(WXID_RE.fullmatch(text))


def wechat_data_dir_name(path_value: object) -> str:
    """Return the WeChat account directory name encoded in an xwechat_files path.

    WeChat itself names ``xwechat_files/<account>/`` after the logged-in
    account id, so this directory name is code-verifiable credential metadata.
    Returns "" for placeholder/unresolved paths or anything that does not sit
    beneath an ``xwechat_files`` directory.
    """
    text = str(path_value or "").strip().replace("\\", "/")
    if not text:
        return ""
    parts = [part for part in text.split("/") if part]
    for index, part in enumerate(parts):
        if part == "xwechat_files":
            if index + 1 >= len(parts):
                return ""
            candidate = parts[index + 1]
            if not candidate or candidate in DATA_DIR_PLACEHOLDERS:
                return ""
            return candidate
    return ""


def collect_legacy_evidence(
    account_id: str,
    *,
    runtime: dict[str, Any] | None = None,
    sync: dict[str, Any] | None = None,
    data_dir_names: Iterable[str] = (),
    identity_map: dict[str, Any] | None = None,
) -> list[tuple[str, str]]:
    """Assemble proven evidence candidates for a legacy account, best first.

    Display names, nicknames and aliases are deliberately never evidence.
    """
    runtime = runtime or {}
    identity_map = identity_map or {}
    evidence: list[tuple[str, str]] = []
    operator = str(identity_map.get(account_id) or "").strip()
    if operator and valid_wxid(operator):
        evidence.append(("operator_credential_map", operator))
    logged_in = str(runtime.get("logged_in_user") or "").strip()
    if logged_in and valid_wxid(logged_in):
        evidence.append(("persisted_runtime_logged_in_user", logged_in))
    for name in data_dir_names:
        if valid_wxid(name):
            evidence.append(("wechat_data_dir", name))
    return evidence


def _one(conn: sqlite3.Connection, sql: str, args: tuple = ()) -> dict[str, Any] | None:
    row = conn.execute(sql, args).fetchone()
    return dict(row) if row is not None else None


def _all(conn: sqlite3.Connection, sql: str, args: tuple = ()) -> list[dict[str, Any]]:
    return [dict(row) for row in conn.execute(sql, args).fetchall()]


# ---------------------------------------------------------------------------
# runtime instances


def instance_by_alias(conn: sqlite3.Connection, account_id: str) -> dict[str, Any] | None:
    return _one(conn, "SELECT * FROM runtime_instances WHERE runtime_alias=?", (str(account_id),))


def instance_by_uuid(conn: sqlite3.Connection, instance_uuid: str) -> dict[str, Any] | None:
    return _one(conn, "SELECT * FROM runtime_instances WHERE instance_uuid=?", (str(instance_uuid),))


def ensure_instance(
    conn: sqlite3.Connection,
    account_id: str,
    *,
    instance_uuid: str = "",
    runtime_alias: str = "",
    resource_key: str = "",
    display_name: str = "",
    runtime_provider: str = "",
) -> dict[str, Any]:
    """Create-or-get the runtime instance row with authoritative Identity v2 keys.

    ``instance_uuid`` is the immutable canonical primary key.  If provided by
    the Runtime (or caller), it takes precedence.  ``resource_key`` is the
    immutable physical resource binding key.  ``runtime_alias`` is the
    controlled technical alias (alias rename is deferred/fail-closed in this
    release, Gate F8).
    """
    account_id = str(account_id or "").strip()
    instance_uuid = str(instance_uuid or "").strip()
    runtime_alias = str(runtime_alias or "").strip()
    resource_key = str(resource_key or "").strip()
    target_alias = runtime_alias or account_id

    # 1. If instance_uuid is given, look up by uuid first
    if instance_uuid:
        existing = instance_by_uuid(conn, instance_uuid)
        if existing is not None:
            if runtime_alias and existing["runtime_alias"] != runtime_alias:
                raise IdentityError(
                    "alias_rename_deferred",
                    400,
                    f"runtime_alias rename ({existing['runtime_alias']!r} -> {runtime_alias!r}) is deferred in this release to protect legacy account bindings; display_name can be updated freely",
                )
            updates: list[str] = []
            args: list[Any] = []
            if display_name and existing["display_name"] != display_name:
                updates.append("display_name=?")
                args.append(display_name)
            if runtime_provider and existing["runtime_provider"] != runtime_provider:
                updates.append("runtime_provider=?")
                args.append(runtime_provider)
            if resource_key and existing["resource_key"] != resource_key and not existing["resource_key"]:
                updates.append("resource_key=?")
                args.append(resource_key)
            if updates:
                updates.append("updated_at=?")
                args.append(utc_now())
                args.append(existing["instance_uuid"])
                conn.execute(f"UPDATE runtime_instances SET {', '.join(updates)} WHERE instance_uuid=?", tuple(args))
                existing = instance_by_uuid(conn, instance_uuid) or existing
            return existing

        existing_by_alias = instance_by_alias(conn, target_alias)
        if existing_by_alias is not None:
            raise IdentityError(
                "instance_uuid_conflict",
                409,
                f"alias {target_alias!r} is already bound to instance_uuid {existing_by_alias['instance_uuid']!r}, cannot overwrite with {instance_uuid!r}",
            )

    # 2. Look up by alias or account_id
    existing_by_alias = instance_by_alias(conn, target_alias)
    if existing_by_alias is None and INSTANCE_UUID_RE.match(account_id):
        existing_by_alias = instance_by_uuid(conn, account_id)

    if existing_by_alias is not None:
        if instance_uuid and existing_by_alias["instance_uuid"].lower() != instance_uuid.lower():
            raise IdentityError(
                "instance_uuid_conflict",
                409,
                f"alias {target_alias!r} is already bound to instance_uuid {existing_by_alias['instance_uuid']!r}, cannot overwrite with {instance_uuid!r}",
            )
        updates = []
        args = []
        if display_name and existing_by_alias["display_name"] != display_name:
            updates.append("display_name=?")
            args.append(display_name)
        if runtime_provider and existing_by_alias["runtime_provider"] != runtime_provider:
            updates.append("runtime_provider=?")
            args.append(runtime_provider)
        if resource_key and existing_by_alias["resource_key"] != resource_key:
            if existing_by_alias["resource_key"] == existing_by_alias["runtime_alias"]:
                updates.append("resource_key=?")
                args.append(resource_key)
        if updates:
            updates.append("updated_at=?")
            args.append(utc_now())
            args.append(existing_by_alias["instance_uuid"])
            conn.execute(f"UPDATE runtime_instances SET {', '.join(updates)} WHERE instance_uuid=?", tuple(args))
            existing_by_alias = instance_by_uuid(conn, existing_by_alias["instance_uuid"]) or existing_by_alias
        return existing_by_alias

    # 3. Not found: create new row
    account = _one(conn, "SELECT * FROM accounts WHERE account_id=?", (target_alias,))
    runtime_json = parse_json_safe(account.get("runtime_json")) if account else {}
    provider = str(runtime_provider or (runtime_json or {}).get("runtime_provider") or "legacy")
    row = {
        "instance_uuid": instance_uuid or new_uuid(),
        "runtime_alias": target_alias,
        "display_name": str(display_name or (account or {}).get("display_name") or target_alias),
        "resource_key": resource_key or target_alias,
        "runtime_provider": provider,
        "created_at": utc_now(),
        "updated_at": utc_now(),
    }
    try:
        conn.execute(
            """
            INSERT INTO runtime_instances
                (instance_uuid, runtime_alias, display_name, resource_key, runtime_provider, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["instance_uuid"], row["runtime_alias"], row["display_name"],
                row["resource_key"], row["runtime_provider"], row["created_at"], row["updated_at"],
            ),
        )
    except sqlite3.IntegrityError:
        existing = instance_by_uuid(conn, row["instance_uuid"]) or instance_by_alias(conn, row["runtime_alias"])
        if existing is not None:
            return existing
        raise
    return row


# ---------------------------------------------------------------------------
# wechat identities


def identity_by_wxid(conn: sqlite3.Connection, wechat_user_id: str) -> dict[str, Any] | None:
    return _one(conn, "SELECT * FROM wechat_identities WHERE wechat_user_id=?", (str(wechat_user_id),))


def identity_by_uuid(conn: sqlite3.Connection, wechat_identity_uuid: str) -> dict[str, Any] | None:
    return _one(
        conn,
        "SELECT * FROM wechat_identities WHERE wechat_identity_uuid=?",
        (str(wechat_identity_uuid),),
    )


def create_identity(conn: sqlite3.Connection, *, wechat_user_id: str = "", nickname: str = "") -> dict[str, Any]:
    now = utc_now()
    row = {
        "wechat_identity_uuid": new_uuid(),
        "wechat_user_id": str(wechat_user_id or "").strip() or None,
        "nickname": str(nickname or ""),
        "avatar_ref": "",
        "profile_json": "{}",
        "created_at": now,
        "updated_at": now,
    }
    conn.execute(
        """
        INSERT INTO wechat_identities
            (wechat_identity_uuid, wechat_user_id, nickname, avatar_ref, profile_json, created_at, updated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["wechat_identity_uuid"], row["wechat_user_id"], row["nickname"],
            row["avatar_ref"], row["profile_json"], row["created_at"], row["updated_at"],
        ),
    )
    return row


def find_or_create_identity(conn: sqlite3.Connection, wechat_user_id: str) -> dict[str, Any]:
    wechat_user_id = str(wechat_user_id or "").strip()
    existing = identity_by_wxid(conn, wechat_user_id)
    if existing is not None:
        return existing
    return create_identity(conn, wechat_user_id=wechat_user_id)


# ---------------------------------------------------------------------------
# bindings and observations


def active_binding(conn: sqlite3.Connection, instance_uuid: str) -> dict[str, Any] | None:
    return _one(
        conn,
        """
        SELECT b.*, i.wechat_user_id, i.nickname, i.avatar_ref
        FROM instance_identity_bindings b
        JOIN wechat_identities i ON i.wechat_identity_uuid = b.wechat_identity_uuid
        WHERE b.instance_uuid=? AND b.active=1
        ORDER BY b.bound_at DESC
        LIMIT 1
        """,
        (str(instance_uuid),),
    )


def create_binding(
    conn: sqlite3.Connection,
    instance_uuid: str,
    wechat_identity_uuid: str,
    *,
    verified_source: str,
) -> dict[str, Any]:
    row = {
        "binding_uuid": new_uuid(),
        "instance_uuid": str(instance_uuid),
        "wechat_identity_uuid": str(wechat_identity_uuid),
        "bound_at": utc_now(),
        "unbound_at": None,
        "active": 1,
        "verified_source": str(verified_source),
    }
    conn.execute(
        """
        INSERT INTO instance_identity_bindings
            (binding_uuid, instance_uuid, wechat_identity_uuid, bound_at, unbound_at, active, verified_source)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            row["binding_uuid"], row["instance_uuid"], row["wechat_identity_uuid"],
            row["bound_at"], row["unbound_at"], row["active"], row["verified_source"],
        ),
    )
    return row


def close_binding(conn: sqlite3.Connection, binding_uuid: str) -> None:
    conn.execute(
        "UPDATE instance_identity_bindings SET active=0, unbound_at=? WHERE binding_uuid=? AND active=1",
        (utc_now(), str(binding_uuid)),
    )


def record_observation(
    conn: sqlite3.Connection,
    instance_uuid: str,
    *,
    observed_wechat_user_id: str,
    resolved_state: str,
    verified_source: str,
) -> dict[str, Any]:
    row = {
        "observation_uuid": new_uuid(),
        "instance_uuid": str(instance_uuid),
        "observed_wechat_user_id": str(observed_wechat_user_id or ""),
        "resolved_state": str(resolved_state),
        "verified_source": str(verified_source),
        "created_at": utc_now(),
    }
    conn.execute(
        """
        INSERT INTO instance_identity_observations
            (observation_uuid, instance_uuid, observed_wechat_user_id, resolved_state, verified_source, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            row["observation_uuid"], row["instance_uuid"], row["observed_wechat_user_id"],
            row["resolved_state"], row["verified_source"], row["created_at"],
        ),
    )
    return row


def last_observation(conn: sqlite3.Connection, instance_uuid: str) -> dict[str, Any] | None:
    return _one(
        conn,
        """
        SELECT * FROM instance_identity_observations
        WHERE instance_uuid=? ORDER BY rowid DESC LIMIT 1
        """,
        (str(instance_uuid),),
    )


def backfill_record(conn: sqlite3.Connection, account_id: str) -> dict[str, Any] | None:
    return _one(
        conn,
        """
        SELECT f.*, i.wechat_user_id, i.nickname
        FROM identity_backfills f
        JOIN wechat_identities i ON i.wechat_identity_uuid = f.wechat_identity_uuid
        WHERE f.account_id=?
        """,
        (str(account_id),),
    )


def binding_state(
    conn: sqlite3.Connection,
    instance_uuid: str,
    account_id: str = "",
) -> dict[str, Any]:
    """Derive the current identity binding state for one runtime instance.

    ``mismatch`` is a persisted condition: the last recorded observation said
    the slot is logged in as a different wxid than the active binding.  It
    stays raised across restarts until a matching re-login or an explicit
    confirm-switch records a new observation.
    """
    binding = active_binding(conn, instance_uuid)
    if binding is None:
        backfill = backfill_record(conn, account_id) if account_id else None
        if backfill is not None:
            return {
                "state": STATE_UNRESOLVED,
                "binding": None,
                "identity": identity_by_uuid(conn, backfill["wechat_identity_uuid"]),
                "observation": None,
                "backfill": backfill,
            }
        return {"state": STATE_UNBOUND, "binding": None, "identity": None, "observation": None, "backfill": None}
    identity = identity_by_uuid(conn, binding["wechat_identity_uuid"])
    observation = last_observation(conn, instance_uuid)
    state = STATE_BOUND
    if observation is not None and observation["resolved_state"] == STATE_MISMATCH:
        state = STATE_MISMATCH
    return {
        "state": state,
        "binding": binding,
        "identity": identity,
        "observation": observation,
        "backfill": None,
    }


def _engaged(conn: sqlite3.Connection, instance_uuid: str, account_id: str) -> bool:
    """True once a slot has any identity metadata (binding/observation/backfill)."""
    if backfill_record(conn, account_id) is not None:
        return True
    if last_observation(conn, instance_uuid) is not None:
        return True
    return active_binding(conn, instance_uuid) is not None


# ---------------------------------------------------------------------------
# state machine transitions


def observe_login(
    conn: sqlite3.Connection,
    account_id: str,
    logged_in_user: str,
    *,
    verified_source: str,
    instance_uuid: str = "",
    runtime_alias: str = "",
    resource_key: str = "",
    display_name: str = "",
    runtime_provider: str = "",
) -> dict[str, Any]:
    """Record a runtime-verified ``logged_in_user`` observation for a slot.

    - No active binding: find-or-create the identity for the observed wxid and
      establish the first active binding (first verified login).
    - Active binding for the same identity: keep ``bound`` (clears a mismatch).
    - Active binding for a different identity: record ``mismatch``.  The old
      binding and all of its data ownership stay untouched.
    """
    observed = str(logged_in_user or "").strip()
    if not valid_wxid(observed):
        raise IdentityError(
            "invalid_wechat_user_id",
            400,
            f"observed logged_in_user is not a verifiable WeChat user id: {observed!r}",
        )
    instance = ensure_instance(
        conn,
        account_id,
        instance_uuid=instance_uuid,
        runtime_alias=runtime_alias,
        resource_key=resource_key,
        display_name=display_name,
        runtime_provider=runtime_provider,
    )
    previous = binding_state(conn, instance["instance_uuid"], account_id)
    binding = previous["binding"]
    identity = previous["identity"]

    if binding is not None and identity is not None and str(identity["wechat_user_id"]) == observed:
        new_state = STATE_BOUND
        switched = False
        created_binding = None
    elif binding is not None:
        new_state = STATE_MISMATCH
        switched = False
        created_binding = None
    else:
        bound_identity = find_or_create_identity(conn, observed)
        created_binding = create_binding(
            conn,
            instance["instance_uuid"],
            bound_identity["wechat_identity_uuid"],
            verified_source=verified_source,
        )
        identity = bound_identity
        new_state = STATE_BOUND
        switched = True

    last = previous["observation"]
    changed = previous["state"] != new_state
    if not changed and last is not None and new_state == STATE_MISMATCH:
        changed = str(last["observed_wechat_user_id"] or "") != observed
    if changed or created_binding is not None:
        record_observation(
            conn,
            instance["instance_uuid"],
            observed_wechat_user_id=observed,
            resolved_state=new_state,
            verified_source=verified_source,
        )
    return {
        "account_id": account_id,
        "instance_uuid": instance["instance_uuid"],
        "observed_wechat_user_id": observed,
        "state": new_state,
        "previous_state": previous["state"],
        "changed": changed or created_binding is not None,
        "binding_created": created_binding is not None,
        "wechat_identity_uuid": str(identity["wechat_identity_uuid"]) if identity else "",
        "bound_wechat_user_id": str(identity["wechat_user_id"]) if identity else "",
    }


def confirm_switch(
    conn: sqlite3.Connection,
    account_id: str,
    *,
    observed_wxid: str = "",
) -> dict[str, Any]:
    """Operator-confirmed identity switch for a slot in ``mismatch``.

    Closes the old active binding, activates (or creates) the identity for the
    observed wxid, and records a fresh bound observation.  Historical data of
    the old identity is never re-attributed.
    """
    instance = instance_by_alias(conn, account_id)
    if instance is None:
        raise IdentityError(
            "account_not_found",
            404,
            f"No runtime instance is registered for account_id: {account_id}",
        )
    current = binding_state(conn, instance["instance_uuid"], account_id)
    binding = current["binding"]
    identity = current["identity"]
    mismatch_observed = ""
    if current["state"] == STATE_MISMATCH and current["observation"] is not None:
        mismatch_observed = str(current["observation"]["observed_wechat_user_id"] or "")
    requested = str(observed_wxid or "").strip()
    if mismatch_observed and requested and requested != mismatch_observed:
        raise IdentityError(
            "identity_switch_conflict",
            409,
            "confirm-switch target does not match the recorded mismatch observation",
            details={
                "instance_uuid": instance["instance_uuid"],
                "observed_identity": mismatch_observed,
                "requested_identity": requested,
            },
        )
    target = requested or mismatch_observed
    if not target:
        raise IdentityError(
            "identity_not_mismatch",
            409,
            "confirm-switch requires an active identity mismatch; refusing to switch a slot that is not in conflict",
            details={"instance_uuid": instance["instance_uuid"], "state": current["state"]},
        )
    if not valid_wxid(target):
        raise IdentityError(
            "invalid_wechat_user_id",
            400,
            f"confirm-switch target is not a verifiable WeChat user id: {target!r}",
        )
    if binding is None:
        raise IdentityError(
            "identity_not_mismatch",
            409,
            "confirm-switch requires an active binding in mismatch state",
            details={"instance_uuid": instance["instance_uuid"], "state": current["state"]},
        )
    assert identity is not None  # binding rows always join an identity
    if str(identity["wechat_user_id"]) == target:
        # The operator re-logged the originally bound identity; just clear the mismatch.
        record_observation(
            conn,
            instance["instance_uuid"],
            observed_wechat_user_id=target,
            resolved_state=STATE_BOUND,
            verified_source=VERIFIED_SOURCE_MANUAL_SWITCH,
        )
        return {
            "account_id": account_id,
            "instance_uuid": instance["instance_uuid"],
            "switched": False,
            "state": STATE_BOUND,
            "wechat_identity_uuid": str(identity["wechat_identity_uuid"]),
            "bound_wechat_user_id": target,
        }
    if current["state"] != STATE_MISMATCH:
        raise IdentityError(
            "identity_not_mismatch",
            409,
            "confirm-switch requires an active identity mismatch; refusing to switch a bound slot",
            details={
                "instance_uuid": instance["instance_uuid"],
                "state": current["state"],
                "bound_identity": str(identity["wechat_user_id"]),
            },
        )
    close_binding(conn, binding["binding_uuid"])
    new_identity = find_or_create_identity(conn, target)
    create_binding(
        conn,
        instance["instance_uuid"],
        new_identity["wechat_identity_uuid"],
        verified_source=VERIFIED_SOURCE_MANUAL_SWITCH,
    )
    record_observation(
        conn,
        instance["instance_uuid"],
        observed_wechat_user_id=target,
        resolved_state=STATE_BOUND,
        verified_source=VERIFIED_SOURCE_MANUAL_SWITCH,
    )
    return {
        "account_id": account_id,
        "instance_uuid": instance["instance_uuid"],
        "switched": True,
        "state": STATE_BOUND,
        "previous_wechat_identity_uuid": str(identity["wechat_identity_uuid"]),
        "previous_wechat_user_id": str(identity["wechat_user_id"]),
        "wechat_identity_uuid": str(new_identity["wechat_identity_uuid"]),
        "bound_wechat_user_id": target,
    }


# ---------------------------------------------------------------------------
# gates


def _mismatch_details(info: dict[str, Any], instance: dict[str, Any]) -> dict[str, Any]:
    observation = info.get("observation") or {}
    binding = info.get("binding") or {}
    return {
        "instance_uuid": instance["instance_uuid"],
        "expected_identity": str(binding.get("wechat_user_id") or ""),
        "observed_identity": str(observation.get("observed_wechat_user_id") or ""),
        "action_required": MISMATCH_ACTION_REQUIRED,
        "state": STATE_MISMATCH,
    }


def sync_gate(conn: sqlite3.Connection, account_id: str) -> dict[str, Any]:
    """Resolve the identity context for one ingest/sync business-data write.

    Returns ``{"instance", "identity", "state", "stamp_identity"}`` where
    ``stamp_identity`` is the owning identity UUID ("" for the legacy
    passthrough window).  Raises ``IdentityError`` when writes must be
    refused: ``mismatch`` (observed login differs from the active binding) or
    ``unresolved`` (legacy history without proven identity ownership).
    """
    instance = ensure_instance(conn, account_id)
    info = binding_state(conn, instance["instance_uuid"], account_id)
    state = info["state"]
    if state == STATE_BOUND:
        return {
            "instance": instance,
            "identity": info["identity"],
            "state": state,
            "stamp_identity": str(info["identity"]["wechat_identity_uuid"]),
        }
    if state == STATE_UNRESOLVED:
        raise IdentityError(
            "identity_sync_blocked",
            409,
            "identity is unresolved for this slot; sync writes are blocked until the identity is verified",
            details={
                "account_id": account_id,
                "instance_uuid": instance["instance_uuid"],
                "state": state,
            },
        )
    if state == STATE_MISMATCH:
        raise IdentityError(
            "identity_sync_blocked",
            409,
            "detected a different WeChat login than the bound identity; sync writes are blocked to prevent cross-account contamination",
            details={"account_id": account_id, **_mismatch_details(info, instance)},
        )
    # unbound: fail-open only while the slot never engaged the identity system.
    if _engaged(conn, instance["instance_uuid"], account_id):
        raise IdentityError(
            "identity_sync_blocked",
            409,
            "slot has identity history but no active binding; sync writes are blocked until a verified login",
            details={"account_id": account_id, "instance_uuid": instance["instance_uuid"], "state": state},
        )
    return {"instance": instance, "identity": None, "state": state, "stamp_identity": ""}


def send_gate(
    conn: sqlite3.Connection,
    account_id: str,
    *,
    expected_wechat_identity_uuid: str = "",
) -> dict[str, Any]:
    """Resolve the identity context for one send; refuse anything unsafe.

    ``expected_wechat_identity_uuid`` is the caller's identity intent.  When it
    disagrees with the currently bound identity the request is rejected with
    ``identity_binding_changed`` before anything can reach upstream WeChat.
    """
    instance = ensure_instance(conn, account_id)
    info = binding_state(conn, instance["instance_uuid"], account_id)
    state = info["state"]
    if state == STATE_MISMATCH:
        raise IdentityError(
            "identity_binding_changed",
            409,
            "detected a different WeChat login than the bound identity; sending is blocked to prevent cross-account sends",
            details={"account_id": account_id, **_mismatch_details(info, instance)},
        )
    if state == STATE_UNRESOLVED:
        raise IdentityError(
            "identity_unbound",
            409,
            "identity is unresolved for this slot; sending is blocked until the identity is verified",
            details={"account_id": account_id, "instance_uuid": instance["instance_uuid"], "state": state},
        )
    if state != STATE_BOUND:
        if _engaged(conn, instance["instance_uuid"], account_id):
            raise IdentityError(
                "identity_unbound",
                409,
                "slot has identity history but no active binding; sending is blocked until a verified login",
                details={"account_id": account_id, "instance_uuid": instance["instance_uuid"], "state": state},
            )
        # Legacy passthrough: the slot never engaged the identity system.
        return {"instance": instance, "identity": None, "state": state, "stamp_identity": ""}
    identity = info["identity"]
    bound_uuid = str(identity["wechat_identity_uuid"])
    expected = str(expected_wechat_identity_uuid or "").strip()
    if expected and expected != bound_uuid:
        expected_record = identity_by_uuid(conn, expected)
        raise IdentityError(
            "identity_binding_changed",
            409,
            "caller expected a different WeChat identity than the one currently bound to this slot",
            details={
                "account_id": account_id,
                "instance_uuid": instance["instance_uuid"],
                "expected_identity": str(
                    (expected_record or {}).get("wechat_user_id") or expected
                ),
                "observed_identity": str(identity["wechat_user_id"] or ""),
                "action_required": MISMATCH_ACTION_REQUIRED,
                "state": state,
            },
        )
    return {"instance": instance, "identity": identity, "state": state, "stamp_identity": bound_uuid}


# ---------------------------------------------------------------------------
# account/status view (B7)


def identity_view(conn: sqlite3.Connection, account_id: str) -> dict[str, Any]:
    """Read-only identity projection for account/status API shapes."""
    account_id = str(account_id)
    view: dict[str, Any] = {
        "instance_uuid": "",
        "runtime_alias": account_id,
        "resource_key": "",
        "runtime_provider": "",
        "identity_binding_state": STATE_UNBOUND,
        "wechat_identity_uuid": "",
        "observed_wechat_user_id": "",
        "wechat_profile": {"wechat_user_id": "", "nickname": "", "avatar_url": ""},
    }
    instance = instance_by_alias(conn, account_id)
    if instance is None and INSTANCE_UUID_RE.match(account_id):
        instance = instance_by_uuid(conn, account_id)
    if instance is None:
        return view
    info = binding_state(conn, instance["instance_uuid"], instance["runtime_alias"])
    identity = info["identity"]
    avatar_ref = str((identity or {}).get("avatar_ref") or "")
    view.update(
        {
            "instance_uuid": instance["instance_uuid"],
            "runtime_alias": instance["runtime_alias"],
            "resource_key": instance["resource_key"],
            "runtime_provider": instance["runtime_provider"],
            "identity_binding_state": info["state"],
            "wechat_identity_uuid": str(identity["wechat_identity_uuid"]) if identity else "",
            "observed_wechat_user_id": str(
                (info.get("observation") or {}).get("observed_wechat_user_id") or ""
            ),
            "wechat_profile": {
                "wechat_user_id": str((identity or {}).get("wechat_user_id") or ""),
                "nickname": str((identity or {}).get("nickname") or ""),
                "avatar_url": avatar_ref if avatar_ref.startswith(("/", "http://", "https://")) else "",
            },
        }
    )
    return view


def stamp_ref(conn: sqlite3.Connection, account_id: str) -> tuple[str, str]:
    """Best-effort ``(instance_uuid, wechat_identity_uuid)`` stamp for events.

    Never raises and never gates: observability rows must keep flowing while
    business writes are blocked.
    """
    try:
        instance = instance_by_alias(conn, account_id)
        if instance is None:
            return ("", "")
        info = binding_state(conn, instance["instance_uuid"], account_id)
        identity = info.get("identity")
        return (str(instance["instance_uuid"]), str(identity["wechat_identity_uuid"]) if identity else "")
    except sqlite3.Error:
        return ("", "")


# ---------------------------------------------------------------------------
# legacy migration (B3)


def backfill_account_rows(
    conn: sqlite3.Connection,
    account_id: str,
    instance_uuid: str,
    wechat_identity_uuid: str,
) -> dict[str, int]:
    """Attribute all legacy rows of one account to one identity, idempotently.

    Only rows whose ``wechat_identity_uuid`` is still empty are stamped, so a
    re-run (or a later migration) can never re-own already-attributed history.
    """
    counts: dict[str, int] = {}
    for table in BACKFILL_TABLES:
        cursor = conn.execute(
            f"UPDATE {table} SET instance_uuid=?, wechat_identity_uuid=? "
            "WHERE account_id=? AND wechat_identity_uuid=''",
            (str(instance_uuid), str(wechat_identity_uuid), str(account_id)),
        )
        counts[table] = int(cursor.rowcount or 0)
    return counts


def _has_business_history(conn: sqlite3.Connection, account_id: str) -> bool:
    for table in HISTORY_TABLES:
        row = conn.execute(
            f"SELECT 1 FROM {table} WHERE account_id=? LIMIT 1", (str(account_id),)
        ).fetchone()
        if row is not None:
            return True
    return False


def _record_backfill(
    conn: sqlite3.Connection,
    account_id: str,
    wechat_identity_uuid: str,
    evidence: str,
) -> None:
    conn.execute(
        """
        INSERT INTO identity_backfills (account_id, wechat_identity_uuid, evidence, migrated_at)
        VALUES (?, ?, ?, ?)
        """,
        (str(account_id), str(wechat_identity_uuid), str(evidence), utc_now()),
    )


def migrate_legacy_accounts(
    conn: sqlite3.Connection,
    accounts: Iterable[dict[str, Any]],
    *,
    identity_map: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """One idempotent pass of the legacy account → identity backfill.

    Each account is attributed from its strongest verified evidence:

    1. operator-provided credential metadata (``identity_map``),
    2. persisted runtime ``logged_in_user``,
    3. the WeChat data directory name beneath ``xwechat_files``.

    Accounts without any verifiable evidence get a dedicated unresolved
    identity (``wechat_user_id = NULL``); their history stays owned by that
    UUID and is never merged into another identity.  Proven accounts receive
    an active binding so their decrypted sync keeps flowing under the proven
    identity.
    """
    identity_map = identity_map or {}
    report: list[dict[str, Any]] = []
    for item in accounts:
        account_id = str(item.get("account_id") or "").strip()
        if not account_id:
            continue
        instance = ensure_instance(
            conn,
            account_id,
            instance_uuid=str(item.get("instance_uuid") or "").strip(),
            runtime_alias=str(item.get("runtime_alias") or "").strip(),
            resource_key=str(item.get("resource_key") or "").strip(),
            display_name=str(item.get("display_name") or ""),
            runtime_provider=str(item.get("runtime_provider") or ""),
        )
        existing = backfill_record(conn, account_id)
        if existing is not None:
            report.append(
                {
                    "account_id": account_id,
                    "instance_uuid": instance["instance_uuid"],
                    "status": "already_backfilled",
                    "wechat_identity_uuid": str(existing["wechat_identity_uuid"]),
                    "wechat_user_id": str(existing["wechat_user_id"] or ""),
                    "evidence": str(existing["evidence"]),
                }
            )
            continue
        # Explicit inputs win; fall back to the persisted accounts row so a
        # store-level migration run is self-sufficient.
        stored_runtime = item.get("runtime")
        stored_sync = item.get("sync")
        if stored_runtime is None or stored_sync is None:
            account_row = _one(conn, "SELECT * FROM accounts WHERE account_id=?", (account_id,))
            if account_row is not None:
                if stored_runtime is None:
                    stored_runtime = parse_json_safe(account_row.get("runtime_json"))
                if stored_sync is None:
                    stored_sync = parse_json_safe(account_row.get("sync_json"))
        evidence = collect_legacy_evidence(
            account_id,
            runtime=stored_runtime,
            sync=stored_sync,
            data_dir_names=item.get("data_dir_names") or (),
            identity_map=identity_map,
        )
        counts: dict[str, int] = {}
        if evidence:
            source, wxid = evidence[0]
            identity = find_or_create_identity(conn, wxid)
            counts = backfill_account_rows(conn, account_id, instance["instance_uuid"], identity["wechat_identity_uuid"])
            _record_backfill(conn, account_id, identity["wechat_identity_uuid"], source)
            if active_binding(conn, instance["instance_uuid"]) is None:
                create_binding(
                    conn,
                    instance["instance_uuid"],
                    identity["wechat_identity_uuid"],
                    verified_source=VERIFIED_SOURCE_LEGACY_BACKFILL,
                )
            report.append(
                {
                    "account_id": account_id,
                    "instance_uuid": instance["instance_uuid"],
                    "status": "backfilled",
                    "wechat_identity_uuid": str(identity["wechat_identity_uuid"]),
                    "wechat_user_id": wxid,
                    "evidence": source,
                    "rows": counts,
                }
            )
        else:
            if not _has_business_history(conn, account_id):
                # A slot with no legacy business data never engaged the
                # identity system: leave it unengaged so the legacy
                # passthrough window keeps working until a verified login
                # binds a real identity (contract §4.2 scopes the unresolved
                # placeholder to orphaned *history*).
                report.append(
                    {
                        "account_id": account_id,
                        "instance_uuid": instance["instance_uuid"],
                        "status": "skipped_no_history",
                        "wechat_identity_uuid": "",
                        "wechat_user_id": "",
                        "evidence": "none",
                    }
                )
                continue
            identity = create_identity(conn, nickname=UNRESOLVED_NICKNAME)
            counts = backfill_account_rows(conn, account_id, instance["instance_uuid"], identity["wechat_identity_uuid"])
            _record_backfill(conn, account_id, identity["wechat_identity_uuid"], "unresolved_no_verified_evidence")
            report.append(
                {
                    "account_id": account_id,
                    "instance_uuid": instance["instance_uuid"],
                    "status": "unresolved",
                    "wechat_identity_uuid": str(identity["wechat_identity_uuid"]),
                    "wechat_user_id": "",
                    "evidence": "unresolved_no_verified_evidence",
                    "rows": counts,
                }
            )
    return {"accounts": report}


def parse_json_safe(value: object) -> Any:
    import json

    if not value:
        return {}
    try:
        return json.loads(str(value))
    except json.JSONDecodeError:
        return {}
