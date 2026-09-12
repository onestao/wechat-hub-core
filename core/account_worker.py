"""Run the existing decrypt/ingest/media pipeline once for every registered account."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import traceback
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from .identity import (
    VERIFIED_SOURCE_AGENT_AUTH,
    VERIFIED_SOURCE_RUNTIME_STATUS,
    IdentityError,
    valid_wxid,
    wechat_data_dir_name,
)
from .normalize import import_account
from .registry import AccountConfig, AccountRegistry
from .runtime_bridge import resolve_runtime_account
from .source_provenance import SourceIdentityError
from .store import CoreStore, parse_rfc3339


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)

DEFAULT_FRESHNESS_SLO_MULTIPLIER = 6.0
DEFAULT_MIN_FRESHNESS_SLO_SECONDS = 60.0


def evaluate_account_freshness(
    account_data: dict[str, Any],
    *,
    sync_interval: float = 0.0,
    slo_multiplier: float = DEFAULT_FRESHNESS_SLO_MULTIPLIER,
    min_slo_seconds: float = DEFAULT_MIN_FRESHNESS_SLO_SECONDS,
    now: datetime | None = None,
) -> dict[str, Any]:
    """Inspect account sync status against projection-freshness SLO without mutating on-disk state."""
    sync = account_data.get("sync")
    if not isinstance(sync, dict) or not sync:
        return account_data

    if sync.get("ok") is False:
        if account_data.get("state") == "online":
            account_data["state"] = "degraded"

    if sync_interval <= 0:
        return account_data

    finished_at_str = sync.get("finished_at")
    if not finished_at_str:
        return account_data

    finished_at = parse_rfc3339(finished_at_str)
    if not finished_at:
        return account_data

    current_time = now or datetime.now(timezone.utc)
    age_seconds = max(0.0, (current_time - finished_at).total_seconds())
    slo_seconds = max(min_slo_seconds, float(sync_interval) * slo_multiplier)

    sync["staleness_seconds"] = round(age_seconds, 1)
    sync["slo_seconds"] = round(slo_seconds, 1)
    if age_seconds > slo_seconds:
        sync["stale"] = True
        if isinstance(sync.get("freshness"), dict):
            sync["freshness"]["status"] = "stale"
            sync["freshness"]["staleness_seconds"] = round(age_seconds, 1)
            sync["freshness"]["slo_seconds"] = round(slo_seconds, 1)
        if account_data.get("state") == "online":
            account_data["state"] = "degraded"
    else:
        sync["stale"] = False

    return account_data


class SyncLiveness:
    """Observable liveness state for the account sync worker.

    Consumers (health endpoint, operators, freshness gates) need to tell a
    healthy worker from a silently dead one without inspecting the sync
    thread.  Mutations are thread-safe and ``flush`` writes the snapshot
    atomically; flushing must never raise into the worker.
    """

    def __init__(self, interval_seconds: float, path: Path | None = None) -> None:
        self.interval_seconds = max(1.0, float(interval_seconds))
        self.path = path
        self._lock = threading.Lock()
        self._state: dict[str, Any] = {
            "worker": "wechat-core-sync",
            "interval_seconds": self.interval_seconds,
            "cycle_count": 0,
            "cycle_started_at": "",
            "last_completed_cycle_at": "",
            "last_clean_cycle_at": "",
            "last_cycle_error": "",
            "last_cycle_error_at": "",
            "last_account_errors": {},
            "consecutive_failed_cycles": 0,
            "flush_error_count": 0,
            "updated_at": "",
        }

    def record_cycle_start(self) -> None:
        with self._lock:
            self._state["cycle_count"] += 1
            self._state["cycle_started_at"] = now_iso()
            self._state["updated_at"] = self._state["cycle_started_at"]

    def record_cycle_completed(self, *, clean: bool, account_errors: dict[str, str] | None = None) -> None:
        with self._lock:
            finished = now_iso()
            self._state["last_completed_cycle_at"] = finished
            if clean:
                self._state["last_clean_cycle_at"] = finished
                self._state["consecutive_failed_cycles"] = 0
                self._state["last_account_errors"] = {}
            else:
                self._state["consecutive_failed_cycles"] += 1
                self._state["last_account_errors"] = dict(account_errors or {})
            self._state["updated_at"] = finished

    def record_cycle_error(self, exc: BaseException) -> None:
        with self._lock:
            recorded = now_iso()
            self._state["last_cycle_error"] = f"{type(exc).__name__}: {exc}"
            self._state["last_cycle_error_at"] = recorded
            self._state["consecutive_failed_cycles"] += 1
            self._state["updated_at"] = recorded

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {key: (dict(value) if isinstance(value, dict) else value) for key, value in self._state.items()}

    def flush(self) -> None:
        if self.path is None:
            return
        try:
            write_json(self.path, self.snapshot())
        except OSError:
            with self._lock:
                self._state["flush_error_count"] += 1


def media_args(account: AccountConfig) -> SimpleNamespace:
    return SimpleNamespace(
        memory_db=account.memory_db,
        decrypted_dir=account.decrypted_dir,
        wechat_base_dir=account.wechat_base_dir,
        runtime_dir=account.runtime_dir,
        media_dir=account.media_dir,
        config_file=account.config_file,
        prefer_thumbnails=False,
        download_stickers=False,
    )


class AccountWorker:
    """Keeps upstream data transformation intact while isolating each account's files."""

    def __init__(self, registry: AccountRegistry, store: CoreStore) -> None:
        self.registry = registry
        self.store = store

    def _assert_source_provenance(self, account: AccountConfig) -> None:
        """Fail closed when the selected source directory belongs to another wxid.

        RB-003: immediately before key export, decrypt and ingest, re-verify that
        the selected source identity still matches the fresh runtime identity so
        a historical account directory can never be ingested under the live login.
        """
        observed_user = str(account.runtime.get("logged_in_user") or "").strip()
        source_wxid = wechat_data_dir_name(account.source_db_dir)
        if not source_wxid or not valid_wxid(source_wxid):
            return
        if not observed_user or not valid_wxid(observed_user):
            return
        if source_wxid == observed_user:
            return
        raise SourceIdentityError(
            "source_identity_mismatch",
            409,
            f"source db directory identity {source_wxid!r} does not match fresh "
            f"logged_in_user {observed_user!r} for account {account.account_id}",
            details={
                "account_id": account.account_id,
                "selected_wxid": source_wxid,
                "expected_wxid": observed_user,
                "source_db_dir": str(account.source_db_dir),
            },
        )

    def _persist_final_account_status(
        self,
        account: AccountConfig,
        *,
        state: str,
        sync_status: dict[str, Any],
    ) -> tuple[AccountConfig, str]:
        """Persist the final Worker projection from a fresh ordered Runtime view.

        The decrypt/import portion of a sync cycle can take long enough for a
        Runtime/API writer to observe an explicit logout or health transition
        after this Worker's initial runtime resolution.  Re-resolve only the
        cheap Runtime projection at persistence time while holding the same
        per-account guard as ``CoreStore.upsert_account``.  This prevents a
        stale healthy-gap projection from overwriting a newer authoritative
        negative state without serializing the expensive sync body.
        """
        # Retry3's second Runtime observation is only needed for AgentWechat,
        # where login/source telemetry can change independently while a sync
        # cycle is decrypting/importing.  Keep legacy providers on their
        # established fast path: CoreStore.upsert_account still supplies the
        # per-account semantic dedup lock, but there is no extra account row /
        # binding read or Runtime re-resolution on every legacy cycle.
        if account.runtime_provider != "agent_wechat":
            final_state = state
            if not account.runtime.get("running", True):
                final_state = "stopped"
            elif account.runtime.get("agent_server_healthy") is False:
                final_state = "degraded"
            elif str(account.runtime.get("wechat_login_status") or "").strip() == "logged_out":
                final_state = "login_required"
            public_runtime = account.public_runtime()
            public_runtime["registered"] = True
            self.store.upsert_account(
                account.account_id,
                account.display_name,
                state=final_state,
                runtime=public_runtime,
                sync=sync_status,
            )
            return account, final_state

        with self.store.account_status_guard(account.account_id):
            latest_existing = self.store.account(account.account_id)
            latest_account = account
            latest_binding = self.store.binding_state(account.account_id)
            latest_bound_wxid = str(
                (latest_binding.get("identity") or {}).get("wechat_user_id") or ""
            ).strip()
            try:
                latest_account = resolve_runtime_account(
                    account,
                    bound_wxid=latest_bound_wxid,
                    previous_runtime=(latest_existing or {}).get("runtime") or {},
                    binding_state=str(latest_binding.get("state") or ""),
                )
            except SourceIdentityError as exc:
                # A fresh final projection can itself discover that the
                # current AgentWechat source is no longer provenance-safe.
                # Do not turn that fail-closed result into a persistence
                # failure that leaves the previous account row looking
                # healthy.  Persist the error state using the last safe
                # runtime projection; business-data writes have already
                # been gated by the earlier provenance checks in this
                # cycle, so this branch is observability-only.
                final_runtime = dict((latest_existing or {}).get("runtime") or {})
                if final_runtime:
                    latest_account = replace(account, runtime=final_runtime)
                state = "error"
                sync_status["ok"] = False
                sync_status.setdefault("error", str(exc))
                sync_status.setdefault(
                    "source_provenance",
                    {"code": exc.code, "status": exc.status, **exc.details},
                )

            final_state = state
            if not latest_account.runtime.get("running", True):
                final_state = "stopped"
            elif latest_account.runtime.get("agent_server_healthy") is False:
                final_state = "degraded"
            else:
                final_login_status = str(
                    latest_account.runtime.get("wechat_login_status") or ""
                ).strip()
                if final_login_status == "logged_out":
                    final_state = "login_required"
                elif final_login_status != "logged_in":
                    # Unknown/empty telemetry after an authoritative negative
                    # state is not evidence of recovery.  Keep that latest
                    # state until a fresh valid positive observation arrives.
                    latest_state = str((latest_existing or {}).get("state") or "")
                    if latest_state in {"login_required", "stopped", "degraded"}:
                        final_state = latest_state

            public_runtime = latest_account.public_runtime()
            public_runtime["registered"] = True
            self.store.upsert_account(
                latest_account.account_id,
                latest_account.display_name,
                state=final_state,
                runtime=public_runtime,
                sync=sync_status,
            )
            return latest_account, final_state

    def run_account(self, account: AccountConfig, *, force_refresh: bool = False) -> dict[str, Any]:
        started = time.monotonic()
        status: dict[str, Any] = {
            "account_id": account.account_id,
            "ok": False,
            "started_at": now_iso(),
            "source_db_dir": str(account.source_db_dir),
        }
        try:
            if account.runtime_provider == "agent_wechat":
                existing_account = self.store.account(account.account_id)
                binding_info = self.store.binding_state(account.account_id)
                bound_wxid = str(
                    (binding_info.get("identity") or {}).get("wechat_user_id") or ""
                ).strip()
                account = resolve_runtime_account(
                    account,
                    bound_wxid=bound_wxid,
                    previous_runtime=(existing_account or {}).get("runtime") or {},
                    binding_state=str(binding_info.get("state") or ""),
                )
            else:
                account = resolve_runtime_account(account)
            status["source_db_dir"] = str(account.source_db_dir)
            self._assert_source_provenance(account)

            # Sync Gate prerequisite (B5): observe the runtime-verified login
            # BEFORE any ingest can write, so the binding state machine decides
            # whether this cycle may write business data at all.
            observed_user = str(account.runtime.get("logged_in_user") or "").strip()
            if observed_user and valid_wxid(observed_user):
                self.store.observe_login(
                    account.account_id,
                    observed_user,
                    verified_source=(
                        VERIFIED_SOURCE_AGENT_AUTH
                        if account.runtime_provider == "agent_wechat"
                        else VERIFIED_SOURCE_RUNTIME_STATUS
                    ),
                    instance_uuid=account.instance_uuid,
                    runtime_alias=account.runtime_alias,
                    resource_key=account.resource_key,
                )

            # Second provenance assertion (Taskbook RB-003):
            # Assert wechat_data_dir_name(source_db_dir) == fresh logged_in_user
            # and matches bound identity before extracting keys, decrypting, or importing.
            source_wxid = wechat_data_dir_name(account.source_db_dir)
            if source_wxid and valid_wxid(source_wxid):
                if observed_user and valid_wxid(observed_user) and source_wxid != observed_user:
                    raise IdentityError(
                        "source_identity_mismatch",
                        409,
                        f"source db directory identity {source_wxid!r} does not match fresh logged_in_user {observed_user!r}",
                        details={
                            "account_id": account.account_id,
                            "selected_wxid": source_wxid,
                            "expected_wxid": observed_user,
                            "source_db_dir": str(account.source_db_dir),
                        },
                    )
                binding_info = self.store.binding_state(account.account_id)
                bound_wxid = str((binding_info.get("identity") or {}).get("wechat_user_id") or "").strip()
                if bound_wxid and valid_wxid(bound_wxid) and source_wxid != bound_wxid:
                    raise IdentityError(
                        "source_identity_mismatch",
                        409,
                        f"source db directory identity {source_wxid!r} does not match bound identity {bound_wxid!r}",
                        details={
                            "account_id": account.account_id,
                            "selected_wxid": source_wxid,
                            "bound_wxid": bound_wxid,
                            "source_db_dir": str(account.source_db_dir),
                        },
                    )
            # These existing upstream modules require production image dependencies
            # such as pycryptodome.  Keep API-only consumers independent of a live
            # decrypt environment until a sync cycle is explicitly requested.
            from memory.decrypt_sync import refresh_decrypted
            from memory.media_sync import sync_media
            from memory.memory_ingest import ingest_memory
            from memory.sync_repair import repair_memory_indexes

            key_extract: dict[str, Any] | None = None
            needs_key_refresh = account.runtime_provider == "agent_wechat" or (
                not account.keys_file.is_file() or account.keys_file.stat().st_size == 0
            )
            if account.runtime.get("runtime_bridge") and needs_key_refresh:
                if not account.source_db_dir.is_dir():
                    raise RuntimeError(
                        f"Runtime account source db_storage is not available yet: {account.source_db_dir}"
                    )
                from .key_extract import extract_account_keys

                key_extract = extract_account_keys(
                    account,
                    root=Path(__file__).resolve().parents[1],
                )
                if int(key_extract.get("returncode") or 0) != 0 or not account.keys_file.is_file():
                    raise RuntimeError(
                        f"account-scoped key extraction failed for {account.account_id}: {key_extract}"
                    )

            refresh = refresh_decrypted(
                source_db_dir=account.source_db_dir,
                decrypted_dir=account.decrypted_dir,
                keys_file=account.keys_file,
                state_file=account.decrypt_state_file,
                force=force_refresh,
            )
            repair = None
            try:
                ingest = ingest_memory(account.decrypted_dir, account.memory_db)
            except sqlite3.DatabaseError as exc:
                if "database disk image is malformed" not in str(exc).lower():
                    raise
                repair = repair_memory_indexes(account.memory_db)
                if not repair.get("ok"):
                    raise
                ingest = ingest_memory(account.decrypted_dir, account.memory_db)
            media = sync_media(media_args(account))
            normalized = import_account(account, self.store)
            has_failed_shards = bool(refresh.get("failed"))
            missing_key_shards = list(refresh.get("missing_key") or [])
            is_complete = not missing_key_shards
            sync_ok = (not has_failed_shards) and is_complete

            freshness = {
                "status": "healthy" if sync_ok else "degraded",
                "completeness": "complete" if is_complete else "incomplete",
                "source": {
                    "source_db_dir": str(account.source_db_dir),
                    "total_shards": len(refresh.get("updated", []))
                    + len(refresh.get("skipped", []))
                    + len(missing_key_shards)
                    + len(refresh.get("failed", [])),
                },
                "decrypt": {
                    "finished_at": now_iso(),
                    "updated_count": len(refresh.get("updated", [])),
                    "skipped_count": len(refresh.get("skipped", [])),
                    "missing_key_count": len(missing_key_shards),
                    "failed_count": len(refresh.get("failed", [])),
                    "missing_keys": missing_key_shards,
                },
                "staging": {
                    "memory_db": str(account.memory_db),
                    "chats": ingest.get("chats", 0) if isinstance(ingest, dict) else 0,
                    "messages": ingest.get("messages", 0) if isinstance(ingest, dict) else 0,
                    "changed_rows": ingest.get("changed_rows", 0) if isinstance(ingest, dict) else 0,
                },
                "core": {
                    "projected_at": now_iso(),
                    "chats": normalized.get("chats", 0) if isinstance(normalized, dict) else 0,
                    "messages": normalized.get("messages", 0) if isinstance(normalized, dict) else 0,
                    "message_changes": normalized.get("message_changes", 0) if isinstance(normalized, dict) else 0,
                },
            }
            status.update(
                {
                    "ok": sync_ok,
                    "completeness": "complete" if is_complete else "incomplete",
                    "key_extract": key_extract,
                    "refresh": refresh,
                    "repair": repair,
                    "ingest": ingest,
                    "media": media,
                    "normalized": normalized,
                    "freshness": freshness,
                }
            )
            state = "online" if sync_ok else "degraded"
            if missing_key_shards:
                status["missing_key_shards"] = missing_key_shards
                status["degraded_reason"] = f"missing decryption key for: {', '.join(missing_key_shards)}"
            elif has_failed_shards:
                failed_names = [item.get("db", "unknown") for item in refresh.get("failed", []) if isinstance(item, dict)]
                status["degraded_reason"] = f"decrypt failed for: {', '.join(failed_names)}"
            if (
                account.runtime_provider == "agent_wechat"
                and account.runtime.get("agent_server_healthy") is False
            ):
                state = "degraded"
                status["runtime_health_error"] = str(
                    account.runtime.get("health_error") or "agent-wechat agent-server is unhealthy"
                )
        except SourceIdentityError as exc:
            status["error"] = str(exc)
            status["source_provenance"] = {"code": exc.code, "status": exc.status, **exc.details}
            state = "error"
        except IdentityError as exc:
            # Sync Gate refusal (mismatch/unresolved): keep the cycle observable
            # without writing business data (contract §3.2 rule 1).
            status["error"] = str(exc)
            status["identity_binding"] = {"code": exc.code, **exc.details}
            state = "degraded"
            try:
                self.store.record_identity_event(
                    account.account_id,
                    "identity.sync_blocked",
                    {"error": {"code": exc.code, "message": str(exc)}, "details": exc.details},
                )
            except Exception:  # observability must never mask the gate refusal
                pass
        except Exception as exc:  # Sync failures are account-scoped and must not stop peer accounts.
            status["error"] = str(exc)
            status["ok"] = False
            status["completeness"] = "unknown"
            status["freshness"] = {
                "status": "error",
                "completeness": "unknown",
                "error": str(exc),
            }
            state = "error"
        status["finished_at"] = now_iso()
        status["elapsed_seconds"] = round(time.monotonic() - started, 3)
        write_json(account.sync_status_file, status)
        # Registry hot-removal can happen while a long decrypt/media cycle is
        # running.  Do not resurrect an account row as online after it has
        # already been removed from the live Runtime registry.
        if self.registry.get(account.account_id) is None:
            status["deregistered_during_sync"] = True
            return status
        try:
            account, state = self._persist_final_account_status(
                account,
                state=state,
                sync_status=status,
            )
        except Exception as exc:
            # Final account-status persistence must never kill the worker.
            # The per-account status file above already recorded the cycle
            # outcome; record the persistence failure observably and stop.
            status["persistence_error"] = f"{type(exc).__name__}: {exc}"
            write_json(account.sync_status_file, status)
        return status

    def run_once(self, *, force_refresh: bool = False) -> dict[str, Any]:
        results: list[dict[str, Any]] = []
        for account in self.registry.all():
            try:
                results.append(self.run_account(account, force_refresh=force_refresh))
            except Exception as exc:
                # Last-resort per-account boundary: one account's unexpected
                # failure must not block its peers from this sync cycle.
                results.append(
                    {
                        "account_id": account.account_id,
                        "ok": False,
                        "error": f"unexpected sync failure: {type(exc).__name__}: {exc}",
                        "unexpected_failure": True,
                        "finished_at": now_iso(),
                    }
                )
        return {"ok": all(result.get("ok") for result in results), "accounts": results, "finished_at": now_iso()}


class AccountSyncLoop:
    # Unexpected cycle exceptions back off exponentially up to this cap so a
    # persistently failing cycle cannot busy-loop the worker. Bounded, never 0.
    MAX_FAILURE_BACKOFF_SECONDS = 60.0

    def __init__(self, worker: AccountWorker, interval_seconds: float, *, liveness_path: Path | None = None) -> None:
        self.worker = worker
        self.interval_seconds = max(1.0, float(interval_seconds))
        self.liveness = SyncLiveness(self.interval_seconds, path=liveness_path)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="wechat-core-sync", daemon=True)
        self.last_run_at: str | None = None
        self.last_run_ok: bool = True
        self.last_error: str = ""
        self.consecutive_failures: int = 0

    def is_alive(self) -> bool:
        return self._thread.is_alive()

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=max(2.0, self.interval_seconds + 1.0))

    def _cycle_account_errors(self, result: dict[str, Any]) -> dict[str, str]:
        errors: dict[str, str] = {}
        for item in result.get("accounts") or []:
            if not isinstance(item, dict):
                continue
            message = str(item.get("error") or item.get("persistence_error") or "").strip()
            if message:
                errors[str(item.get("account_id"))] = message
        return errors

    def _failure_delay(self) -> float:
        failures = self.liveness.snapshot()["consecutive_failed_cycles"]
        return min(
            self.MAX_FAILURE_BACKOFF_SECONDS,
            self.interval_seconds * (2 ** min(int(failures), 6)),
        )

    def _run(self) -> None:
        while not self._stop.is_set():
            self.liveness.record_cycle_start()
            self.liveness.flush()
            try:
                result = self.worker.run_once()
            except Exception as exc:
                # Last-resort boundary: an unexpected per-cycle exception is
                # logged and recorded, then the loop continues after a bounded
                # delay instead of terminating the worker thread.
                self.liveness.record_cycle_error(exc)
                traceback.print_exc()
                self.liveness.flush()
                self.last_run_at = now_iso()
                self.last_run_ok = False
                self.consecutive_failures += 1
                self.last_error = f"{type(exc).__name__}: {exc}"
                self._stop.wait(self._failure_delay())
                continue
            account_errors = self._cycle_account_errors(result)
            self.liveness.record_cycle_completed(clean=not account_errors, account_errors=account_errors)
            self.liveness.flush()
            cycle_ok = bool(result.get("ok"))
            self.last_run_at = now_iso()
            self.last_run_ok = cycle_ok
            if cycle_ok:
                self.consecutive_failures = 0
                self.last_error = ""
            else:
                # Account-level degradation is normal operation: the loop keeps
                # the regular cadence, but the failure streak stays observable.
                self.consecutive_failures += 1
                self.last_error = (
                    "; ".join(f"{account_id}: {message}" for account_id, message in account_errors.items())
                    or "sync cycle reported failures"
                )
            self._stop.wait(self.interval_seconds)
