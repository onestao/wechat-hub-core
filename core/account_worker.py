"""Run the existing decrypt/ingest/media pipeline once for every registered account."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
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
from .store import CoreStore


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


def write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    temporary.replace(path)


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

    def run_account(self, account: AccountConfig, *, force_refresh: bool = False) -> dict[str, Any]:
        started = time.monotonic()
        status: dict[str, Any] = {
            "account_id": account.account_id,
            "ok": False,
            "started_at": now_iso(),
            "source_db_dir": str(account.source_db_dir),
        }
        try:
            account = resolve_runtime_account(account)
            status["source_db_dir"] = str(account.source_db_dir)
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
            status.update(
                {
                    "ok": not refresh["failed"],
                    "key_extract": key_extract,
                    "refresh": refresh,
                    "repair": repair,
                    "ingest": ingest,
                    "media": media,
                    "normalized": normalized,
                }
            )
            state = "online" if status["ok"] else "degraded"
            if (
                account.runtime_provider == "agent_wechat"
                and account.runtime.get("agent_server_healthy") is False
            ):
                state = "degraded"
                status["runtime_health_error"] = str(
                    account.runtime.get("health_error") or "agent-wechat agent-server is unhealthy"
                )
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
        public_runtime = account.public_runtime()
        public_runtime["registered"] = True
        self.store.upsert_account(
            account.account_id,
            account.display_name,
            state=state,
            runtime=public_runtime,
            sync=status,
        )
        return status

    def run_once(self, *, force_refresh: bool = False) -> dict[str, Any]:
        results = [self.run_account(account, force_refresh=force_refresh) for account in self.registry.all()]
        return {"ok": all(result.get("ok") for result in results), "accounts": results, "finished_at": now_iso()}


class AccountSyncLoop:
    def __init__(self, worker: AccountWorker, interval_seconds: float) -> None:
        self.worker = worker
        self.interval_seconds = max(1.0, float(interval_seconds))
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="wechat-core-sync", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        self._thread.join(timeout=max(2.0, self.interval_seconds + 1.0))

    def _run(self) -> None:
        while not self._stop.is_set():
            self.worker.run_once()
            self._stop.wait(self.interval_seconds)
