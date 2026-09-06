"""Run the existing decrypt/ingest/media pipeline once for every registered account."""

from __future__ import annotations

import json
import sqlite3
import threading
import time
import traceback
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

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

    def run_account(self, account: AccountConfig, *, force_refresh: bool = False) -> dict[str, Any]:
        account = resolve_runtime_account(account)
        started = time.monotonic()
        status: dict[str, Any] = {
            "account_id": account.account_id,
            "ok": False,
            "started_at": now_iso(),
            "source_db_dir": str(account.source_db_dir),
        }
        try:
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
        try:
            self.store.upsert_account(
                account.account_id,
                account.display_name,
                state=state,
                runtime=public_runtime,
                sync=status,
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
                self._stop.wait(self._failure_delay())
                continue
            account_errors = self._cycle_account_errors(result)
            self.liveness.record_cycle_completed(clean=not account_errors, account_errors=account_errors)
            self.liveness.flush()
            self._stop.wait(self.interval_seconds)
