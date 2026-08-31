#!/usr/bin/env python3
"""Periodic read-only WeChat decrypt + memory ingestion worker."""

from __future__ import annotations

import argparse
import json
import signal
import sqlite3
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

if __package__:
    # Package imports keep the module reusable from Core.  The script branch
    # below preserves the historical `python memory/sync_worker.py` entrypoint.
    from .decrypt_sync import refresh_decrypted
    from .media_sync import sync_media
    from .memory_ingest import ingest_memory
    from .sync_repair import repair_memory_indexes
else:
    from decrypt_sync import refresh_decrypted
    from media_sync import sync_media
    from memory_ingest import ingest_memory
    from sync_repair import repair_memory_indexes


STOP = False


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    tmp.replace(path)


def handle_stop(signum, frame) -> None:  # noqa: ARG001
    global STOP
    STOP = True


def run_once(args) -> dict:
    started = time.time()
    refresh = refresh_decrypted(
        source_db_dir=args.source_db_dir,
        decrypted_dir=args.decrypted_dir,
        keys_file=args.keys_file,
        state_file=args.decrypt_state_file,
        force=args.force_refresh,
    )
    repair = None
    try:
        ingest = ingest_memory(args.decrypted_dir, args.memory_db)
    except sqlite3.DatabaseError as exc:
        if "database disk image is malformed" not in str(exc).lower():
            raise
        repair = repair_memory_indexes(args.memory_db)
        if not repair.get("ok"):
            raise
        ingest = ingest_memory(args.decrypted_dir, args.memory_db)
    media = sync_media(args)
    return {
        "ok": not refresh["failed"],
        "started_at": datetime.fromtimestamp(started, timezone.utc).isoformat(timespec="seconds"),
        "finished_at": now_iso(),
        "elapsed_seconds": round(time.time() - started, 3),
        "interval_seconds": args.interval,
        "refresh": refresh,
        "repair": repair,
        "ingest": ingest,
        "media": media,
    }


def parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description="Run periodic WeChat memory sync")
    parser.add_argument("--interval", type=float, default=5.0, help="Polling interval seconds")
    parser.add_argument("--source-db-dir", type=Path, default=Path("config/xwechat_files/PLEASE_SET_WECHAT_ACCOUNT_DIR/db_storage"))
    parser.add_argument("--decrypted-dir", type=Path, default=Path("runtime/wechat-decrypt/decrypted"))
    parser.add_argument("--keys-file", type=Path, default=Path("runtime/wechat-decrypt/keys/all_keys.json"))
    parser.add_argument("--decrypt-state-file", type=Path, default=Path("runtime/wechat-decrypt/sync_state.json"))
    parser.add_argument("--memory-db", type=Path, default=Path("runtime/memory/wechat_memory.sqlite"))
    parser.add_argument("--status-file", type=Path, default=Path("runtime/memory/sync_status.json"))
    parser.add_argument("--wechat-base-dir", type=Path, default=Path("config/xwechat_files/PLEASE_SET_WECHAT_ACCOUNT_DIR"))
    parser.add_argument("--runtime-dir", type=Path, default=Path("runtime"))
    parser.add_argument("--media-dir", type=Path, default=Path("runtime/media"))
    parser.add_argument("--config-file", type=Path, default=Path("runtime/wechat-decrypt/config.json"))
    parser.add_argument("--prefer-full-images", action="store_true", help="Prefer full image .dat over thumbnails")
    parser.add_argument("--once", action="store_true", help="Run one cycle and exit")
    parser.add_argument("--force-refresh", action="store_true", help="Refresh all decrypted copies this run")
    return parser.parse_args(argv)


def resolve_args(args):
    root = Path.cwd()
    for key in (
        "source_db_dir",
        "decrypted_dir",
        "keys_file",
        "decrypt_state_file",
        "memory_db",
        "status_file",
        "wechat_base_dir",
        "runtime_dir",
        "media_dir",
        "config_file",
    ):
        value = getattr(args, key)
        if not value.is_absolute():
            setattr(args, key, root / value)
    args.prefer_thumbnails = False
    return args


def main(argv: list[str] | None = None) -> int:
    args = resolve_args(parse_args(argv))
    signal.signal(signal.SIGTERM, handle_stop)
    signal.signal(signal.SIGINT, handle_stop)

    while not STOP:
        try:
            status = run_once(args)
            write_json(args.status_file, status)
            print(json.dumps(status, ensure_ascii=False), flush=True)
        except Exception as exc:
            status = {
                "ok": False,
                "finished_at": now_iso(),
                "interval_seconds": args.interval,
                "error": str(exc),
            }
            write_json(args.status_file, status)
            print(json.dumps(status, ensure_ascii=False), file=sys.stderr, flush=True)

        if args.once:
            return 0 if status.get("ok") else 1
        deadline = time.time() + max(args.interval, 1.0)
        while not STOP and time.time() < deadline:
            time.sleep(min(0.25, deadline - time.time()))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
