#!/usr/bin/env python3
"""Regression test suite for SQLite numeric chat_username handling in media sync.

Covers T1-T8 required by Section 4 (M2) of FLASH_RC5_MEDIA_SYNC_HOTFIX_AND_IDENTITY_RC8_TASKBOOK.md:
- T1: find_dat_files accepts numeric chat_username without AttributeError
- T2: numeric 12345 hashes identically to textual "12345"
- T3: load_resource_map normalizes numeric ChatName2Id.user_name to string key
- T4: sync_image matches a normalized numeric message chat_username to resource map
- T5: sync_video uses the same normalized key contract
- T6: None/invalid chat username fails safely or produces missing metadata/file, but never crashes the whole worker
- T7: an account-worker cycle containing numeric chat value completes without incrementing consecutive_failed_cycles
- T8: successful cycles after historical failure condition restore worker health projection expected by H2 (consecutive_failed_cycles == 0)
"""

from __future__ import annotations

import hashlib
import json
import shutil
import sqlite3
import sys
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

CORE_ROOT = Path(__file__).resolve().parents[2]
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from core.account_worker import AccountSyncLoop, AccountWorker, SyncLiveness, media_args  # noqa: E402
from core.registry import AccountRegistry, parse_account  # noqa: E402
from core.store import CoreStore  # noqa: E402
from memory import media_sync  # noqa: E402


class MediaSyncNumericChatTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = CORE_ROOT / ".tmp" / f"test-numeric-chat-{uuid.uuid4().hex}"
        self.root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root, ignore_errors=True)

    def test_t1_find_dat_files_accepts_numeric_chat_username_without_attribute_error(self) -> None:
        """T1: find_dat_files accepts numeric chat_username without AttributeError."""
        wechat_dir = self.root / "t1_wechat"
        wechat_dir.mkdir(parents=True, exist_ok=True)
        # In the unpatched version, 12345.encode() raised AttributeError: 'int' object has no attribute 'encode'
        files = media_sync.find_dat_files(wechat_dir, 12345, "a" * 32)
        self.assertIsInstance(files, list)
        self.assertEqual(files, [])

    def test_t2_numeric_12345_hashes_identically_to_textual_12345(self) -> None:
        """T2: numeric 12345 hashes identically to textual '12345'."""
        wechat_dir = self.root / "t2_wechat"
        expected_hash = hashlib.md5(b"12345").hexdigest()
        attach_img_dir = wechat_dir / "msg" / "attach" / expected_hash / "01" / "Img"
        attach_img_dir.mkdir(parents=True, exist_ok=True)
        media_md5 = "b" * 32
        sample_dat = attach_img_dir / f"{media_md5}_t.dat"
        sample_dat.write_bytes(b"dat_payload")

        files_numeric = media_sync.find_dat_files(wechat_dir, 12345, media_md5)
        files_textual = media_sync.find_dat_files(wechat_dir, "12345", media_md5)

        self.assertEqual(len(files_numeric), 1)
        self.assertEqual(files_numeric, files_textual)
        self.assertEqual(files_numeric[0], sample_dat)
        self.assertEqual(
            hashlib.md5(media_sync.normalize_chat_username(12345).encode("utf-8")).hexdigest(),
            expected_hash,
        )

    def test_t3_load_resource_map_normalizes_numeric_chatname_to_string_key(self) -> None:
        """T3: load_resource_map normalizes numeric ChatName2Id.user_name to string key."""
        res_db = self.root / "t3_resource.db"
        with sqlite3.connect(res_db) as conn:
            conn.execute("CREATE TABLE ChatName2Id (rowid INTEGER PRIMARY KEY, user_name TEXT);")
            conn.execute(
                """
                CREATE TABLE MessageResourceInfo (
                    chat_id INTEGER,
                    message_local_id INTEGER,
                    message_create_time INTEGER,
                    message_local_type INTEGER,
                    packed_info BLOB
                );
                """
            )
            # Store integer in SQLite user_name column
            conn.execute("INSERT INTO ChatName2Id (rowid, user_name) VALUES (1, 12345);")
            md5_hex = "c" * 32
            # Packed info with marker b"\x12\x22\x0a\x20" + 32 ascii hex bytes
            blob = b"prefix\x12\x22\x0a\x20" + md5_hex.encode("ascii") + b"suffix"
            conn.execute("INSERT INTO MessageResourceInfo VALUES (1, 42, 1000, 3, ?);", (blob,))
            conn.commit()

        rmap = media_sync.load_resource_map(res_db)
        self.assertIn(("12345", 42), rmap)
        self.assertEqual(rmap[("12345", 42)], md5_hex)
        # Must NOT have integer key
        self.assertNotIn((12345, 42), rmap)
        for key in rmap:
            self.assertIsInstance(key[0], str)
            self.assertIsInstance(key[1], int)

    def test_t4_sync_image_matches_normalized_numeric_chat_username_to_resource_map(self) -> None:
        """T4: sync_image matches a normalized numeric message chat_username to resource map."""
        row = {
            "message_uid": "msg-img-101",
            "chat_username": 12345,  # SQLite numeric chat_username
            "local_id": 101,
            "type_label": "image",
        }
        md5_hex = "d" * 32
        # Resource map keyed by normalized string chat_username
        resource_map = {("12345", 101): md5_hex}

        args = SimpleNamespace(
            wechat_base_dir=self.root / "t4_wechat",
            media_dir=self.root / "t4_media",
            prefer_thumbnails=False,
        )

        result = media_sync.sync_image(row, args, resource_map, {})
        # Should NOT report missing_metadata because the key was normalized and matched
        self.assertNotEqual(result["status"], "missing_metadata")
        self.assertEqual(result["original_md5"], md5_hex)
        self.assertEqual(result["chat_username"], "12345")
        self.assertIsInstance(result["chat_username"], str)
        self.assertEqual(result["local_id"], 101)

    def test_t5_sync_video_uses_normalized_key_contract(self) -> None:
        """T5: sync_video uses the same normalized key contract."""
        row = {
            "message_uid": "msg-vid-202",
            "chat_username": 98765,  # SQLite numeric chat_username
            "local_id": 202,
            "type_label": "video",
        }
        md5_hex = "e" * 32
        resource_map = {("98765", 202): md5_hex}

        args = SimpleNamespace(
            wechat_base_dir=self.root / "t5_wechat",
            media_dir=self.root / "t5_media",
        )

        result = media_sync.sync_video(row, args, resource_map)
        self.assertNotEqual(result["status"], "missing_metadata")
        self.assertEqual(result["original_md5"], md5_hex)
        self.assertEqual(result["chat_username"], "98765")
        self.assertIsInstance(result["chat_username"], str)
        self.assertEqual(result["local_id"], 202)

    def test_t6_none_or_invalid_chat_fails_safely_never_crashes(self) -> None:
        """T6: None/invalid chat username fails safely or produces missing metadata/file, never crashes worker."""
        # None and empty chat in find_dat_files
        self.assertEqual(media_sync.find_dat_files(self.root, None, "abc"), [])
        self.assertEqual(media_sync.find_dat_files(self.root, "", "abc"), [])
        self.assertEqual(media_sync.find_dat_files(self.root, "valid_chat", ""), [])

        # normalize_chat_username contract
        self.assertEqual(media_sync.normalize_chat_username(None), "")
        self.assertEqual(media_sync.normalize_chat_username(""), "")
        self.assertEqual(media_sync.normalize_chat_username(123), "123")
        self.assertEqual(media_sync.normalize_chat_username(b"test"), "test")
        with self.assertRaises(UnicodeDecodeError):
            media_sync.normalize_chat_username(b"\xff\xfe")

        # sync_image with None chat_username
        row_none = {
            "message_uid": "msg-none-303",
            "chat_username": None,
            "local_id": 303,
            "type_label": "image",
        }
        args = SimpleNamespace(
            wechat_base_dir=self.root / "t6_wechat",
            media_dir=self.root / "t6_media",
            prefer_thumbnails=False,
        )
        res_img = media_sync.sync_image(row_none, args, {}, {})
        self.assertEqual(res_img["status"], "missing_metadata")
        self.assertEqual(res_img["chat_username"], "")

        res_vid = media_sync.sync_video(row_none, args, {})
        self.assertEqual(res_vid["status"], "missing_metadata")
        self.assertEqual(res_vid["chat_username"], "")

    def _setup_sync_environment(self, name: str) -> tuple[AccountWorker, AccountSyncLoop, Path]:
        acc_dir = self.root / name
        acc_dir.mkdir(parents=True, exist_ok=True)
        runtime_dir = acc_dir / "runtime"
        runtime_dir.mkdir(parents=True, exist_ok=True)

        account = parse_account(
            {
                "account_id": f"acc_{name}",
                "display_name": f"Account {name}",
                "runtime_dir": f"runtime/accounts/{name}",
                "runtime": {"logged_in": True},
            },
            root=acc_dir,
        )
        account.source_db_dir.mkdir(parents=True, exist_ok=True)
        account.decrypted_dir.mkdir(parents=True, exist_ok=True)
        account.memory_db.parent.mkdir(parents=True, exist_ok=True)
        account.keys_file.parent.mkdir(parents=True, exist_ok=True)
        account.keys_file.write_text("{}", encoding="utf-8")
        account.wechat_base_dir.mkdir(parents=True, exist_ok=True)

        # Set up memory db with messages table containing numeric chat_username = 12345
        with sqlite3.connect(account.memory_db) as conn:
            conn.execute(
                """
                CREATE TABLE messages (
                    message_uid TEXT PRIMARY KEY,
                    chat_username TEXT,
                    local_id INTEGER,
                    type_label TEXT,
                    create_time INTEGER,
                    message_content TEXT
                );
                """
            )
            # Numeric chat_username
            conn.execute(
                "INSERT INTO messages VALUES ('msg-1', 12345, 101, 'image', 1000, 'hello');"
            )
            conn.commit()

        # Set up message_resource.db with ChatName2Id having numeric user_name = 12345
        res_dir = account.decrypted_dir / "message"
        res_dir.mkdir(parents=True, exist_ok=True)
        res_db = res_dir / "message_resource.db"
        with sqlite3.connect(res_db) as conn:
            conn.execute("CREATE TABLE ChatName2Id (rowid INTEGER PRIMARY KEY, user_name TEXT);")
            conn.execute(
                """
                CREATE TABLE MessageResourceInfo (
                    chat_id INTEGER,
                    message_local_id INTEGER,
                    message_create_time INTEGER,
                    message_local_type INTEGER,
                    packed_info BLOB
                );
                """
            )
            conn.execute("INSERT INTO ChatName2Id (rowid, user_name) VALUES (1, 12345);")
            md5_hex = "f" * 32
            blob = b"prefix\x12\x22\x0a\x20" + md5_hex.encode("ascii") + b"suffix"
            conn.execute("INSERT INTO MessageResourceInfo VALUES (1, 101, 1000, 3, ?);", (blob,))
            conn.commit()

        # Create attach directory for 12345
        chat_hash = hashlib.md5(b"12345").hexdigest()
        attach_img_dir = account.wechat_base_dir / "msg" / "attach" / chat_hash / "01" / "Img"
        attach_img_dir.mkdir(parents=True, exist_ok=True)
        dat_file = attach_img_dir / f"{md5_hex}_t.dat"
        dat_file.write_bytes(b"dat_payload")

        registry = AccountRegistry([account], acc_dir / "accounts.json")
        store = CoreStore(acc_dir / "core.sqlite")
        worker = AccountWorker(registry, store)

        liveness_path = acc_dir / "sync_liveness.json"
        loop = AccountSyncLoop(worker, interval_seconds=5.0, liveness_path=liveness_path)
        return worker, loop, liveness_path

    def test_t7_worker_cycle_with_numeric_chat_completes_without_incrementing_failed_cycles(self) -> None:
        """T7: an account-worker cycle containing numeric chat value completes without incrementing consecutive_failed_cycles."""
        worker, loop, liveness_path = self._setup_sync_environment("t7")
        try:
            # Mock refresh_decrypted and import_account so we run real sync_media
            mock_refresh = {"updated": ["message/message_0.db"], "skipped": [], "missing_key": [], "failed": []}
            mock_ingest = {"chats": 1, "messages": 1, "changed_rows": 1}
            mock_import = {"chats": 1, "messages": 1, "message_changes": 1}

            with patch("memory.decrypt_sync.refresh_decrypted", return_value=mock_refresh), \
                 patch("memory.memory_ingest.ingest_memory", return_value=mock_ingest), \
                 patch("core.account_worker.import_account", return_value=mock_import):
                result = worker.run_once()

            self.assertTrue(result["ok"], f"Cycle should be ok, but got: {result}")
            self.assertEqual(len(result["accounts"]), 1)
            acc_result = result["accounts"][0]
            self.assertTrue(acc_result["ok"])
            self.assertNotIn("error", acc_result)

            # Record cycle completed in loop liveness
            account_errors = loop._cycle_account_errors(result)
            self.assertEqual(account_errors, {})
            loop.liveness.record_cycle_completed(clean=not account_errors, account_errors=account_errors)
            loop.liveness.flush()

            snapshot = loop.liveness.snapshot()
            self.assertEqual(snapshot["consecutive_failed_cycles"], 0)
            self.assertEqual(snapshot["last_account_errors"], {})
        finally:
            worker.store.close()

    def test_t8_successful_cycles_restore_consecutive_failed_cycles_zero(self) -> None:
        """T8: successful cycles after historical failure condition restore worker health projection expected by H2."""
        worker, loop, liveness_path = self._setup_sync_environment("t8")
        try:
            # Simulate historical failure condition from the live incident:
            historical_exc = AttributeError("'int' object has no attribute 'encode'")
            loop.liveness.record_cycle_error(historical_exc)
            loop.liveness.flush()

            snapshot_before = loop.liveness.snapshot()
            self.assertEqual(snapshot_before["consecutive_failed_cycles"], 1)
            self.assertIn("AttributeError", snapshot_before["last_cycle_error"])

            # Now execute a clean cycle with the fix in place
            mock_refresh = {"updated": ["message/message_0.db"], "skipped": [], "missing_key": [], "failed": []}
            mock_ingest = {"chats": 1, "messages": 1, "changed_rows": 1}
            mock_import = {"chats": 1, "messages": 1, "message_changes": 1}

            with patch("memory.decrypt_sync.refresh_decrypted", return_value=mock_refresh), \
                 patch("memory.memory_ingest.ingest_memory", return_value=mock_ingest), \
                 patch("core.account_worker.import_account", return_value=mock_import):
                result = worker.run_once()

            self.assertTrue(result["ok"])
            account_errors = loop._cycle_account_errors(result)
            self.assertEqual(account_errors, {})
            loop.liveness.record_cycle_completed(clean=not account_errors, account_errors=account_errors)
            loop.liveness.flush()

            snapshot_after = loop.liveness.snapshot()
            # H2 health projection restored:
            self.assertEqual(
                snapshot_after["consecutive_failed_cycles"],
                0,
                "consecutive_failed_cycles must be restored to 0 after successful cycle",
            )
            self.assertEqual(snapshot_after["last_account_errors"], {})
            self.assertTrue(bool(snapshot_after["last_clean_cycle_at"]))
        finally:
            worker.store.close()


if __name__ == "__main__":
    unittest.main()
