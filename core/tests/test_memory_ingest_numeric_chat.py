#!/usr/bin/env python3
"""Regression test suite for SQLite numeric chat_username handling in memory ingest.

Covers T9-T19 required by Section 4 (M4) of FLASH_RC5_MEMORY_INGEST_HOTFIX2_AND_IDENTITY_RC9_TASKBOOK.md:
- T9:  load_name2id: integer 12345 produces the same Msg_<md5> key as "12345"
- T10: load_name2id: returned username value is textual "12345"
- T11: ingest_memory: Name2Id.user_name INTEGER completes without AttributeError
- T12: ingest_memory: resulting chats/messages chat_username is text-stable
- T13: load_contact_names: numeric username is normalized to textual dictionary key
- T14: load_sessions: numeric username is normalized and cannot crash .endswith()
- T15: bytes username: valid UTF-8 normalizes identically to equivalent str
- T16: invalid UTF-8 bytes: fail-closed, no silent identity substitution
- T17: AccountWorker cycle containing numeric Name2Id completes successfully
- T18: successful cycle resets consecutive_failed_cycles to 0 after prior failure
- T19: media_sync T1-T8 remain PASS on the same branch
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

from core.account_worker import AccountSyncLoop, AccountWorker, media_args  # noqa: E402
from core.registry import parse_account  # noqa: E402
from core.store import CoreStore  # noqa: E402
from memory import media_sync, memory_ingest  # noqa: E402
from core.tests.test_media_sync_numeric_chat import MediaSyncNumericChatTest  # noqa: E402


class MemoryIngestNumericChatTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = CORE_ROOT / ".tmp" / f"test-memory-numeric-{uuid.uuid4().hex}"
        self.root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        if self.root.exists():
            shutil.rmtree(self.root, ignore_errors=True)

    def _create_mock_message_db(self, db_path: Path, username_val: object) -> str:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(db_path)) as conn:
            conn.execute("CREATE TABLE Name2Id (user_name ANY)")
            conn.execute("INSERT INTO Name2Id (user_name) VALUES (?)", (username_val,))
            norm_user = memory_ingest.normalize_chat_username(username_val)
            table_name = f"Msg_{hashlib.md5(norm_user.encode('utf-8')).hexdigest()}"
            conn.execute(
                f"""
                CREATE TABLE [{table_name}] (
                    local_id INTEGER PRIMARY KEY,
                    server_id INTEGER,
                    local_type INTEGER,
                    sort_seq INTEGER,
                    real_sender_id TEXT,
                    create_time INTEGER,
                    status INTEGER,
                    upload_status INTEGER,
                    download_status INTEGER,
                    server_seq INTEGER,
                    origin_source TEXT,
                    source TEXT,
                    message_content TEXT,
                    compress_content BLOB,
                    packed_info_data BLOB,
                    WCDB_CT_message_content INTEGER
                )
                """
            )
            conn.execute(
                f"""
                INSERT INTO [{table_name}] (
                    local_id, server_id, local_type, sort_seq, real_sender_id,
                    create_time, status, upload_status, download_status, server_seq,
                    origin_source, source, message_content, compress_content,
                    packed_info_data, WCDB_CT_message_content
                ) VALUES (1, 1001, 1, 1, 'sender1', 1700000000, 1, 0, 0, 1, '', '', 'Hello world', NULL, NULL, NULL)
                """
            )
        return table_name

    def test_t9_load_name2id_numeric_produces_identical_msg_key(self) -> None:
        """T9: integer 12345 produces the exact same Msg_<md5> key as textual '12345'."""
        db_num = self.root / "t9_num.db"
        db_str = self.root / "t9_str.db"
        t_num = self._create_mock_message_db(db_num, 12345)
        t_str = self._create_mock_message_db(db_str, "12345")

        map_num = memory_ingest.load_name2id(db_num)
        map_str = memory_ingest.load_name2id(db_str)

        self.assertEqual(t_num, t_str)
        self.assertEqual(list(map_num.keys()), list(map_str.keys()))
        expected_table = f"Msg_{hashlib.md5(b'12345').hexdigest()}"
        self.assertIn(expected_table, map_num)
        self.assertEqual(map_num[expected_table], "12345")

    def test_t10_load_name2id_returned_value_is_textual_string(self) -> None:
        """T10: returned username value in mapping is strictly str, never int."""
        db_path = self.root / "t10.db"
        self._create_mock_message_db(db_path, 987654)

        mapping = memory_ingest.load_name2id(db_path)
        for tbl, uname in mapping.items():
            self.assertIsInstance(uname, str)
            self.assertEqual(uname, "987654")

    def test_t11_ingest_memory_with_integer_name2id_completes_without_error(self) -> None:
        """T11: ingest_memory with Name2Id containing INTEGER completes without AttributeError."""
        decrypted_dir = self.root / "decrypted"
        msg_dir = decrypted_dir / "message"
        msg_dir.mkdir(parents=True, exist_ok=True)
        db_path = msg_dir / "message_1.db"
        self._create_mock_message_db(db_path, 12345)

        memory_db = self.root / "memory.db"
        result = memory_ingest.ingest_memory(decrypted_dir, memory_db)

        self.assertEqual(result["chats"], 1)
        self.assertEqual(result["messages"], 1)

    def test_t12_ingest_memory_persisted_identifiers_are_text_stable(self) -> None:
        """T12: resulting chats.username and messages.chat_username are text-stable strings."""
        decrypted_dir = self.root / "decrypted_t12"
        msg_dir = decrypted_dir / "message"
        msg_dir.mkdir(parents=True, exist_ok=True)
        db_path = msg_dir / "message_1.db"
        self._create_mock_message_db(db_path, 88888)

        memory_db = self.root / "memory_t12.db"
        memory_ingest.ingest_memory(decrypted_dir, memory_db)

        with sqlite3.connect(str(memory_db)) as conn:
            chat_row = conn.execute("SELECT username FROM chats").fetchone()
            self.assertIsNotNone(chat_row)
            self.assertIsInstance(chat_row[0], str)
            self.assertEqual(chat_row[0], "88888")

            msg_row = conn.execute("SELECT chat_username FROM messages").fetchone()
            self.assertIsNotNone(msg_row)
            self.assertIsInstance(msg_row[0], str)
            self.assertEqual(msg_row[0], "88888")

    def test_t13_load_contact_names_numeric_username_normalized(self) -> None:
        """T13: numeric contact.username is normalized to textual dictionary key."""
        contact_db = self.root / "contact.db"
        with sqlite3.connect(str(contact_db)) as conn:
            conn.execute("CREATE TABLE contact (username ANY, remark TEXT, nick_name TEXT, alias TEXT, local_type INTEGER)")
            conn.execute("INSERT INTO contact VALUES (55555, 'Remark5', 'Nick5', 'alias5', 1)")

        contacts = memory_ingest.load_contact_names(contact_db)
        self.assertIn("55555", contacts)
        self.assertNotIn(55555, contacts)
        self.assertEqual(contacts["55555"]["display_name"], "Remark5")

    def test_t14_load_sessions_numeric_username_normalized(self) -> None:
        """T14: numeric SessionTable.username is normalized and does not crash .endswith()."""
        session_db = self.root / "session.db"
        with sqlite3.connect(str(session_db)) as conn:
            conn.execute(
                """
                CREATE TABLE SessionTable (
                    username ANY, type INTEGER, unread_count INTEGER, is_hidden INTEGER,
                    status INTEGER, last_timestamp INTEGER, sort_timestamp INTEGER,
                    last_msg_locald_id INTEGER, last_msg_type INTEGER,
                    last_msg_sub_type INTEGER, last_msg_sender TEXT
                )
                """
            )
            conn.execute("INSERT INTO SessionTable VALUES (77777, 1, 0, 0, 0, 1700000000, 1700000000, 1, 1, 0, '')")

        sessions = memory_ingest.load_sessions(session_db)
        self.assertIn("77777", sessions)
        self.assertNotIn(77777, sessions)

    def test_t15_bytes_username_strict_utf8_normalizes_identically(self) -> None:
        """T15: bytes username with valid UTF-8 normalizes identically to equivalent str."""
        raw_bytes = b"wxid_test123"
        norm = memory_ingest.normalize_chat_username(raw_bytes)
        self.assertEqual(norm, "wxid_test123")

    def test_t16_invalid_utf8_bytes_fail_closed(self) -> None:
        """T16: invalid UTF-8 bytes raise UnicodeDecodeError (fail-closed, no silent corruption)."""
        invalid_bytes = b"\xff\xfe\xfd"
        with self.assertRaises(UnicodeDecodeError):
            memory_ingest.normalize_chat_username(invalid_bytes)

    def _setup_worker_env(self, name: str, numeric_user: object) -> tuple[AccountWorker, AccountSyncLoop, Path]:
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

        msg_dir = account.decrypted_dir / "message"
        msg_dir.mkdir(parents=True, exist_ok=True)
        self._create_mock_message_db(msg_dir / "message_1.db", numeric_user)

        contact_dir = account.decrypted_dir / "contact"
        contact_dir.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(contact_dir / "contact.db")) as conn:
            conn.execute("CREATE TABLE contact (username ANY, remark TEXT, nick_name TEXT, alias TEXT, local_type INTEGER)")
            conn.execute("INSERT INTO contact VALUES (?, 'User Display', '', '', 1)", (numeric_user,))

        session_dir = account.decrypted_dir / "session"
        session_dir.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(str(session_dir / "session.db")) as conn:
            conn.execute(
                """
                CREATE TABLE SessionTable (
                    username ANY, type INTEGER, unread_count INTEGER, is_hidden INTEGER,
                    status INTEGER, last_timestamp INTEGER, sort_timestamp INTEGER,
                    last_msg_locald_id INTEGER, last_msg_type INTEGER,
                    last_msg_sub_type INTEGER, last_msg_sender TEXT
                )
                """
            )
            conn.execute("INSERT INTO SessionTable VALUES (?, 1, 0, 0, 0, 1700000000, 1700000000, 1, 1, 0, '')", (numeric_user,))

        from core.registry import AccountRegistry
        registry = AccountRegistry([account], acc_dir / "accounts.json")
        store = CoreStore(acc_dir / "core.sqlite")
        worker = AccountWorker(registry, store)
        liveness_path = acc_dir / "sync_liveness.json"
        loop = AccountSyncLoop(worker, interval_seconds=5.0, liveness_path=liveness_path)
        return worker, loop, liveness_path

    def test_t17_account_worker_cycle_with_numeric_name2id_succeeds(self) -> None:
        """T17: AccountWorker cycle containing numeric Name2Id completes successfully."""
        worker, loop, liveness_path = self._setup_worker_env("t17", 12345)
        mock_refresh = {"updated": ["message/message_1.db"], "skipped": [], "missing_key": [], "failed": []}
        mock_media = {"ok": True, "images": 0}
        mock_import = {"chats": 1, "messages": 1, "message_changes": 1}

        with patch("memory.decrypt_sync.refresh_decrypted", return_value=mock_refresh), \
             patch("memory.media_sync.sync_media", return_value=mock_media), \
             patch("core.account_worker.import_account", return_value=mock_import):
            result = worker.run_once()

        self.assertTrue(result["ok"], f"Cycle should be ok, but got: {result}")
        self.assertEqual(len(result["accounts"]), 1)
        acc_result = result["accounts"][0]
        self.assertTrue(acc_result["ok"])
        self.assertNotIn("error", acc_result)

        # Confirm liveness records clean cycle with 0 consecutive failures
        account_errors = loop._cycle_account_errors(result)
        self.assertEqual(account_errors, {})
        loop.liveness.record_cycle_completed(clean=True, account_errors=account_errors)
        loop.liveness.flush()
        snapshot = loop.liveness.snapshot()
        self.assertEqual(snapshot["consecutive_failed_cycles"], 0)
        self.assertEqual(snapshot["last_cycle_error"], "")

    def test_t18_successful_cycle_resets_consecutive_failed_cycles(self) -> None:
        """T18: successful cycle resets consecutive_failed_cycles to 0 after prior failure."""
        worker, loop, liveness_path = self._setup_worker_env("t18", 99999)
        # Simulate prior failure condition
        loop.liveness.record_cycle_completed(
            clean=False,
            account_errors={"acc_t18": "AttributeError: 'int' object has no attribute 'encode'"}
        )
        self.assertGreater(loop.liveness.snapshot()["consecutive_failed_cycles"], 0)

        mock_refresh = {"updated": ["message/message_1.db"], "skipped": [], "missing_key": [], "failed": []}
        mock_media = {"ok": True, "images": 0}
        mock_import = {"chats": 1, "messages": 1, "message_changes": 1}

        with patch("memory.decrypt_sync.refresh_decrypted", return_value=mock_refresh), \
             patch("memory.media_sync.sync_media", return_value=mock_media), \
             patch("core.account_worker.import_account", return_value=mock_import):
            result = worker.run_once()

        self.assertTrue(result["ok"])
        account_errors = loop._cycle_account_errors(result)
        self.assertEqual(account_errors, {})
        loop.liveness.record_cycle_completed(clean=True, account_errors=account_errors)
        loop.liveness.flush()

        snapshot = loop.liveness.snapshot()
        self.assertEqual(snapshot["consecutive_failed_cycles"], 0)
        self.assertEqual(snapshot["last_cycle_error"], "")

    def test_t19_media_sync_t1_to_t8_remain_pass_on_same_branch(self) -> None:
        """T19: media_sync numeric chat regression test suite (T1-T8) remains 100% PASS."""
        suite = unittest.TestLoader().loadTestsFromTestCase(MediaSyncNumericChatTest)
        runner = unittest.TextTestRunner(verbosity=0)
        result = runner.run(suite)
        self.assertEqual(result.testsRun, 8)
        self.assertTrue(result.wasSuccessful())


if __name__ == "__main__":
    unittest.main()
