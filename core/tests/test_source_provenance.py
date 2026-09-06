"""Mandatory regression tests for the RB-003 release-lineage provenance port.

Covers all 8 scenarios from FLASH_RC5_RB003_FINAL_CLOSURE_TASKBOOK Section 3
(Flash-RB3-E):
1. matching single source PASS;
2. stale directory with newer mtime never wins;
3. missing current wxid + historical source -> FAIL CLOSED;
4. testB-style regression: wxid_rpfflqttdz4a22_7fcd current vs
   wxid_yx40oh06ya1322_7b57 historical;
5. key-export account_dir mismatch -> fail closed;
6. one account mismatch does not affect its peer;
7. no business projection from a rejected source;
8. legacy / non-AgentWechat behavior unchanged.
"""

from __future__ import annotations

import gc
import json
import os
import sqlite3
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

CORE_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CORE_ROOT))

_TEST_GUI_LEASE_DIR = tempfile.TemporaryDirectory()
os.environ.setdefault("WECHAT_GUI_LEASE_DIR", _TEST_GUI_LEASE_DIR.name)

from core.account_worker import AccountWorker
from core.key_extract import import_agent_wechat_keys
from core.normalize import import_account
from core.registry import AccountConfig, AccountRegistry, parse_account
from core.runtime_bridge import discover_agent_wechat_source_db, discover_source_db, resolve_runtime_account
from core.source_provenance import SourceIdentityError, valid_wxid, wechat_data_dir_name
from core.store import CoreStore
from memory.memory_ingest import init_memory_db

WXID_A = "wxid_alpha_1111"
WXID_B = "wxid_bravo_2222"
# The exact identities observed in the deployed RB-003 incident (testB):
WXID_CURRENT = "wxid_rpfflqttdz4a22_7fcd"
WXID_HISTORICAL = "wxid_yx40oh06ya1322_7b57"


def _status_file(root: Path, name: str, logged_in_user: str) -> Path:
    status_file = root / name
    status_file.write_text(
        json.dumps({"running": True, "logged_in_user": logged_in_user}),
        encoding="utf-8",
    )
    return status_file


def _agent_account(root: Path, account_id: str, home: Path, status_file: Path) -> AccountConfig:
    return parse_account(
        {
            "account_id": account_id,
            "display_name": account_id,
            "runtime": {
                "runtime_provider": "agent_wechat",
                "runtime_bridge": True,
                "source_home": str(home),
                "runtime_status_file": str(status_file),
            },
        },
        root=root,
    )


class SourceProvenanceReleaseLineageTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = tempfile.TemporaryDirectory(ignore_cleanup_errors=True)
        self.root = Path(self.temp_dir.name)
        self.store = CoreStore(self.root / "core.db")

    def tearDown(self) -> None:
        self.store.close()
        gc.collect()
        try:
            self.temp_dir.cleanup()
        except OSError:
            pass

    def _create_mock_staging_db(self, memory_db: Path) -> None:
        memory_db.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(memory_db) as conn:
            init_memory_db(conn)
            conn.execute(
                "INSERT INTO chats (username, display_name, is_group, updated_at) VALUES (?, ?, ?, ?)",
                ("chat-1", "Chat One", 0, "2026-09-06T00:00:00Z"),
            )
            conn.execute(
                """
                INSERT INTO messages (
                    message_uid, source_identity, chat_username, chat_display_name,
                    message_table, source_message_db, local_id, server_id,
                    type_label, message_content, create_time, origin_source,
                    content_sha256, ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "msg-1", "src-1", "chat-1", "Chat One",
                    "MSG1", "message_0.db", 1, 1001,
                    "text", "hello", 1788652800, 0,
                    "fake-sha256", "2026-09-06T00:00:00Z",
                ),
            )
            conn.commit()

    def _create_mock_contact_db(self, decrypted_dir: Path) -> None:
        contact_db = decrypted_dir / "contact" / "contact.db"
        contact_db.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(contact_db) as conn:
            conn.execute(
                """
                CREATE TABLE contact (
                    username TEXT PRIMARY KEY,
                    remark TEXT,
                    nick_name TEXT,
                    alias TEXT
                )
                """
            )
            conn.execute(
                "INSERT INTO contact VALUES (?, ?, ?, ?)",
                ("peer-1", "Remark One", "Nick One", "alias1"),
            )
            conn.commit()

    def _core_table_count(self, table: str, account_id: str) -> int:
        with self.store.connection() as conn:
            row = conn.execute(
                f"SELECT COUNT(*) AS n FROM {table} WHERE account_id=?",
                (account_id,),
            ).fetchone()
        return int(row["n"])

    # 1. matching single source -> exact selection, provenance PASS
    def test_01_matching_single_source_selects_logged_in_user(self) -> None:
        home = self.root / "home_a"
        target_db = home / "Documents" / "xwechat_files" / WXID_A / "db_storage"
        target_db.mkdir(parents=True)

        account = _agent_account(
            self.root,
            "acc_a",
            home,
            _status_file(self.root, "status_a.json", WXID_A),
        )

        resolved = resolve_runtime_account(account)
        self.assertEqual(resolved.source_db_dir, target_db)
        self.assertEqual(resolved.wechat_base_dir, target_db.parent)
        self.assertEqual(wechat_data_dir_name(resolved.source_db_dir), WXID_A)
        self.assertTrue(valid_wxid(WXID_A))

    # 2. stale directory with newer mtime must never win
    def test_02_stale_dir_with_newer_mtime_never_wins(self) -> None:
        home = self.root / "home_multi"
        current_db = home / "xwechat_files" / WXID_A / "db_storage"
        stale_db = home / "xwechat_files" / WXID_B / "db_storage"
        current_db.mkdir(parents=True)
        stale_db.mkdir(parents=True)

        # The stale/historical account directory carries a newer directory
        # mtime (scan/backup/attribute churn), the RB-003 incident trigger.
        future_mtime = time.time() + 1000
        os.utime(stale_db, (future_mtime, future_mtime))
        os.utime(stale_db.parent, (future_mtime, future_mtime))

        account = _agent_account(
            self.root,
            "acc_a",
            home,
            _status_file(self.root, "status_multi.json", WXID_A),
        )

        resolved = resolve_runtime_account(account)
        self.assertEqual(resolved.source_db_dir, current_db)
        self.assertEqual(resolved.wechat_base_dir, current_db.parent)
        self.assertNotEqual(wechat_data_dir_name(resolved.source_db_dir), WXID_B)

        # Direct discovery agrees, and never returns the stale wxid.
        discovered = discover_agent_wechat_source_db(home, WXID_A, account_id="acc_a")
        self.assertIsNotNone(discovered)
        assert discovered is not None
        self.assertEqual(wechat_data_dir_name(discovered[0]), WXID_A)

    # 3. current wxid missing while historical wxid exists -> FAIL CLOSED
    def test_03_current_wxid_missing_with_historical_source_fails_closed(self) -> None:
        home = self.root / "home_stale_only"
        (home / "xwechat_files" / WXID_B / "db_storage").mkdir(parents=True)

        account = _agent_account(
            self.root,
            "acc_a",
            home,
            _status_file(self.root, "status_stale.json", WXID_A),
        )

        with self.assertRaises(SourceIdentityError) as ctx:
            resolve_runtime_account(account)

        err = ctx.exception
        self.assertEqual(err.code, "source_identity_mismatch")
        self.assertEqual(err.status, 409)
        self.assertEqual(err.details["expected_wxid"], WXID_A)
        self.assertIn(WXID_B, err.details["found_wxids"])

        # No source is resolved, so nothing can be decrypted or ingested.
        with self.assertRaises(SourceIdentityError):
            discover_agent_wechat_source_db(home, WXID_A, account_id="acc_a")

    # 4. testB regression: current wxid_rpff... must beat historical wxid_yx40...
    def test_04_testb_current_rpff_never_loses_to_historical_yx40(self) -> None:
        home = self.root / "home_testb"
        current_db = home / "xwechat_files" / WXID_CURRENT / "db_storage"
        historical_db = home / "xwechat_files" / WXID_HISTORICAL / "db_storage"
        current_db.mkdir(parents=True)
        historical_db.mkdir(parents=True)

        # Deployed incident shape: the historical wxid directory looks newer.
        future_mtime = time.time() + 5000
        os.utime(historical_db, (future_mtime, future_mtime))
        os.utime(historical_db.parent, (future_mtime, future_mtime))

        account = _agent_account(
            self.root,
            "testb",
            home,
            _status_file(self.root, "status_testb.json", WXID_CURRENT),
        )

        resolved = resolve_runtime_account(account)
        self.assertEqual(resolved.source_db_dir, current_db)
        self.assertEqual(wechat_data_dir_name(resolved.source_db_dir), WXID_CURRENT)

        # Symmetry: selection is identity-driven, not mtime-driven.  When the
        # fresh login is the historical wxid, the newer rpff dir must not win.
        account_swapped = _agent_account(
            self.root,
            "testb-swapped",
            home,
            _status_file(self.root, "status_testb_swapped.json", WXID_HISTORICAL),
        )
        resolved_swapped = resolve_runtime_account(account_swapped)
        self.assertEqual(resolved_swapped.source_db_dir, historical_db)
        self.assertEqual(wechat_data_dir_name(resolved_swapped.source_db_dir), WXID_HISTORICAL)

    # 5. key-export account_dir mismatch -> fail closed, no credential borrowing
    def test_05_key_export_account_dir_mismatch_fails_closed(self) -> None:
        home = self.root / "home_keys"
        source_a = home / "xwechat_files" / WXID_A / "db_storage"
        source_b = home / "xwechat_files" / WXID_B / "db_storage"
        (source_a / "session").mkdir(parents=True)
        (source_b / "session").mkdir(parents=True)

        dummy_session = b"SQLite format 3\x00" + b"\x00" * 100
        (source_a / "session" / "session.db").write_bytes(dummy_session)
        (source_b / "session" / "session.db").write_bytes(dummy_session)

        credentials = [
            {
                "account_dir": WXID_A,
                "db_name": "session.db",
                "hex_key": "aa" * 32,
                "verified_at": "2026-09-06T00:00:00Z",
            }
        ]

        # Matching case: selected source and key account_dir agree.
        account_a = parse_account(
            {
                "account_id": "acc_a",
                "display_name": "Account A",
                "source_db_dir": str(source_a),
                "runtime_dir": str(self.root / "runtime" / "acc_a"),
            },
            root=self.root,
        )
        res_a = import_agent_wechat_keys(account_a, credentials=credentials)
        self.assertEqual(res_a["returncode"], 0)
        self.assertEqual(res_a["account_dir"], WXID_A)
        self.assertEqual(
            res_a["account_dir"],
            wechat_data_dir_name(str(account_a.source_db_dir)),
        )
        self.assertTrue(account_a.keys_file.is_file())

        # Mismatch case: source is WXID_B but only WXID_A credentials exist.
        # The old len(available)==1 fallback borrowed the wrong credential.
        account_b = parse_account(
            {
                "account_id": "acc_b",
                "display_name": "Account B",
                "source_db_dir": str(source_b),
                "runtime_dir": str(self.root / "runtime" / "acc_b"),
            },
            root=self.root,
        )
        res_b = import_agent_wechat_keys(account_b, credentials=credentials)
        self.assertEqual(res_b["returncode"], 2)
        self.assertIn("not in stored credentials", str(res_b["error"]))
        self.assertEqual(res_b["account_dir"], WXID_B)
        self.assertFalse(account_b.keys_file.is_file())

    # 6. one account mismatch does not affect its peer
    def test_06_one_account_mismatch_does_not_affect_peer(self) -> None:
        registry_file = self.root / "accounts.json"

        home_a = self.root / "home_peer_a"
        # Account A fail-closes: fresh login WXID_A, only historical WXID_B exists.
        (home_a / "xwechat_files" / WXID_B / "db_storage").mkdir(parents=True)
        status_a = _status_file(self.root, "status_peer_a.json", WXID_A)

        home_b = self.root / "home_peer_b"
        # Account B is healthy and matching.
        source_b = home_b / "xwechat_files" / WXID_B / "db_storage"
        source_b.mkdir(parents=True)
        status_b = _status_file(self.root, "status_peer_b.json", WXID_B)

        reg_data = [
            {
                "account_id": "acc_a",
                "display_name": "Account A",
                "runtime": {
                    "runtime_provider": "agent_wechat",
                    "runtime_bridge": True,
                    "source_home": str(home_a),
                    "runtime_status_file": str(status_a),
                },
            },
            {
                "account_id": "acc_b",
                "display_name": "Account B",
                "runtime": {
                    "runtime_provider": "agent_wechat",
                    "runtime_bridge": True,
                    "source_home": str(home_b),
                    "runtime_status_file": str(status_b),
                },
            },
        ]
        registry_file.write_text(json.dumps(reg_data), encoding="utf-8")
        registry = AccountRegistry(
            [parse_account(item, root=self.root) for item in reg_data],
            registry_file,
        )
        worker = AccountWorker(registry, self.store)

        self.store.upsert_account("acc_a", "Account A", state="online")
        self.store.upsert_account("acc_b", "Account B", state="online")

        acc_b_cfg = registry.require("acc_b")
        self._create_mock_staging_db(acc_b_cfg.memory_db)
        self._create_mock_contact_db(acc_b_cfg.decrypted_dir)

        def _mock_key_extract(acc, root=None):
            acc.keys_file.parent.mkdir(parents=True, exist_ok=True)
            acc.keys_file.write_text("{}", encoding="utf-8")
            return {"returncode": 0}

        with patch("memory.decrypt_sync.refresh_decrypted", return_value={"updated": [], "skipped": [], "missing_key": [], "failed": []}), patch(
            "memory.media_sync.sync_media", return_value={}
        ), patch("memory.memory_ingest.ingest_memory", return_value={}), patch(
            "core.key_extract.extract_account_keys", side_effect=_mock_key_extract
        ):
            result = worker.run_once()

        accounts_result = {res["account_id"]: res for res in result["accounts"]}
        self.assertEqual(len(accounts_result), 2)

        # Account A failed closed, visibly, without killing the cycle.
        self.assertFalse(accounts_result["acc_a"]["ok"])
        self.assertEqual(
            accounts_result["acc_a"]["source_provenance"]["code"],
            "source_identity_mismatch",
        )
        self.assertEqual(self.store.account("acc_a")["state"], "error")

        # Account B completed its full cycle and projected its business rows.
        self.assertTrue(accounts_result["acc_b"]["ok"])
        self.assertEqual(self._core_table_count("chats", "acc_b"), 1)
        self.assertEqual(self._core_table_count("messages", "acc_b"), 1)
        self.assertEqual(self._core_table_count("contacts", "acc_b"), 1)

    # 7. no business projection from a rejected source
    def test_07_no_business_projection_from_rejected_source(self) -> None:
        registry_file = self.root / "accounts.json"
        home = self.root / "home_reject"
        wrong_db = home / "xwechat_files" / WXID_B / "db_storage"
        wrong_db.mkdir(parents=True)

        reg_data = [
            {
                "account_id": "acc_reject",
                "display_name": "Account Reject",
                "source_db_dir": str(wrong_db),
                "runtime_dir": str(self.root / "runtime" / "acc_reject"),
                "runtime": {"logged_in_user": WXID_A},
            },
        ]
        registry_file.write_text(json.dumps(reg_data), encoding="utf-8")
        registry = AccountRegistry(
            [parse_account(item, root=self.root) for item in reg_data],
            registry_file,
        )
        worker = AccountWorker(registry, self.store)

        self.store.upsert_account("acc_reject", "Account Reject", state="online")
        account = registry.require("acc_reject")
        self._create_mock_staging_db(account.memory_db)
        self._create_mock_contact_db(account.decrypted_dir)

        # If any decrypt/ingest stage were reached against the rejected source,
        # these mocks would make the test fail by being invoked.
        refresh_mock = MagicMock(return_value={"updated": [], "skipped": [], "missing_key": [], "failed": []})
        ingest_mock = MagicMock(return_value={})
        media_mock = MagicMock(return_value={})

        with patch("memory.decrypt_sync.refresh_decrypted", refresh_mock), patch(
            "memory.media_sync.sync_media", media_mock
        ), patch("memory.memory_ingest.ingest_memory", ingest_mock):
            status = worker.run_account(account)

        self.assertFalse(status["ok"])
        self.assertEqual(status["source_provenance"]["code"], "source_identity_mismatch")
        self.assertEqual(status["source_provenance"]["selected_wxid"], WXID_B)
        self.assertEqual(status["source_provenance"]["expected_wxid"], WXID_A)
        refresh_mock.assert_not_called()
        ingest_mock.assert_not_called()
        media_mock.assert_not_called()

        # No chats/messages/contacts rows reached Core for the rejected source.
        self.assertEqual(self._core_table_count("chats", "acc_reject"), 0)
        self.assertEqual(self._core_table_count("messages", "acc_reject"), 0)
        self.assertEqual(self._core_table_count("contacts", "acc_reject"), 0)
        self.assertEqual(self.store.account("acc_reject")["state"], "error")

        # Defense in depth: a direct normalize import is also blocked.
        with self.assertRaises(SourceIdentityError) as ctx:
            import_account(account, self.store)
        self.assertEqual(ctx.exception.code, "source_identity_mismatch")
        self.assertEqual(self._core_table_count("chats", "acc_reject"), 0)

    # 8. legacy / non-AgentWechat behavior unchanged
    def test_08_legacy_and_non_agent_wechat_behavior_unchanged(self) -> None:
        home = self.root / "home_legacy"
        db_1 = home / "Documents" / "xwechat_files" / "wxid_legacy1" / "db_storage"
        db_2 = home / "Documents" / "xwechat_files" / "wxid_legacy2" / "db_storage"
        db_1.mkdir(parents=True)
        db_2.mkdir(parents=True)

        now = time.time()
        os.utime(db_1, (now, now))
        os.utime(db_2, (now + 50, now + 50))

        # 8a. AgentWechat without a fresh logged_in_user keeps the documented
        # legacy mtime selection.
        discovered = discover_agent_wechat_source_db(home, logged_in_user="", account_id="acc_legacy")
        self.assertIsNotNone(discovered)
        assert discovered is not None
        self.assertEqual(discovered[0], db_2)

        # 8b. A non-wxid logged_in_user value also keeps legacy behavior.
        discovered_invalid = discover_agent_wechat_source_db(home, logged_in_user="wx", account_id="acc_legacy")
        self.assertIsNotNone(discovered_invalid)
        assert discovered_invalid is not None
        self.assertEqual(discovered_invalid[0], db_2)

        # 8c. The pure legacy discovery helper is untouched.
        legacy_discovered = discover_source_db(home)
        self.assertIsNotNone(legacy_discovered)
        assert legacy_discovered is not None
        self.assertEqual(legacy_discovered[0], db_2)

        # 8d. A non-agent_wechat provider account still resolves via the
        # legacy mtime path (no provenance gate, no behavior change).
        account = parse_account(
            {
                "account_id": "acc_legacy",
                "display_name": "Legacy Account",
                "runtime": {
                    "runtime_bridge": True,
                    "source_home": str(home),
                },
            },
            root=self.root,
        )
        self.assertEqual(account.runtime_provider, "legacy")
        resolved = resolve_runtime_account(account)
        self.assertEqual(resolved.source_db_dir, db_2)


if __name__ == "__main__":
    unittest.main()
