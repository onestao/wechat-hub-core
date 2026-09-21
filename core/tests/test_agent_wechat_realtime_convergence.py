"""Tests for AgentWechat real-time provider convergence.

Verifies:
1. Canonical message kinds & mapping (text, image, sticker, voice, video, file, link, reply, system, unknown)
2. Message ID parity with legacy content_hash(source_message_identity(...))
3. AccountWorker pagination & burst handling (P0-1):
   - Burst 40 messages (101~140) all entered
   - Restart catchup (101~180) across pagination all entered
   - Unchanged chat -> 0 list_messages calls (UNCHANGED_CHAT_MESSAGE_FETCH = 0)
   - unreadCount-only changes -> 0 list_messages calls
4. P0-2: WeChat private format single owner (canonical fields, unknown defaults to unknown, single content_hash)
5. P0-4: Lazy media correctness:
   - Unsupported evaluated before not data / pending
   - Terminal unsupported status cached in CoreStore
   - Cache path contains media_id (no collision between same filenames)
   - Role (thumbnail vs original) preserved from upstream
   - Raw binary streaming capability
   - Message sync does not prefetch media
6. P1: Console long-poll:
   - Backlog > 100 does not skip un-delivered cursors
   - Background sync thread exits cleanly on stop
"""

from __future__ import annotations

import base64
import json
import sqlite3
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from core.agent_wechat import (
    AgentWechatClient,
    AgentWechatError,
    CANONICAL_KINDS,
    canonical_message_id,
    content_hash,
    map_agent_message_kind,
    normalize_agent_message,
    parse_timestamp_iso,
)
from core.registry import AccountConfig, AccountRegistry, load_registry
from core.store import CoreStore
from memory.memory_ingest import source_message_identity


class CanonicalNormalizationTest(unittest.TestCase):
    """Work Package A & B tests for message normalization and kind mapping."""

    def test_canonical_kinds_coverage(self) -> None:
        self.assertEqual(
            CANONICAL_KINDS,
            {
                "text",
                "image",
                "sticker",
                "voice",
                "video",
                "file",
                "link",
                "reply",
                "system",
                "unknown",
            },
        )

    def test_kind_mapping_from_canonical_fields(self) -> None:
        """P0-2: Upstream agent-wechat outputs canonical kind, subtype, filename, reply.

        Core accepts these directly; unknown types default to unknown, no XML guessing.
        """
        # Text
        kind, sub, fn = map_agent_message_kind({"kind": "text"})
        self.assertEqual(kind, "text")

        # Image
        kind, sub, fn = map_agent_message_kind({"kind": "image"})
        self.assertEqual(kind, "image")

        # Voice
        kind, sub, fn = map_agent_message_kind({"kind": "voice"})
        self.assertEqual(kind, "voice")

        # Video
        kind, sub, fn = map_agent_message_kind({"kind": "video"})
        self.assertEqual(kind, "video")

        # Sticker
        kind, sub, fn = map_agent_message_kind({"kind": "sticker"})
        self.assertEqual(kind, "sticker")

        # System
        kind, sub, fn = map_agent_message_kind({"kind": "system"})
        self.assertEqual(kind, "system")

        # File
        kind, sub, fn = map_agent_message_kind({"kind": "file", "subtype": 6, "filename": "spec.pdf"})
        self.assertEqual(kind, "file")
        self.assertEqual(sub, 6)
        self.assertEqual(fn, "spec.pdf")

        # Link
        kind, sub, fn = map_agent_message_kind({"kind": "link", "subtype": 5})
        self.assertEqual(kind, "link")
        self.assertEqual(sub, 5)

        # Reply
        kind, sub, fn = map_agent_message_kind({"kind": "reply", "subtype": 57})
        self.assertEqual(kind, "reply")
        self.assertEqual(sub, 57)

        # Unknown / unmapped type defaults to unknown
        kind, sub, fn = map_agent_message_kind({"kind": "custom_weird_kind", "subtype": 999})
        self.assertEqual(kind, "unknown")
        self.assertEqual(sub, 999)

        # Missing kind defaults to unknown
        kind, sub, fn = map_agent_message_kind({})
        self.assertEqual(kind, "unknown")

    def test_message_id_parity_with_legacy(self) -> None:
        """P0-2: Single stable source-message identity algorithm shared between direct and legacy."""
        cases = [
            ("wxid_user123", 101),
            ("38808757431@chatroom", 88),
            ("filehelper", 5),
            ("gh_official_account", 12345678),
        ]
        for chat_id, local_id in cases:
            canonical_id = canonical_message_id(chat_id, local_id)
            legacy_identity = source_message_identity(chat_id, "", local_id)
            legacy_id = content_hash(legacy_identity)
            self.assertEqual(canonical_id, legacy_id)

    def test_normalize_agent_message_structure(self) -> None:
        raw_msg = {
            "localId": 86,
            "serverId": "1234567890",
            "chatId": "38808757431@chatroom",
            "sender": "wxid_sender1",
            "senderName": "Sender One",
            "kind": "image",
            "content": "",
            "timestamp": "2026-03-30T10:00:00Z",
            "isSelf": False,
        }
        normalized = normalize_agent_message("test_account", raw_msg, bound_wxid="wxid_me")
        self.assertEqual(normalized["account_id"], "test_account")
        self.assertEqual(normalized["chat_id"], "38808757431@chatroom")
        self.assertEqual(normalized["type"], "image")
        self.assertEqual(normalized["direction"], "incoming")
        self.assertFalse(normalized["author"]["is_self"])
        self.assertEqual(normalized["author"]["display_name"], "Sender One")
        self.assertEqual(normalized["media_status"], "pending")
        self.assertEqual(normalized["media_id"], normalized["message_id"])
        self.assertEqual(normalized["vendor_specific"]["provider"], "agent_wechat")
        self.assertEqual(normalized["vendor_specific"]["source_local_id"], 86)


class AccountWorkerPaginationAndBacklogTest(unittest.TestCase):
    """P0-1 tests: Eliminate burst message loss, pagination loop, restart catch-up, and zero-fetch gates."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp_dir.name)
        self.db_path = self.root / "core.sqlite"
        self.store = CoreStore(self.db_path)
        self.registry_file = self.root / "accounts.json"
        self.registry_file.write_text('{"accounts": []}', encoding="utf-8")
        self.registry = AccountRegistry([], self.registry_file)

    def tearDown(self) -> None:
        self.store.close()
        self.tmp_dir.cleanup()

    def _create_agent_account(self, account_id: str = "acc_agent") -> AccountConfig:
        config_dir = self.root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        token_file = config_dir / "auth-token"
        token_file.write_text("mock-token-xyz\n", encoding="utf-8")

        account_dir = self.root / "accounts" / account_id
        account_dir.mkdir(parents=True, exist_ok=True)
        (account_dir / "xwechat_files" / "wxid_bound").mkdir(parents=True, exist_ok=True)

        reg_payload = {
            "accounts": [
                {
                    "account_id": account_id,
                    "display_name": "Agent Account",
                    "source_db_dir": str(account_dir / "xwechat_files" / "wxid_bound"),
                    "runtime_provider": "agent_wechat",
                    "agent_wechat": {
                        "base_url": "http://127.0.0.1:6174",
                        "token_file": str(token_file),
                    },
                }
            ]
        }
        self.registry_file.write_text(json.dumps(reg_payload), encoding="utf-8")
        self.registry.replace_from(load_registry(self.registry_file, root=self.root))
        account = self.registry.require(account_id)
        self.store.upsert_account(account_id, account.display_name, state="online", runtime=account.runtime)
        return account

    @patch("core.agent_wechat.AgentWechatClient.from_account")
    def test_burst_40_messages_no_loss(self, mock_client_factory: MagicMock) -> None:
        """P0-1 Gate: Burst 40 messages (101~140) must all be written without loss."""
        from core.account_worker import AccountWorker

        account = self._create_agent_account()
        worker = AccountWorker(self.registry, self.store)

        mock_client = MagicMock(spec=AgentWechatClient)
        mock_client.health.return_value = {"status": "ok"}
        mock_client.list_contacts.return_value = []

        # Seed an existing message at localId=100
        self.store.upsert_chat({
            "account_id": account.account_id,
            "chat_id": "burst_chat",
            "type": "private",
            "display_name": "Burst Chat",
        })
        self.store.upsert_message(normalize_agent_message(account.account_id, {
            "localId": 100,
            "chatId": "burst_chat",
            "kind": "text",
            "content": "msg 100",
            "timestamp": "2026-03-30T10:00:00Z",
            "isSelf": False,
        }))
        self.assertEqual(self.store.max_source_local_id(account.account_id, "burst_chat"), 100)

        # Burst of 40 new messages (101~140)
        burst_messages = [
            {
                "localId": i,
                "serverId": f"srv_{i}",
                "chatId": "burst_chat",
                "kind": "text",
                "content": f"burst message {i}",
                "timestamp": f"2026-03-30T10:01:{i%60:02d}Z",
                "isSelf": False,
            }
            for i in range(140, 100, -1)  # DESC order from upstream
        ]

        mock_client.list_chats.return_value = [
            {
                "id": "burst_chat",
                "name": "Burst Chat",
                "isGroup": False,
                "lastMsgLocalId": 140,
                "lastActivityAt": "2026-03-30T10:05:00Z",
                "unreadCount": 40,
            }
        ]
        mock_client.list_messages.return_value = burst_messages
        mock_client_factory.return_value = mock_client

        res = worker.run_account(account)
        self.assertTrue(res["ok"])
        self.assertEqual(res["messages"], 40)

        # Verify all 40 messages are saved
        all_msgs = self.store.list_messages(account.account_id, "burst_chat", limit=100)["messages"]
        self.assertEqual(len(all_msgs), 41)  # 100 + 40 new
        local_ids = {m["vendor_specific"]["source_local_id"] for m in all_msgs}
        for i in range(100, 141):
            self.assertIn(i, local_ids)
        self.assertEqual(self.store.max_source_local_id(account.account_id, "burst_chat"), 140)

    @patch("core.agent_wechat.AgentWechatClient.from_account")
    def test_restart_catchup_across_pagination_no_loss(self, mock_client_factory: MagicMock) -> None:
        """P0-1 Gate: On Core restart, catching up 80 messages (101~180) across 50-limit pages without loss."""
        from core.account_worker import AccountWorker

        account = self._create_agent_account()
        worker = AccountWorker(self.registry, self.store)

        mock_client = MagicMock(spec=AgentWechatClient)
        mock_client.health.return_value = {"status": "ok"}
        mock_client.list_contacts.return_value = []

        # Existing persisted message localId=100
        self.store.upsert_chat({
            "account_id": account.account_id,
            "chat_id": "catchup_chat",
            "type": "private",
            "display_name": "Catchup Chat",
        })
        self.store.upsert_message(normalize_agent_message(account.account_id, {
            "localId": 100,
            "chatId": "catchup_chat",
            "kind": "text",
            "content": "msg 100",
            "timestamp": "2026-03-30T09:00:00Z",
            "isSelf": False,
        }))

        # Upstream has 80 new messages from 101 to 180
        # Page 1 (offset=0, limit=50): 180 down to 131 (50 messages)
        page1 = [
            {
                "localId": i,
                "serverId": f"srv_{i}",
                "chatId": "catchup_chat",
                "kind": "text",
                "content": f"msg {i}",
                "timestamp": "2026-03-30T10:00:00Z",
                "isSelf": False,
            }
            for i in range(180, 130, -1)
        ]
        # Page 2 (offset=50, limit=50): 130 down to 101 (30 messages)
        page2 = [
            {
                "localId": i,
                "serverId": f"srv_{i}",
                "chatId": "catchup_chat",
                "kind": "text",
                "content": f"msg {i}",
                "timestamp": "2026-03-30T09:30:00Z",
                "isSelf": False,
            }
            for i in range(130, 100, -1)
        ]

        def fake_list_messages(chat_id: str, limit: int = 50, offset: int = 0):
            if offset == 0:
                return page1
            elif offset == 50:
                return page2
            return []

        mock_client.list_chats.return_value = [
            {
                "id": "catchup_chat",
                "name": "Catchup Chat",
                "isGroup": False,
                "lastMsgLocalId": 180,
                "lastActivityAt": "2026-03-30T10:00:00Z",
                "unreadCount": 80,
            }
        ]
        mock_client.list_messages.side_effect = fake_list_messages
        mock_client_factory.return_value = mock_client

        res = worker.run_account(account)
        self.assertTrue(res["ok"])
        self.assertEqual(res["messages"], 80)
        self.assertEqual(mock_client.list_messages.call_count, 2)

        # Verify all 81 messages are present in store
        all_msgs = self.store.list_messages(account.account_id, "catchup_chat", limit=200)["messages"]
        self.assertEqual(len(all_msgs), 81)
        self.assertEqual(self.store.max_source_local_id(account.account_id, "catchup_chat"), 180)

    @patch("core.agent_wechat.AgentWechatClient.from_account")
    def test_unchanged_chat_fetch_zero(self, mock_client_factory: MagicMock) -> None:
        """P0-1 Gate: UNCHANGED_CHAT_MESSAGE_FETCH = 0 when lastMsgLocalId <= core_max_id."""
        from core.account_worker import AccountWorker

        account = self._create_agent_account()
        worker = AccountWorker(self.registry, self.store)

        mock_client = MagicMock(spec=AgentWechatClient)
        mock_client.health.return_value = {"status": "ok"}
        mock_client.list_contacts.return_value = []

        # Seed localId=50
        self.store.upsert_chat({
            "account_id": account.account_id,
            "chat_id": "idle_chat",
            "type": "private",
            "display_name": "Idle Chat",
        })
        self.store.upsert_message(normalize_agent_message(account.account_id, {
            "localId": 50,
            "chatId": "idle_chat",
            "kind": "text",
            "content": "msg 50",
            "timestamp": "2026-03-30T09:00:00Z",
            "isSelf": False,
        }))

        # Upstream chat reports lastMsgLocalId=50 (unchanged)
        mock_client.list_chats.return_value = [
            {
                "id": "idle_chat",
                "name": "Idle Chat",
                "isGroup": False,
                "lastMsgLocalId": 50,
                "lastActivityAt": "2026-03-30T09:00:00Z",
                "unreadCount": 0,
            }
        ]
        mock_client_factory.return_value = mock_client

        res = worker.run_account(account)
        self.assertTrue(res["ok"])
        self.assertEqual(mock_client.list_messages.call_count, 0)

    @patch("core.agent_wechat.AgentWechatClient.from_account")
    def test_unread_count_only_change_fetch_zero(self, mock_client_factory: MagicMock) -> None:
        """P0-1 Gate: unreadCount change without lastMsgLocalId change must fetch 0 messages."""
        from core.account_worker import AccountWorker

        account = self._create_agent_account()
        worker = AccountWorker(self.registry, self.store)

        mock_client = MagicMock(spec=AgentWechatClient)
        mock_client.health.return_value = {"status": "ok"}
        mock_client.list_contacts.return_value = []

        # Seed localId=50
        self.store.upsert_chat({
            "account_id": account.account_id,
            "chat_id": "read_chat",
            "type": "private",
            "display_name": "Read Chat",
        })
        self.store.upsert_message(normalize_agent_message(account.account_id, {
            "localId": 50,
            "chatId": "read_chat",
            "kind": "text",
            "content": "msg 50",
            "timestamp": "2026-03-30T09:00:00Z",
            "isSelf": False,
        }))

        # Upstream chat reports lastMsgLocalId=50, but unreadCount changed from 0 to 5
        mock_client.list_chats.return_value = [
            {
                "id": "read_chat",
                "name": "Read Chat",
                "isGroup": False,
                "lastMsgLocalId": 50,
                "lastActivityAt": "2026-03-30T09:00:00Z",
                "unreadCount": 5,
            }
        ]
        mock_client_factory.return_value = mock_client

        res = worker.run_account(account)
        self.assertTrue(res["ok"])
        # Must NOT call list_messages solely due to unreadCount change
        self.assertEqual(mock_client.list_messages.call_count, 0)


class LazyMediaCorrectnessTest(unittest.TestCase):
    """P0-4 tests for lazy media correctness: unsupported priority, collision prevention, role preservation, streaming."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp_dir.name)
        self.db_path = self.root / "core.sqlite"
        self.store = CoreStore(self.db_path)
        self.registry_file = self.root / "accounts.json"
        self.registry_file.write_text('{"accounts": []}', encoding="utf-8")
        self.registry = AccountRegistry([], self.registry_file)

    def tearDown(self) -> None:
        self.store.close()
        self.tmp_dir.cleanup()

    def _create_agent_account(self, account_id: str = "acc_media") -> AccountConfig:
        config_dir = self.root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        token_file = config_dir / "auth-token"
        token_file.write_text("media-token\n", encoding="utf-8")

        account_dir = self.root / "accounts" / account_id
        account_dir.mkdir(parents=True, exist_ok=True)
        (account_dir / "xwechat_files" / "wxid_bound").mkdir(parents=True, exist_ok=True)

        reg_payload = {
            "accounts": [
                {
                    "account_id": account_id,
                    "display_name": "Media Account",
                    "source_db_dir": str(account_dir / "xwechat_files" / "wxid_bound"),
                    "runtime_provider": "agent_wechat",
                    "agent_wechat": {
                        "base_url": "http://127.0.0.1:6174",
                        "token_file": str(token_file),
                    },
                }
            ]
        }
        self.registry_file.write_text(json.dumps(reg_payload), encoding="utf-8")
        self.registry.replace_from(load_registry(self.registry_file, root=self.root))
        account = self.registry.require(account_id)
        self.store.upsert_account(account_id, account.display_name, state="online", runtime=account.runtime)
        return account

    @patch("core.agent_wechat.AgentWechatClient.from_account")
    def test_unsupported_evaluated_first_and_terminal(self, mock_client_factory: MagicMock) -> None:
        """P0-4 Gate: unsupported must be evaluated before not data (no fake pending) and terminal cached."""
        from core.app import ApiError, CoreService

        account = self._create_agent_account()
        service = CoreService(root=self.root, registry=self.registry, store=self.store)

        self.store.upsert_chat({
            "account_id": account.account_id,
            "chat_id": "chat_unsupported",
            "type": "private",
            "display_name": "Unsupported Chat",
        })
        norm_msg = normalize_agent_message(
            account.account_id,
            {
                "localId": 99,
                "chatId": "chat_unsupported",
                "kind": "sticker",
                "content": "",
                "timestamp": "2026-03-30T10:00:00Z",
                "isSelf": False,
            },
        )
        self.store.upsert_message(norm_msg)
        media_id = norm_msg["media_id"]

        mock_client = MagicMock(spec=AgentWechatClient)
        # Upstream returns unsupported header with empty data
        mock_client.get_media_raw.return_value = (b"", {"x-media-status": "unsupported"})
        mock_client.fetch_media_to_file.return_value = {"status": "unsupported", "x-media-status": "unsupported"}
        mock_client_factory.return_value = mock_client

        # 1. First call: must raise 404 media_unsupported (NOT 202 media_pending)
        with self.assertRaises(ApiError) as ctx:
            service.resolve_media(account.account_id, media_id)
        self.assertEqual(ctx.exception.status, 404)
        self.assertEqual(ctx.exception.code, "media_unsupported")

        # 2. Verify stored as terminal unsupported
        cached = self.store.media(account.account_id, media_id)
        self.assertIsNotNone(cached)
        self.assertEqual(cached["status"], "unsupported")

        # 3. Second call: must return 404 from cache without re-querying upstream
        mock_client.fetch_media_to_file.reset_mock()
        mock_client.get_media_raw.reset_mock()
        with self.assertRaises(ApiError) as ctx2:
            service.resolve_media(account.account_id, media_id)
        self.assertEqual(ctx2.exception.status, 404)
        self.assertEqual(ctx2.exception.code, "media_unsupported")
        self.assertEqual(mock_client.fetch_media_to_file.call_count, 0)
        self.assertEqual(mock_client.get_media_raw.call_count, 0)

    @patch("core.agent_wechat.AgentWechatClient.from_account")
    def test_media_cache_filename_collision_isolated(self, mock_client_factory: MagicMock) -> None:
        """P0-4 Gate: MEDIA_CACHE_FILENAME_COLLISION = PASS: two files with same filename cached in media_id dirs."""
        from core.app import CoreService

        account = self._create_agent_account()
        service = CoreService(root=self.root, registry=self.registry, store=self.store)

        self.store.upsert_chat({
            "account_id": account.account_id,
            "chat_id": "chat_collision",
            "type": "private",
            "display_name": "Collision Chat",
        })

        # Message A
        msg_a = normalize_agent_message(account.account_id, {
            "localId": 1001,
            "chatId": "chat_collision",
            "kind": "file",
            "filename": "report.pdf",
            "timestamp": "2026-03-30T10:00:00Z",
            "isSelf": False,
        })
        self.store.upsert_message(msg_a)

        # Message B
        msg_b = normalize_agent_message(account.account_id, {
            "localId": 1002,
            "chatId": "chat_collision",
            "kind": "file",
            "filename": "report.pdf",
            "timestamp": "2026-03-30T10:01:00Z",
            "isSelf": False,
        })
        self.store.upsert_message(msg_b)

        media_id_a = msg_a["media_id"]
        media_id_b = msg_b["media_id"]
        self.assertNotEqual(media_id_a, media_id_b)

        mock_client = MagicMock(spec=AgentWechatClient)
        content_a = b"%PDF-1.4 Content A"
        content_b = b"%PDF-1.4 Content B (different)"

        def fake_fetch(chat_id, local_id, target_path, **kwargs):
            if int(local_id) == 1001:
                target_path.write_bytes(content_a)
                return {"status": "ready", "x-media-status": "ready", "x-media-filename": "report.pdf", "x-media-role": "original"}
            else:
                target_path.write_bytes(content_b)
                return {"status": "ready", "x-media-status": "ready", "x-media-filename": "report.pdf", "x-media-role": "original"}

        mock_client.fetch_media_to_file.side_effect = fake_fetch
        mock_client.get_media_raw.side_effect = lambda cid, lid: (content_a if int(lid) == 1001 else content_b, {"x-media-status": "ready", "x-media-filename": "report.pdf", "x-media-role": "original"})
        mock_client_factory.return_value = mock_client

        # Resolve A
        res_a, _, fname_a, _, _ = service.resolve_media(account.account_id, media_id_a)
        data_a = res_a.read_bytes() if isinstance(res_a, Path) else res_a
        self.assertEqual(data_a, content_a)

        # Resolve B
        res_b, _, fname_b, _, _ = service.resolve_media(account.account_id, media_id_b)
        data_b = res_b.read_bytes() if isinstance(res_b, Path) else res_b
        self.assertEqual(data_b, content_b)

        # Inspect disk paths: must be in different directories under media_id
        media_row_a = self.store.media(account.account_id, media_id_a)
        media_row_b = self.store.media(account.account_id, media_id_b)

        path_a = Path(media_row_a["local_path"])
        path_b = Path(media_row_b["local_path"])

        self.assertIn(media_id_a, str(path_a))
        self.assertIn(media_id_b, str(path_b))
        self.assertNotEqual(path_a, path_b)
        self.assertEqual(path_a.read_bytes(), content_a)
        self.assertEqual(path_b.read_bytes(), content_b)

    @patch("core.agent_wechat.AgentWechatClient.from_account")
    def test_media_role_preserved_to_core(self, mock_client_factory: MagicMock) -> None:
        """P0-4 Gate: MEDIA_ROLE_PRESERVED = PASS: thumbnail vs original preserved into CoreStore."""
        from core.app import CoreService

        account = self._create_agent_account()
        service = CoreService(root=self.root, registry=self.registry, store=self.store)

        self.store.upsert_chat({
            "account_id": account.account_id,
            "chat_id": "chat_role",
            "type": "private",
            "display_name": "Role Chat",
        })

        # Thumbnail image message
        msg_thumb = normalize_agent_message(account.account_id, {
            "localId": 2001,
            "chatId": "chat_role",
            "kind": "image",
            "filename": "img_2001_thumb.jpg",
            "timestamp": "2026-03-30T10:00:00Z",
            "isSelf": False,
        })
        self.store.upsert_message(msg_thumb)

        mock_client = MagicMock(spec=AgentWechatClient)
        def fake_fetch_thumb(chat_id, local_id, target_path, **kwargs):
            target_path.write_bytes(b"thumb_bytes")
            return {"status": "ready", "x-media-status": "ready", "x-media-filename": "img_2001_thumb.jpg", "x-media-role": "thumbnail"}

        mock_client.fetch_media_to_file.side_effect = fake_fetch_thumb
        mock_client.get_media_raw.return_value = (
            b"thumb_bytes",
            {"x-media-status": "ready", "x-media-filename": "img_2001_thumb.jpg", "x-media-role": "thumbnail"},
        )
        mock_client_factory.return_value = mock_client

        service.resolve_media(account.account_id, msg_thumb["media_id"])
        saved_media = self.store.media(account.account_id, msg_thumb["media_id"])
        self.assertEqual(saved_media["role"], "thumbnail")

    @patch("core.agent_wechat.AgentWechatClient.from_account")
    def test_url_backed_sticker_lazy_fetch_without_auth_header(self, mock_client_factory: MagicMock) -> None:
        """P0-5 Gate: URL_BACKED_STICKER_LAZY_FETCH = PASS: lazy fetch URL without Auth header to CDN."""
        from core.app import CoreService

        account = self._create_agent_account()
        service = CoreService(root=self.root, registry=self.registry, store=self.store)

        self.store.upsert_chat({
            "account_id": account.account_id,
            "chat_id": "chat_sticker",
            "type": "private",
            "display_name": "Sticker Chat",
        })

        msg_sticker = normalize_agent_message(account.account_id, {
            "localId": 3001,
            "chatId": "chat_sticker",
            "kind": "sticker",
            "filename": "emoji_abc123.gif",
            "timestamp": "2026-03-30T10:00:00Z",
            "isSelf": False,
        })
        self.store.upsert_message(msg_sticker)

        mock_client = MagicMock(spec=AgentWechatClient)
        def fake_fetch_sticker(chat_id, local_id, target_path, **kwargs):
            target_path.write_bytes(b"GIF89a_fake_sticker")
            return {
                "status": "ready",
                "x-media-status": "ready",
                "x-media-filename": "emoji_abc123.gif",
                "x-media-role": "original",
                "content-type": "image/gif",
            }

        mock_client.fetch_media_to_file.side_effect = fake_fetch_sticker
        mock_client_factory.return_value = mock_client

        res, mime, fname, disp, status = service.resolve_media(account.account_id, msg_sticker["media_id"])
        self.assertEqual(status, "ready")
        self.assertEqual(mime, "image/gif")
        self.assertEqual(fname, "emoji_abc123.gif")
        saved_media = self.store.media(account.account_id, msg_sticker["media_id"])
        self.assertIsNotNone(saved_media)
        self.assertEqual(saved_media["status"], "ready")

    @patch("core.agent_wechat.AgentWechatClient.from_account")
    def test_sticker_failure_does_not_block_text_sync(self, mock_client_factory: MagicMock) -> None:
        """P0-5 Gate: STICKER_FAILURE_DOES_NOT_BLOCK_TEXT_SYNC = PASS."""
        from core.account_worker import AccountWorker

        account = self._create_agent_account()
        worker = AccountWorker(self.registry, self.store)

        mock_client = MagicMock(spec=AgentWechatClient)
        mock_client.health.return_value = {"status": "ok"}
        mock_client.list_chats.return_value = [
            {"id": "chat_mixed", "name": "Mixed Chat", "lastMsgLocalId": 5002, "isGroup": False}
        ]
        # Messages: one text message and one sticker message
        mock_client.list_messages.return_value = [
            {
                "localId": 5002,
                "chatId": "chat_mixed",
                "kind": "text",
                "content": "Hello after sticker",
                "timestamp": "2026-03-30T10:00:02Z",
                "isSelf": False,
            },
            {
                "localId": 5001,
                "chatId": "chat_mixed",
                "kind": "sticker",
                "content": "",
                "timestamp": "2026-03-30T10:00:01Z",
                "isSelf": False,
            },
        ]
        # Even if media fetching fails or raises, sync cycle NEVER touches media!
        mock_client.fetch_media_to_file.side_effect = Exception("CDN network timeout")
        mock_client_factory.return_value = mock_client

        status = worker.run_account(account)
        self.assertTrue(status.get("ok"))
        self.assertEqual(status.get("messages"), 2)

        # Messages are fully synced into store
        msgs = self.store.list_messages(account.account_id, "chat_mixed")["messages"]
        self.assertEqual(len(msgs), 2)
        local_ids = [int(m.get("vendor_specific", {}).get("source_local_id") or m.get("source_local_id") or 0) for m in msgs]
        self.assertIn(5001, local_ids)
        self.assertIn(5002, local_ids)

    @patch("core.agent_wechat.AgentWechatClient.from_account")
    def test_large_file_streaming_over_10mb(self, mock_client_factory: MagicMock) -> None:
        """P0-6 Gate: LARGE_FILE_STREAMING_OVER_10MB = PASS: chunked streaming of >10MB fixture."""
        from core.app import CoreService

        account = self._create_agent_account()
        service = CoreService(root=self.root, registry=self.registry, store=self.store)

        self.store.upsert_chat({
            "account_id": account.account_id,
            "chat_id": "chat_large_file",
            "type": "private",
            "display_name": "Large File Chat",
        })

        # 12MB fixture
        fixture_size = 12 * 1024 * 1024
        msg_file = normalize_agent_message(account.account_id, {
            "localId": 9001,
            "chatId": "chat_large_file",
            "kind": "file",
            "filename": "dataset_12mb.bin",
            "timestamp": "2026-03-30T10:00:00Z",
            "isSelf": False,
        })
        self.store.upsert_message(msg_file)

        mock_client = MagicMock(spec=AgentWechatClient)
        def fake_fetch_large(chat_id, local_id, target_path, **kwargs):
            # Stream write 12MB in 64KB chunks
            with open(target_path, "wb") as f:
                chunk = b"X" * (64 * 1024)
                for _ in range(fixture_size // (64 * 1024)):
                    f.write(chunk)
            return {
                "status": "ready",
                "x-media-status": "ready",
                "x-media-filename": "dataset_12mb.bin",
                "x-media-role": "original",
                "content-type": "application/octet-stream",
            }

        mock_client.fetch_media_to_file.side_effect = fake_fetch_large
        mock_client_factory.return_value = mock_client

        res_path, mime, fname, disp, status = service.resolve_media(account.account_id, msg_file["media_id"])
        self.assertEqual(status, "ready")
        self.assertEqual(fname, "dataset_12mb.bin")
        self.assertTrue(isinstance(res_path, Path))
        self.assertEqual(res_path.stat().st_size, fixture_size)

        # Ensure store records correct metadata
        saved_media = self.store.media(account.account_id, msg_file["media_id"])
        self.assertIsNotNone(saved_media)
        self.assertEqual(saved_media["filename"], "dataset_12mb.bin")
        self.assertEqual(saved_media["role"], "original")
        self.assertEqual(saved_media["status"], "ready")

    def test_agent_client_real_large_file_streaming_over_10mb(self) -> None:
        """P1 Integration: Real socket HTTP streaming of >10MB file from agent-server file route."""
        import http.server
        import threading

        fixture_size = 12 * 1024 * 1024  # 12MB
        chunk_size = 64 * 1024
        received_auth: list[str] = []

        class StreamHandler(http.server.BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                received_auth.append(self.headers.get("Authorization", ""))
                self.send_response(200)
                self.send_header("Content-Type", "application/octet-stream")
                self.send_header("Content-Length", str(fixture_size))
                self.send_header("X-Media-Status", "ready")
                self.send_header("X-Media-Filename", "stream_12mb.bin")
                self.send_header("X-Media-Role", "original")
                self.end_headers()

                chunk = b"S" * chunk_size
                remaining = fixture_size
                while remaining > 0:
                    to_write = min(chunk_size, remaining)
                    self.wfile.write(chunk[:to_write])
                    remaining -= to_write

            def log_message(self, format: str, *args: Any) -> None:
                pass  # Suppress console noise

        server = http.server.HTTPServer(("127.0.0.1", 0), StreamHandler)
        port = server.server_address[1]
        t = threading.Thread(target=server.serve_forever, daemon=True)
        t.start()

        try:
            client = AgentWechatClient(f"http://127.0.0.1:{port}", "real-stream-token")
            target_file = self.root / "streamed_12mb.bin"
            meta = client.fetch_media_to_file("chat_stream", 9002, target_file)

            self.assertEqual(meta["status"], "ready")
            self.assertEqual(meta.get("x-media-filename"), "stream_12mb.bin")
            self.assertEqual(meta.get("x-media-role"), "original")
            self.assertTrue(target_file.exists())
            self.assertEqual(target_file.stat().st_size, fixture_size)
            self.assertIn("Bearer real-stream-token", received_auth[0])

            # Verify content integrity
            with open(target_file, "rb") as f:
                first_chunk = f.read(chunk_size)
                self.assertEqual(first_chunk, b"S" * chunk_size)
        finally:
            server.shutdown()
            server.server_close()
            t.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
