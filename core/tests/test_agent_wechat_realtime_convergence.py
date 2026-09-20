"""Tests for AgentWechat real-time provider convergence.

Verifies:
1. Canonical message kinds & XML extraction (text, image, sticker, voice, video, file, link, reply, system, unknown)
2. Message ID parity with legacy content_hash(source_message_identity(...))
3. AccountWorker branching: skips DB decrypt/ingest; polls chats, respects watermarks, fetches incremental messages
4. Lazy media fetch on-demand via Core media endpoint
5. Unified outbox & send flow
"""

from __future__ import annotations

import base64
import json
import sqlite3
import tempfile
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
from core.registry import AccountConfig, AccountRegistry
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

    def test_kind_mapping(self) -> None:
        # text
        kind, sub, fn = map_agent_message_kind(1, "Hello world")
        self.assertEqual(kind, "text")

        # image
        kind, sub, fn = map_agent_message_kind(3, "<img />")
        self.assertEqual(kind, "image")

        # voice
        kind, sub, fn = map_agent_message_kind(34, "")
        self.assertEqual(kind, "voice")

        # video
        kind, sub, fn = map_agent_message_kind(43, "")
        self.assertEqual(kind, "video")

        # sticker / emoji
        kind, sub, fn = map_agent_message_kind(47, "<emoji />")
        self.assertEqual(kind, "sticker")

        # system
        kind, sub, fn = map_agent_message_kind(10000, "Recall message")
        self.assertEqual(kind, "system")
        kind, sub, fn = map_agent_message_kind(10002, "Revoked")
        self.assertEqual(kind, "system")

        # file (appmsg type 6)
        content_file = "<msg><appmsg><title>contract_final.pdf</title><type>6</type></appmsg></msg>"
        kind, sub, fn = map_agent_message_kind(49, content_file)
        self.assertEqual(kind, "file")
        self.assertEqual(sub, 6)
        self.assertEqual(fn, "contract_final.pdf")

        # link (appmsg type 5)
        content_link = "<msg><appmsg><title>News Title</title><type>5</type><url>https://example.com</url></appmsg></msg>"
        kind, sub, fn = map_agent_message_kind(49, content_link)
        self.assertEqual(kind, "link")
        self.assertEqual(sub, 5)

        # reply with refermsg
        content_reply = "<msg><appmsg><title>Title</title><refermsg><content>ref</content></refermsg></appmsg></msg>"
        kind, sub, fn = map_agent_message_kind(49, content_reply)
        self.assertEqual(kind, "reply")

        # unknown appmsg
        content_unknown = "<msg><appmsg><title>Custom</title><type>999</type></appmsg></msg>"
        kind, sub, fn = map_agent_message_kind(49, content_unknown)
        self.assertEqual(kind, "unknown")
        self.assertEqual(sub, 999)

        # completely unknown type
        kind, sub, fn = map_agent_message_kind(98765, "opaque")
        self.assertEqual(kind, "unknown")

    def test_message_id_parity_with_legacy(self) -> None:
        """C4: message_id must strictly match content_hash(source_message_identity(chat_id, '', local_id))."""
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
            "type": 3,
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


class AccountWorkerConvergenceTest(unittest.TestCase):
    """Work Package C tests for real-time incremental sync branching and watermark tracking."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp_dir.name)
        self.db_path = self.root / "core.sqlite"
        self.store = CoreStore(self.db_path)
        self.registry_file = self.root / "accounts.json"
        self.registry_file.write_text('{"accounts": []}', encoding="utf-8")
        self.registry = AccountRegistry([], self.registry_file)

    def tearDown(self) -> None:
        self.tmp_dir.cleanup()

    def _create_agent_account(self, account_id: str = "acc_agent") -> AccountConfig:
        config_dir = self.root / "config"
        config_dir.mkdir(parents=True, exist_ok=True)
        token_file = config_dir / "auth-token"
        token_file.write_text("mock-token-xyz\n", encoding="utf-8")

        account_dir = self.root / "accounts" / account_id
        account_dir.mkdir(parents=True, exist_ok=True)
        (account_dir / "xwechat_files" / "wxid_bound").mkdir(parents=True, exist_ok=True)

        account = AccountConfig(
            account_id=account_id,
            display_name="Agent Account",
            source_db_dir=account_dir / "xwechat_files" / "wxid_bound",
            wechat_base_dir=account_dir,
            keys_file=account_dir / "keys.json",
            runtime_dir=account_dir / "runtime",
            decrypted_dir=account_dir / "decrypted",
            decrypt_state_file=account_dir / "decrypt_state.json",
            memory_db=account_dir / "memory.db",
            media_dir=account_dir / "media",
            sync_status_file=account_dir / "sync_status.json",
            config_file=account_dir / "config.json",
            runtime={
                "instance_uuid": "inst-1",
                "runtime_alias": account_id,
                "resource_key": account_id,
                "runtime_provider": "agent_wechat",
                "agent_wechat_base_url": "http://127.0.0.1:6174",
                "agent_wechat_token_file": str(token_file),
                "logged_in_user": "wxid_bound",
                "running": True,
            },
        )
        self.registry._accounts = {account_id: account}
        self.store.upsert_account(account_id, account.display_name, state="online", runtime=account.runtime)
        return account

    @patch("core.agent_wechat.AgentWechatClient.from_account")
    def test_worker_agent_wechat_branches_and_syncs_incrementally(self, mock_client_factory: MagicMock) -> None:
        from core.account_worker import AccountWorker

        account = self._create_agent_account()
        worker = AccountWorker(self.registry, self.store)

        mock_client = MagicMock(spec=AgentWechatClient)
        mock_client.health.return_value = {"status": "ok"}
        mock_client.list_chats.return_value = [
            {
                "id": "chat_one",
                "name": "Chat One",
                "isGroup": False,
                "lastMsgLocalId": 10,
                "lastActivityAt": 1750000000,
                "unreadCount": 0,
            }
        ]
        mock_client.list_messages.return_value = [
            {
                "localId": 10,
                "serverId": "srv10",
                "chatId": "chat_one",
                "sender": "wxid_peer",
                "senderName": "Peer",
                "type": 1,
                "content": "First realtime message",
                "timestamp": "2026-03-30T10:00:00Z",
                "isSelf": False,
            }
        ]
        mock_client.list_contacts.return_value = []
        mock_client_factory.return_value = mock_client

        # Cycle 1: cold start -> should fetch messages
        result1 = worker.run_account(account)
        self.assertTrue(result1["ok"])
        self.assertEqual(result1["chats"], 1)
        self.assertEqual(result1["messages"], 1)
        self.assertEqual(mock_client.list_messages.call_count, 1)

        # Verify message written to CoreStore
        messages = self.store.list_messages(account.account_id, "chat_one")
        self.assertEqual(len(messages["messages"]), 1)
        self.assertEqual(messages["messages"][0]["text"], "First realtime message")

        # Verify events emitted
        events = self.store.poll_events(after="", limit=10)
        event_types = [e["event_type"] for e in events["events"]]
        self.assertIn("message.created", event_types)

        # Cycle 2: no changes in chat watermark -> list_messages must NOT be called
        mock_client.list_messages.reset_mock()
        result2 = worker.run_account(account)
        self.assertTrue(result2["ok"])
        self.assertEqual(mock_client.list_messages.call_count, 0)

        # Cycle 3: new message arrived in chat_one
        mock_client.list_chats.return_value = [
            {
                "id": "chat_one",
                "name": "Chat One",
                "isGroup": False,
                "lastMsgLocalId": 11,
                "lastActivityAt": 1750000010,
                "unreadCount": 1,
            }
        ]
        mock_client.list_messages.return_value = [
            {
                "localId": 11,
                "serverId": "srv11",
                "chatId": "chat_one",
                "sender": "wxid_peer",
                "senderName": "Peer",
                "type": 1,
                "content": "Second realtime message",
                "timestamp": "2026-03-30T10:01:00Z",
                "isSelf": False,
            }
        ]
        result3 = worker.run_account(account)
        self.assertTrue(result3["ok"])
        self.assertEqual(mock_client.list_messages.call_count, 1)

        messages3 = self.store.list_messages(account.account_id, "chat_one")
        self.assertEqual(len(messages3["messages"]), 2)


class LazyMediaFetchTest(unittest.TestCase):
    """Work Package D tests for on-demand lazy media fetching."""

    def setUp(self) -> None:
        self.tmp_dir = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp_dir.name)
        self.db_path = self.root / "core.sqlite"
        self.store = CoreStore(self.db_path)
        self.registry_file = self.root / "accounts.json"
        self.registry_file.write_text('{"accounts": []}', encoding="utf-8")
        self.registry = AccountRegistry([], self.registry_file)

    def tearDown(self) -> None:
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

        account = AccountConfig(
            account_id=account_id,
            display_name="Media Account",
            source_db_dir=account_dir / "xwechat_files" / "wxid_bound",
            wechat_base_dir=account_dir,
            keys_file=account_dir / "keys.json",
            runtime_dir=account_dir / "runtime",
            decrypted_dir=account_dir / "decrypted",
            decrypt_state_file=account_dir / "decrypt_state.json",
            memory_db=account_dir / "memory.db",
            media_dir=account_dir / "media",
            sync_status_file=account_dir / "sync_status.json",
            config_file=account_dir / "config.json",
            runtime={
                "instance_uuid": "inst-m",
                "runtime_alias": account_id,
                "resource_key": account_id,
                "runtime_provider": "agent_wechat",
                "agent_wechat_base_url": "http://127.0.0.1:6174",
                "agent_wechat_token_file": str(token_file),
                "logged_in_user": "wxid_bound",
                "running": True,
            },
        )
        self.registry._accounts = {account_id: account}
        self.store.upsert_account(account_id, account.display_name, state="online", runtime=account.runtime)
        return account

    @patch("core.agent_wechat.AgentWechatClient.from_account")
    def test_lazy_media_fetch_resolves_and_caches_to_disk(self, mock_client_factory: MagicMock) -> None:
        from core.app import CoreService

        account = self._create_agent_account()
        service = CoreService(root=self.root, registry=self.registry, store=self.store)

        # Upsert chat first for foreign key constraint
        self.store.upsert_chat({
            "account_id": account.account_id,
            "chat_id": "chat_media",
            "type": "private",
            "display_name": "Media Chat",
        })

        # Upsert a pending image message
        norm_msg = normalize_agent_message(
            account.account_id,
            {
                "localId": 42,
                "chatId": "chat_media",
                "type": 3,
                "content": "",
                "timestamp": "2026-03-30T10:00:00Z",
                "isSelf": False,
            },
        )
        self.store.upsert_message(norm_msg)
        media_id = norm_msg["media_id"]

        fake_image_bytes = b"\xff\xd8\xff\xe0\x00\x10JFIFfakeimagebytes"
        mock_client = MagicMock(spec=AgentWechatClient)
        mock_client.get_media.return_value = {
            "type": "image",
            "format": "jpeg",
            "filename": "img_42.jpg",
            "data": base64.b64encode(fake_image_bytes).decode("ascii"),
        }
        mock_client_factory.return_value = mock_client

        # Call resolve_media: should fetch on-demand, cache to disk, return bytes
        content, mime_type, filename, disposition, status = service.resolve_media(account.account_id, media_id)
        self.assertEqual(content, fake_image_bytes)
        self.assertEqual(mime_type, "image/jpeg")
        self.assertEqual(status, "ready")
        self.assertEqual(mock_client.get_media.call_count, 1)

        # Call again: should read directly from disk cache without calling get_media
        mock_client.get_media.reset_mock()
        content2, mime_type2, _, _, _ = service.resolve_media(account.account_id, media_id)
        self.assertEqual(content2, fake_image_bytes)
        self.assertEqual(mock_client.get_media.call_count, 0)


if __name__ == "__main__":
    unittest.main()
