"""Regression tests for message instance_uuid and wechat_identity_uuid projection.

Covers criteria A-G from Factory Fresh user acceptance defect taskbook:
A. normalized message persistence: instance_uuid == account instance_uuid,
   wechat_identity_uuid == bound identity uuid, and event payload carries both.
B. GET messages with (account_id, chat_id, instance_uuid, wechat_identity_uuid)
   returns matching messages.
C. Mismatched instance_uuid returns 0 messages.
D. Mismatched wechat_identity_uuid returns 0 messages.
E. Identity rebound does not expose previous identity's messages to new identity.
F. Outgoing messages correctly inherit and persist both identity fields.
G. Console messages.js filter simulation: filtered query returns exact messages
   without requiring identity filter deletion.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import tempfile
import unittest
import uuid
from pathlib import Path
import sys

CORE_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CORE_ROOT))

from core import identity
from core.normalize import _normalized_message
from core.store import CoreStore


WXID_A = "wxid_identity_aaa_1111"
WXID_B = "wxid_identity_bbb_2222"


class MessageIdentityProjectionTest(unittest.TestCase):
    def setUp(self):
        self.temp_dir = tempfile.TemporaryDirectory()
        self.db_path = Path(self.temp_dir.name) / "core_test.sqlite"
        self.store = CoreStore(self.db_path)
        self.account_id = "test-account"
        self.chat_id = "25497796206@chatroom"

        # Seed account and chat
        self.store.upsert_account(self.account_id, "Test Account", state="online")
        self.store.upsert_chat(
            {
                "account_id": self.account_id,
                "chat_id": self.chat_id,
                "type": "group",
                "display_name": "Test Group",
            }
        )

        # Bind identity A
        login_res = self.store.observe_login(
            self.account_id,
            WXID_A,
            verified_source=identity.VERIFIED_SOURCE_AGENT_AUTH,
        )
        self.assertEqual(login_res["state"], "bound")
        self.identity_a = login_res["wechat_identity_uuid"]
        self.instance_uuid = login_res["instance_uuid"]
        self.assertTrue(self.identity_a)
        self.assertTrue(self.instance_uuid)

    def tearDown(self):
        self.temp_dir.cleanup()

    def test_criterion_a_normalized_message_persists_identity_and_emits_event(self):
        """A. normalized message 写入后：instance_uuid == account instance_uuid, wechat_identity_uuid == bound identity uuid."""
        msg_in = {
            "account_id": self.account_id,
            "message_id": "msg-incoming-001",
            "chat_id": self.chat_id,
            "type": "text",
            "direction": "incoming",
            "created_at": "2026-09-20T07:25:00Z",
            "author": {"member_id": "sender_1", "display_name": "Sender 1", "is_self": False},
            "text": "incoming hello",
        }
        res = self.store.upsert_message(msg_in)
        self.assertEqual(res, "created")

        # Verify Core database row
        with self.store.connection() as conn:
            row = conn.execute(
                "SELECT * FROM messages WHERE account_id=? AND message_id=?",
                (self.account_id, "msg-incoming-001"),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["instance_uuid"], self.instance_uuid)
            self.assertEqual(row["wechat_identity_uuid"], self.identity_a)

        # Verify event stream payload contains identity fields
        events_page = self.store.poll_events(after="0", limit=100)
        msg_events = [
            e for e in events_page["events"]
            if e.get("event_type") == "message.created"
            and e.get("payload", {}).get("message", {}).get("message_id") == "msg-incoming-001"
        ]
        self.assertEqual(len(msg_events), 1)
        event_msg = msg_events[0]["payload"]["message"]
        self.assertEqual(event_msg.get("instance_uuid"), self.instance_uuid)
        self.assertEqual(event_msg.get("wechat_identity_uuid"), self.identity_a)

    def test_criterion_b_get_messages_with_exact_identities_returns_data(self):
        """B. GET messages 使用 account_id + chat_id + instance_uuid + wechat_identity_uuid 可以返回消息。"""
        self.store.upsert_message(
            {
                "account_id": self.account_id,
                "message_id": "msg-002",
                "chat_id": self.chat_id,
                "type": "text",
                "direction": "incoming",
                "created_at": "2026-09-20T07:26:00Z",
                "author": {"member_id": "sender_1", "display_name": "Sender 1", "is_self": False},
                "text": "test message 2",
            }
        )

        result = self.store.list_messages(
            self.account_id,
            self.chat_id,
            instance_uuid=self.instance_uuid,
            wechat_identity_uuid=self.identity_a,
        )
        messages = result.get("messages", [])
        self.assertEqual(len(messages), 1)
        self.assertEqual(messages[0]["message_id"], "msg-002")
        self.assertEqual(messages[0]["instance_uuid"], self.instance_uuid)
        self.assertEqual(messages[0]["wechat_identity_uuid"], self.identity_a)

    def test_criterion_c_wrong_instance_uuid_returns_empty(self):
        """C. 错误 instance_uuid 返回 0。"""
        self.store.upsert_message(
            {
                "account_id": self.account_id,
                "message_id": "msg-003",
                "chat_id": self.chat_id,
                "type": "text",
                "direction": "incoming",
                "created_at": "2026-09-20T07:26:30Z",
                "author": {"member_id": "sender_1", "display_name": "Sender 1", "is_self": False},
                "text": "test message 3",
            }
        )

        wrong_instance = "00000000-0000-0000-0000-000000000000"
        result = self.store.list_messages(
            self.account_id,
            self.chat_id,
            instance_uuid=wrong_instance,
            wechat_identity_uuid=self.identity_a,
        )
        self.assertEqual(len(result.get("messages", [])), 0)

    def test_criterion_d_wrong_wechat_identity_uuid_returns_empty(self):
        """D. 错误 wechat_identity_uuid 返回 0。"""
        self.store.upsert_message(
            {
                "account_id": self.account_id,
                "message_id": "msg-004",
                "chat_id": self.chat_id,
                "type": "text",
                "direction": "incoming",
                "created_at": "2026-09-20T07:26:45Z",
                "author": {"member_id": "sender_1", "display_name": "Sender 1", "is_self": False},
                "text": "test message 4",
            }
        )

        wrong_identity = "ffffffff-ffff-ffff-ffff-ffffffffffff"
        result = self.store.list_messages(
            self.account_id,
            self.chat_id,
            instance_uuid=self.instance_uuid,
            wechat_identity_uuid=wrong_identity,
        )
        self.assertEqual(len(result.get("messages", [])), 0)

    def test_criterion_e_rebound_identity_does_not_leak_old_identity_messages(self):
        """E. 身份重新绑定后不能把旧 identity message 暴露给新 identity。"""
        self.store.upsert_message(
            {
                "account_id": self.account_id,
                "message_id": "msg-identity-a",
                "chat_id": self.chat_id,
                "type": "text",
                "direction": "incoming",
                "created_at": "2026-09-20T07:27:00Z",
                "author": {"member_id": "sender_1", "display_name": "Sender 1", "is_self": False},
                "text": "secret of identity A",
            }
        )

        # Switch and bind identity B
        self.store.observe_login(
            self.account_id,
            WXID_B,
            verified_source=identity.VERIFIED_SOURCE_AGENT_AUTH,
        )
        switch = self.store.confirm_switch(self.account_id)
        identity_b = switch["wechat_identity_uuid"]
        self.assertNotEqual(self.identity_a, identity_b)

        # Query messages for identity B: must return 0 messages
        result_b = self.store.list_messages(
            self.account_id,
            self.chat_id,
            instance_uuid=self.instance_uuid,
            wechat_identity_uuid=identity_b,
        )
        self.assertEqual(len(result_b.get("messages", [])), 0)

        # Ingest message under identity B
        self.store.upsert_message(
            {
                "account_id": self.account_id,
                "message_id": "msg-identity-b",
                "chat_id": self.chat_id,
                "type": "text",
                "direction": "incoming",
                "created_at": "2026-09-20T07:28:00Z",
                "author": {"member_id": "sender_2", "display_name": "Sender 2", "is_self": False},
                "text": "secret of identity B",
            }
        )

        # Query messages for identity B: only B's message returned
        result_b_new = self.store.list_messages(
            self.account_id,
            self.chat_id,
            instance_uuid=self.instance_uuid,
            wechat_identity_uuid=identity_b,
        )
        msgs_b = result_b_new.get("messages", [])
        self.assertEqual(len(msgs_b), 1)
        self.assertEqual(msgs_b[0]["message_id"], "msg-identity-b")

        # Query messages for identity A: only A's message returned
        result_a = self.store.list_messages(
            self.account_id,
            self.chat_id,
            instance_uuid=self.instance_uuid,
            wechat_identity_uuid=self.identity_a,
        )
        msgs_a = result_a.get("messages", [])
        self.assertEqual(len(msgs_a), 1)
        self.assertEqual(msgs_a[0]["message_id"], "msg-identity-a")

    def test_criterion_f_outgoing_message_persists_both_identities(self):
        """F. outgoing message 也必须写入两个 identity fields。"""
        outbox_row = self.store.queue_send(
            kind="text",
            payload={
                "account_id": self.account_id,
                "chat_id": self.chat_id,
                "text": "hello outgoing",
                "client_request_id": "client-req-001",
            },
            idempotency_key="client-req-001",
        )
        self.assertEqual(outbox_row["status"], "accepted")

        # Submit send
        with self.store.connection() as conn:
            conn.execute("UPDATE outbox SET status='submitted' WHERE send_id=?", (outbox_row["send_id"],))

        # Normalized outgoing echo message sync
        msg_out = {
            "account_id": self.account_id,
            "message_id": "echo-msg-out-001",
            "chat_id": self.chat_id,
            "type": "text",
            "direction": "outgoing",
            "created_at": "2026-09-20T07:27:08Z",
            "author": {"member_id": "self", "display_name": "self", "is_self": True},
            "text": "hello outgoing",
        }
        res = self.store.upsert_message(msg_out)
        self.assertEqual(res, "created")

        # Verify DB row has identity fields
        with self.store.connection() as conn:
            row = conn.execute(
                "SELECT * FROM messages WHERE account_id=? AND message_id=?",
                (self.account_id, "echo-msg-out-001"),
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertEqual(row["instance_uuid"], self.instance_uuid)
            self.assertEqual(row["wechat_identity_uuid"], self.identity_a)

        # Verify event payload
        events_page = self.store.poll_events(after="0", limit=200)
        out_events = [
            e for e in events_page["events"]
            if e.get("event_type") == "message.created"
            and e.get("payload", {}).get("message", {}).get("message_id") == "echo-msg-out-001"
        ]
        self.assertEqual(len(out_events), 1)
        self.assertEqual(out_events[0]["payload"]["message"]["instance_uuid"], self.instance_uuid)
        self.assertEqual(out_events[0]["payload"]["message"]["wechat_identity_uuid"], self.identity_a)

    def test_criterion_g_console_messages_filter_behavior_simulation(self):
        """G. 现有 Console messages.js 无需删除身份过滤即可返回有效消息。"""
        # Normalizer producing message
        row_mock = {
            "message_uid": "msg-005",
            "chat_username": self.chat_id,
            "type_label": "text",
            "message_content": "hello world",
            "compress_content": "",
            "source": "",
            "origin_source": 0,
            "create_time": 1789889228,
        }
        normalized = _normalized_message(
            self.account_id,
            row_mock,
            {},
            instance_uuid=self.instance_uuid,
            wechat_identity_uuid=self.identity_a,
        )
        self.assertEqual(normalized["instance_uuid"], self.instance_uuid)
        self.assertEqual(normalized["wechat_identity_uuid"], self.identity_a)

        self.store.upsert_message(normalized)

        # Simulated Console messages.js query with both filters intact
        params = {
            "account_id": self.account_id,
            "chat_id": self.chat_id,
            "instance_uuid": self.instance_uuid,
            "wechat_identity_uuid": self.identity_a,
        }
        res = self.store.list_messages(
            params["account_id"],
            params["chat_id"],
            instance_uuid=params["instance_uuid"],
            wechat_identity_uuid=params["wechat_identity_uuid"],
        )
        self.assertEqual(len(res["messages"]), 1)
        self.assertEqual(res["messages"][0]["message_id"], "msg-005")
        self.assertEqual(res["messages"][0]["instance_uuid"], self.instance_uuid)
        self.assertEqual(res["messages"][0]["wechat_identity_uuid"], self.identity_a)


if __name__ == "__main__":
    unittest.main()
