from __future__ import annotations

import base64
import os
import tempfile
import json
import shutil
import sqlite3
import sys
import struct
import threading
import time
import unittest
import urllib.error
import urllib.request
import uuid
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch


CORE_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CORE_ROOT))

_TEST_GUI_LEASE_DIR = tempfile.TemporaryDirectory()
os.environ.setdefault('WECHAT_GUI_LEASE_DIR', _TEST_GUI_LEASE_DIR.name)

from core.app import CoreService, RegistryReloadLoop, create_server  # noqa: E402
from core.account_worker import media_args  # noqa: E402
from core.key_extract import import_agent_wechat_keys, scanner_command  # noqa: E402
from core.normalize import import_account  # noqa: E402
from core.registry import AccountRegistry, RegistryError, load_registry, parse_account, parse_runtime_account  # noqa: E402
from core.runtime_bridge import account_processes, resolve_runtime_account  # noqa: E402
from core.sender import AccountSender, sender_capabilities  # noqa: E402
from core.store import CoreStore, utc_now  # noqa: E402
from agent_console.wechat_controller import chat_search_query, chat_window_ready, open_chat  # noqa: E402
from memory.memory_ingest import ingest_chat, ingest_session_chats, init_memory_db  # noqa: E402
from memory.sync_repair import repair_memory_indexes  # noqa: E402
from memory import media_sync  # noqa: E402


@contextmanager
def sqlite_connection(path: Path):
    conn = sqlite3.connect(path)
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


class CoreHttpTest(unittest.TestCase):
    def setUp(self):
        self.temp_root = CORE_ROOT / ".tmp" / f"core-test-{uuid.uuid4().hex}"
        self.temp_root.mkdir(parents=True)
        self.registry = AccountRegistry(
            [
                parse_account({"account_id": "alpha", "display_name": "Alpha", "runtime_dir": "runtime/accounts/alpha"}, root=self.temp_root),
                parse_account({"account_id": "beta", "display_name": "Beta", "runtime_dir": "runtime/accounts/beta"}, root=self.temp_root),
            ],
            self.temp_root / "accounts.json",
        )
        self.store = CoreStore(self.temp_root / "core.sqlite")
        self.service = CoreService(root=self.temp_root, registry=self.registry, store=self.store)
        for account_id in ("alpha", "beta"):
            self.store.upsert_chat(
                {
                    "account_id": account_id,
                    "chat_id": "same-chat",
                    "type": "private",
                    "display_name": f"{account_id.title()} Contact",
                }
            )
        self.server = create_server("127.0.0.1", 0, self.service)
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def request(self, path, method="GET", payload=None, headers=None):
        body = None
        request_headers = dict(headers or {})
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            request_headers["Content-Type"] = "application/json"
        request = urllib.request.Request(self.base_url + path, data=body, method=method, headers=request_headers)
        with urllib.request.urlopen(request, timeout=3) as response:
            data = response.read()
            if response.headers.get_content_type() == "application/json":
                return response.status, json.loads(data)
            return response.status, data

    def write_registry(self, account_ids):
        accounts = []
        for account_id in account_ids:
            accounts.append(
                {
                    "account_id": account_id,
                    "display_name": account_id.title(),
                    "runtime_dir": f"runtime/accounts/{account_id}",
                    "runtime": {"display": ":1", "sender_enabled": True},
                }
            )
        self.registry.source_path.write_text(
            json.dumps({"accounts": accounts}),
            encoding="utf-8",
        )

    def test_accounts_chats_and_account_scoped_message_identity(self):
        status, health = self.request("/health")
        self.assertEqual(status, 200)
        self.assertEqual(health["contract_version"], 1)
        self.assertEqual(health["accounts"], 2)
        self.assertFalse(health["sender_capabilities"]["text"])
        self.assertFalse(health["sender_capabilities"]["image"])
        self.assertFalse(health["sender_capabilities"]["file"])
        self.assertFalse(health["sender_capabilities"]["native_reply"])
        self.assertEqual(health["sender_capabilities"]["max_mentions"], 0)
        self.assertFalse(health["sender_capabilities"]["verified_chat_target"])
        for account_id in ("alpha", "beta"):
            self.store.upsert_message(
                {
                    "account_id": account_id,
                    "message_id": "same-message",
                    "chat_id": "same-chat",
                    "type": "text",
                    "direction": "incoming",
                    "created_at": "2026-08-31T08:00:00Z",
                    "author": {"member_id": "member", "display_name": "Member", "is_self": False},
                    "text": account_id,
                }
            )
        _, accounts = self.request("/v1/accounts")
        self.assertEqual({item["account_id"] for item in accounts["accounts"]}, {"alpha", "beta"})
        _, chats = self.request("/v1/accounts/alpha/chats")
        self.assertEqual(chats["chats"][0]["display_name"], "Alpha Contact")
        page = self.store.poll_events(after="0", limit=200)
        messages = [event["payload"]["message"] for event in page["events"] if event["event_type"] == "message.created"]
        self.assertEqual({(item["account_id"], item["message_id"]) for item in messages}, {("alpha", "same-message"), ("beta", "same-message")})

    def test_registry_hot_reload_adds_and_removes_accounts_without_restart(self):
        self.write_registry(["alpha", "beta"])
        self.service.reload_registry(force=True)
        _, receipt = self.request(
            "/v1/send/text",
            method="POST",
            payload={"account_id": "beta", "chat_id": "same-chat", "text": "will be cancelled"},
            headers={"Idempotency-Key": "remove-beta-send"},
        )
        self.assertEqual(receipt["status"], "accepted")

        self.write_registry(["alpha", "gamma"])
        result = self.service.reload_registry(force=True)
        self.assertEqual(result["added"], ["gamma"])
        self.assertEqual(result["removed"], ["beta"])
        self.assertIsNotNone(self.registry.get("gamma"))
        self.assertIsNone(self.registry.get("beta"))

        _, accounts = self.request("/v1/accounts")
        self.assertEqual({row["account_id"] for row in accounts["accounts"]}, {"alpha", "gamma"})
        _, health = self.request("/health")
        self.assertEqual(health["accounts"], 2)
        self.assertTrue(health["registry"]["hot_reload"])
        removed = self.store.account("beta")
        self.assertEqual(removed["state"], "stopped")
        self.assertFalse(removed["runtime"]["registered"])
        failed = self.store.receipt_by_idempotency_key("remove-beta-send")
        self.assertEqual(failed["status"], "failed")

    def test_registry_watcher_detects_external_runtime_cli_changes(self):
        self.write_registry(["alpha", "beta"])
        self.service.reload_registry(force=True)
        loop = RegistryReloadLoop(self.service, 0.05)
        loop.start()
        try:
            self.write_registry(["alpha", "external"])
            deadline = time.monotonic() + 2.0
            active_ids = {row["account_id"] for row in self.service.accounts()}
            while time.monotonic() < deadline and active_ids != {"alpha", "external"}:
                time.sleep(0.02)
                active_ids = {row["account_id"] for row in self.service.accounts()}
            self.assertIsNotNone(self.registry.get("external"))
            self.assertIsNone(self.registry.get("beta"))
            self.assertEqual(active_ids, {"alpha", "external"})
        finally:
            loop.stop()

    def test_runtime_management_api_reloads_registry_immediately(self):
        self.write_registry(["alpha", "beta"])
        self.service.reload_registry(force=True)
        service = self.service

        class FakeRuntimeControl:
            available = True

            def request(self, action, **payload):
                current = json.loads(service.registry.source_path.read_text(encoding="utf-8"))
                accounts = current["accounts"]
                if action == "list":
                    return {
                        "accounts": [
                            {
                                "account_id": row["account_id"],
                                "display_name": row["display_name"],
                                "display": ":1",
                                "running": row["account_id"] != "beta",
                                "pids": [5000] if row["account_id"] != "beta" else [],
                                "windows": [],
                                "autostart": True,
                                "legacy": False,
                            }
                            for row in accounts
                        ]
                    }
                if action == "register":
                    account_id = payload["account_id"]
                    accounts.append(
                        {
                            "account_id": account_id,
                            "display_name": payload.get("display_name") or account_id,
                            "runtime_dir": f"runtime/accounts/{account_id}",
                            "runtime": {"display": payload.get("display") or ":1", "sender_enabled": True},
                        }
                    )
                    service.registry.source_path.write_text(json.dumps(current), encoding="utf-8")
                    return {"account": {"id": account_id}, "status": {"account_id": account_id, "running": True}}
                if action in {"start", "stop", "restart"}:
                    return {"status": {"account_id": payload["account_id"], "running": action != "stop", "action": action}}
                if action == "login_status":
                    return {
                        "login": {
                            "account_id": payload["account_id"],
                            "display_name": "Alpha",
                            "running": True,
                            "snapshot_available": True,
                            "windows": [{"window_id": 88, "title": "Weixin"}],
                            "window_title": "Weixin",
                        }
                    }
                if action == "start_login":
                    return {
                        "login": {
                            "account_id": payload["account_id"],
                            "running": True,
                            "snapshot_available": False,
                            "login_flow_state": "authenticating",
                            "login_flow_status": "Preparing QR",
                            "login_flow_error": "",
                        }
                    }
                if action == "capture_login":
                    return {
                        "account_id": payload["account_id"],
                        "content_type": "image/png",
                        "content_base64": base64.b64encode(b"\x89PNG\r\n\x1a\nlogin-image").decode("ascii"),
                    }
                if action == "desktop":
                    return {
                        "desktop": {
                            "account_id": payload["account_id"],
                            "runtime_provider": "agent_wechat",
                            "desktop_provider": "selkies",
                            "scheme": "https",
                            "host": "wechat.example.test",
                            "port": 443,
                            "path": "/desktop/opaque-session/",
                            "gateway_session_expires_at": 1234567890,
                            "file_exchange_path": "/home/wechat/WeChatHubFiles/Desktop",
                            "features": {
                                "local_ime": True,
                                "clipboard_text": True,
                                "file_upload": True,
                                "dynamic_resize": True,
                            },
                        }
                    }
                if action == "unregister":
                    account_id = payload["account_id"]
                    current["accounts"] = [row for row in accounts if row["account_id"] != account_id]
                    service.registry.source_path.write_text(json.dumps(current), encoding="utf-8")
                    return {"removed": account_id, "data_preserved": f"/config/{account_id}"}
                raise AssertionError(action)

        self.service.runtime_control = FakeRuntimeControl()
        alpha = self.store.account("alpha")
        self.store.upsert_account(
            "alpha",
            alpha["display_name"],
            state="online",
            runtime=alpha["runtime"],
            sync=alpha["sync"],
        )
        status, runtime = self.request("/v1/runtime/accounts")
        self.assertEqual(status, 200)
        self.assertEqual(len(runtime["accounts"]), 2)
        self.assertEqual(self.store.account("alpha")["state"], "online")

        # The ordinary account list must refresh the Runtime projection too;
        # otherwise a frozen child can remain falsely "healthy" forever even
        # though Runtime's bounded list probe already knows it is degraded.
        _, public_accounts = self.request("/v1/accounts")
        public_by_id = {row["account_id"]: row for row in public_accounts["accounts"]}
        self.assertTrue(public_by_id["alpha"]["runtime"]["running"])
        self.assertFalse(public_by_id["beta"]["runtime"]["running"])

        _, login = self.request("/v1/runtime/accounts/alpha/login")
        self.assertEqual(login["state"], "online")
        status, started = self.request("/v1/runtime/accounts/alpha/login", method="POST", payload={})
        self.assertEqual(status, 202)
        self.assertEqual(started["login_flow_state"], "authenticating")
        status, snapshot = self.request("/v1/runtime/accounts/alpha/login/snapshot")
        self.assertEqual(status, 200)
        self.assertEqual(snapshot, b"\x89PNG\r\n\x1a\nlogin-image")
        status, desktop = self.request("/v1/runtime/accounts/alpha/desktop")
        self.assertEqual(status, 200)
        self.assertEqual(desktop["desktop_provider"], "selkies")
        self.assertEqual(desktop["scheme"], "https")
        self.assertEqual(desktop["host"], "wechat.example.test")
        self.assertEqual(desktop["port"], 443)
        self.assertTrue(desktop["features"]["local_ime"])
        self.assertTrue(desktop["features"]["file_upload"])
        self.assertEqual(desktop["file_exchange_path"], "/home/wechat/WeChatHubFiles/Desktop")
        self.assertNotIn("token=", desktop["path"])

        status, created = self.request(
            "/v1/runtime/accounts",
            method="POST",
            payload={"account_id": "gamma", "display_name": "Gamma Work", "start": True},
        )
        self.assertEqual(status, 201)
        self.assertEqual(created["registry_reload"]["added"], ["gamma"])
        self.assertIsNotNone(self.registry.get("gamma"))

        _, stopped = self.request("/v1/runtime/accounts/gamma/stop", method="POST", payload={})
        self.assertFalse(stopped["status"]["running"])
        self.assertEqual(self.store.account("gamma")["state"], "stopped")
        _, removed = self.request("/v1/runtime/accounts/gamma", method="DELETE")
        self.assertEqual(removed["removed"], "gamma")
        self.assertIsNone(self.registry.get("gamma"))

        class NoQrRuntimeControl:
            available = True

            def request(self, action, **payload):
                if action != "capture_login":
                    raise AssertionError(action)
                return {
                    "account_id": payload["account_id"],
                    "status": "qr_not_ready",
                    "login_flow_state": "authenticating",
                    "login_flow_status": "Preparing QR",
                    "login_flow_error": "",
                }

        self.service.runtime_control = NoQrRuntimeControl()
        with self.assertRaises(urllib.error.HTTPError) as caught:
            self.request("/v1/runtime/accounts/alpha/login/snapshot")
        self.assertEqual(caught.exception.code, 409)
        error = json.loads(caught.exception.read().decode("utf-8"))["error"]
        self.assertEqual(error["code"], "qr_not_ready")
        self.assertEqual(error["details"]["login_flow_state"], "authenticating")

    def test_media_event_ack_and_account_scope(self):
        media_path = self.temp_root / "tiny.bin"
        media_path.write_bytes(b"media-bytes")
        self.store.upsert_media(
            {
                "account_id": "beta",
                "media_id": "same-media",
                "filename": "tiny.bin",
                "mime_type": "application/octet-stream",
                "local_path": str(media_path),
                "status": "ready",
            }
        )
        status, body = self.request("/v1/media/same-media?account_id=beta")
        self.assertEqual(status, 200)
        self.assertEqual(body, b"media-bytes")
        with self.assertRaises(urllib.error.HTTPError) as caught:
            self.request("/v1/media/same-media?account_id=alpha")
        self.assertEqual(caught.exception.code, 404)
        _, page = self.request("/v1/events/poll?after=0&account_id=beta&limit=200")
        media_events = [item["event_id"] for item in page["events"] if item["event_type"] == "media.ready"]
        self.assertEqual(len(media_events), 1)
        _, ack = self.request("/v1/events/ack", method="POST", payload={"consumer_id": "efb-test", "event_ids": media_events})
        self.assertEqual(ack["acked_count"], 1)

    def test_send_idempotency_and_inline_media(self):
        payload = {"account_id": "alpha", "chat_id": "same-chat", "text": "hello", "client_request_id": "client-1"}
        _, first = self.request("/v1/send/text", method="POST", payload=payload, headers={"Idempotency-Key": "same-key"})
        _, second = self.request("/v1/send/text", method="POST", payload=payload, headers={"Idempotency-Key": "same-key"})
        self.assertEqual(first["send_id"], second["send_id"])
        image_payload = {
            "account_id": "beta",
            "chat_id": "same-chat",
            "filename": "upload.bin",
            "content_base64": base64.b64encode(b"inline-content").decode("ascii"),
        }
        status, image = self.request(
            "/v1/send/image",
            method="POST",
            payload=image_payload,
            headers={"Idempotency-Key": "same-image-key"},
        )
        self.assertEqual(status, 202)
        self.assertEqual(image["kind"], "image")
        _, repeated_image = self.request(
            "/v1/send/image", method="POST", payload=image_payload, headers={"Idempotency-Key": "same-image-key"}
        )
        self.assertEqual(repeated_image["send_id"], image["send_id"])
        with self.store.connection() as conn:
            self.assertEqual(conn.execute("SELECT COUNT(*) FROM media WHERE account_id='beta'").fetchone()[0], 1)
        file_path = self.temp_root / "forward.bin"
        file_path.write_bytes(b"forward")
        self.store.upsert_media(
            {
                "account_id": "alpha",
                "media_id": "forward-media",
                "filename": "forward.bin",
                "mime_type": "application/octet-stream",
                "local_path": str(file_path),
                "status": "ready",
            }
        )
        status, file_receipt = self.request(
            "/v1/send/file",
            method="POST",
            payload={"account_id": "alpha", "chat_id": "same-chat", "media_id": "forward-media", "filename": "forward.bin"},
        )
        self.assertEqual(status, 202)
        self.assertEqual(file_receipt["kind"], "file")
        with self.assertRaises(urllib.error.HTTPError) as caught:
            self.request("/v1/send/text", method="POST", payload={"account_id": "missing", "chat_id": "x", "text": "no"})
        self.assertEqual(caught.exception.code, 404)

    def test_unique_plain_text_echo_is_reconciled_before_message_event(self):
        _, receipt = self.request(
            "/v1/send/text",
            method="POST",
            payload={"account_id": "alpha", "chat_id": "same-chat", "text": "echo me"},
            headers={"Idempotency-Key": "echo-one"},
        )
        self.store.transition_send(
            receipt["send_id"],
            "submitted",
            details={"confirmed": False, "delivery_certainty": "pending_confirmation", "automatic_retry": False},
        )
        before = self.store.poll_events(after="0", limit=200)["next_cursor"]
        result = self.store.upsert_message(
            {
                "account_id": "alpha",
                "message_id": "wechat-echo-1",
                "chat_id": "same-chat",
                "type": "text",
                "direction": "outgoing",
                "created_at": utc_now(),
                "author": {"member_id": "self", "display_name": "Self", "is_self": True},
                "text": "echo me",
            }
        )
        self.assertEqual(result, "created")
        page = self.store.poll_events(after=before, limit=20)
        self.assertEqual([event["event_type"] for event in page["events"]], ["send.updated", "message.created"])
        self.assertEqual(page["events"][0]["payload"]["send"]["echo_message_id"], "wechat-echo-1")
        with self.store.connection() as conn:
            row = conn.execute(
                "SELECT status, echo_message_id FROM outbox WHERE send_id=?", (receipt["send_id"],)
            ).fetchone()
        self.assertEqual(row["status"], "sent")
        self.assertEqual(row["echo_message_id"], "wechat-echo-1")

    def test_wrong_chat_text_echo_does_not_confirm_submission(self):
        self.store.upsert_chat(
            {
                "account_id": "alpha",
                "chat_id": "other-chat",
                "type": "private",
                "display_name": "Other Contact",
            }
        )
        receipt = self.store.queue_send(
            "text",
            {"account_id": "alpha", "chat_id": "same-chat", "text": "same payload"},
            idempotency_key="echo-wrong-chat",
        )
        self.store.transition_send(
            receipt["send_id"],
            "submitted",
            details={"delivery_certainty": "pending_confirmation", "automatic_retry": False},
        )
        self.store.upsert_message(
            {
                "account_id": "alpha",
                "message_id": "wrong-chat-echo",
                "chat_id": "other-chat",
                "type": "text",
                "direction": "outgoing",
                "created_at": utc_now(),
                "author": {"member_id": "self", "display_name": "Self", "is_self": True},
                "text": "same payload",
            }
        )
        with self.store.connection() as conn:
            row = conn.execute(
                "SELECT status, echo_message_id FROM outbox WHERE send_id=?", (receipt["send_id"],)
            ).fetchone()
        self.assertEqual(row["status"], "submitted")
        self.assertEqual(row["echo_message_id"], "")

    def test_ambiguous_text_echo_is_not_guessed(self):
        receipts = []
        for key in ("echo-ambiguous-a", "echo-ambiguous-b"):
            receipt = self.store.queue_send(
                "text",
                {"account_id": "alpha", "chat_id": "same-chat", "text": "same text"},
                idempotency_key=key,
            )
            self.store.transition_send(
                receipt["send_id"],
                "submitted",
                details={"confirmed": False, "delivery_certainty": "pending_confirmation", "automatic_retry": False},
            )
            receipts.append(receipt)
        self.store.upsert_message(
            {
                "account_id": "alpha",
                "message_id": "wechat-ambiguous-1",
                "chat_id": "same-chat",
                "type": "text",
                "direction": "outgoing",
                "created_at": utc_now(),
                "author": {"member_id": "self", "display_name": "Self", "is_self": True},
                "text": "same text",
            }
        )
        with self.store.connection() as conn:
            rows = [
                conn.execute(
                    "SELECT status, echo_message_id FROM outbox WHERE send_id=?", (receipt["send_id"],)
                ).fetchone()
                for receipt in receipts
            ]
        self.assertEqual([row["echo_message_id"] for row in rows], ["", ""])
        self.assertEqual([row["status"] for row in rows], ["submitted", "submitted"])

    def test_idempotency_key_reuse_with_different_account_or_request_conflicts(self):
        first_payload = {"account_id": "alpha", "chat_id": "same-chat", "text": "hello"}
        _, first = self.request(
            "/v1/send/text",
            method="POST",
            payload=first_payload,
            headers={"Idempotency-Key": "scope-key"},
        )
        self.assertEqual(first["account_id"], "alpha")
        with self.assertRaises(urllib.error.HTTPError) as cross_account:
            self.request(
                "/v1/send/text",
                method="POST",
                payload={"account_id": "beta", "chat_id": "same-chat", "text": "hello"},
                headers={"Idempotency-Key": "scope-key"},
            )
        self.assertEqual(cross_account.exception.code, 409)
        with self.assertRaises(urllib.error.HTTPError) as changed_request:
            self.request(
                "/v1/send/text",
                method="POST",
                payload={"account_id": "alpha", "chat_id": "same-chat", "text": "changed"},
                headers={"Idempotency-Key": "scope-key"},
            )
        self.assertEqual(changed_request.exception.code, 409)


class NormalizationRegressionTest(unittest.TestCase):
    def setUp(self):
        self.temp_root = CORE_ROOT / ".tmp" / f"normalize-test-{uuid.uuid4().hex}"
        self.temp_root.mkdir(parents=True)
        self.accounts = [
            parse_account({"account_id": "alpha", "display_name": "Alpha", "runtime_dir": "runtime/accounts/alpha"}, root=self.temp_root),
            parse_account({"account_id": "beta", "display_name": "Beta", "runtime_dir": "runtime/accounts/beta"}, root=self.temp_root),
        ]
        self.store = CoreStore(self.temp_root / "core.sqlite")
        for account in self.accounts:
            self.store.upsert_account(account.account_id, account.display_name, state="online")
            self._staging_fixture(account)

    def tearDown(self):
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def _staging_fixture(self, account):
        account.memory_db.parent.mkdir(parents=True, exist_ok=True)
        with sqlite_connection(account.memory_db) as conn:
            conn.executescript(
                """
                CREATE TABLE chats (username TEXT PRIMARY KEY, display_name TEXT, is_group INTEGER, updated_at TEXT, message_table TEXT);
                CREATE TABLE messages (
                    message_uid TEXT PRIMARY KEY, chat_username TEXT, chat_display_name TEXT, message_table TEXT,
                    local_id INTEGER, server_id INTEGER, type_label TEXT, create_time INTEGER, source TEXT,
                    message_content TEXT, compress_content TEXT, real_sender_id INTEGER, origin_source INTEGER
                );
                CREATE TABLE message_media (message_uid TEXT PRIMARY KEY, media_path TEXT, thumb_path TEXT, mime_type TEXT, status TEXT);
                """
            )
            conn.execute("INSERT INTO chats VALUES (?, ?, 1, ?, ?)", ("same@chatroom", "Research Group", "2026-08-31T08:00:00Z", "Msg_same"))
            conn.execute("INSERT INTO chats VALUES (?, ?, 0, ?, ?)", ("member-a", "Member A", "2026-08-31T08:00:01Z", "Msg_private"))
            conn.execute(
                "INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("same-message", "same@chatroom", "Research Group", "Msg_same", 1, 1, "text", 1725091200, "", "member-a:\nHello", "", 1, 0),
            )
            conn.execute(
                "INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("same-image", "same@chatroom", "Research Group", "Msg_same", 2, 2, "image", 1725091201, "", "member-a:\n<msg><img /></msg>", "", 1, 0),
            )
            conn.execute(
                "INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("private-incoming", "member-a", "Member A", "Msg_private", 3, 3, "text", 1725091202, "", "Hello privately", "", 1, 0),
            )
            conn.execute(
                "INSERT INTO messages VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("private-outgoing", "member-a", "Member A", "Msg_private", 4, 4, "text", 1725091203, "", "My reply", "", 0, 1),
            )
            media_path = account.runtime_dir / "media" / "same-image.png"
            media_path.parent.mkdir(parents=True, exist_ok=True)
            media_path.write_bytes(b"fixture-image")
            conn.execute(
                "INSERT INTO message_media VALUES (?, ?, ?, ?, ?)",
                ("same-image", "media/same-image.png", "", "image/png", "ready"),
            )
        contact_db = account.decrypted_dir / "contact" / "contact.db"
        contact_db.parent.mkdir(parents=True, exist_ok=True)
        with sqlite_connection(contact_db) as conn:
            conn.executescript(
                """
                CREATE TABLE contact (id INTEGER PRIMARY KEY, username TEXT, remark TEXT, nick_name TEXT, alias TEXT);
                CREATE TABLE chat_room (id INTEGER PRIMARY KEY, username TEXT, ext_buffer BLOB);
                CREATE TABLE chatroom_member (room_id INTEGER, member_id INTEGER);
                """
            )
            conn.execute("INSERT INTO contact VALUES (1, 'member-a', '', 'Member A', 'membera')")
            conn.execute("INSERT INTO chat_room VALUES (1, 'same@chatroom', ?)", (b"\n\x08member-a\x12\x07Group A",))
            conn.execute("INSERT INTO chatroom_member VALUES (1, 1)")

    def test_two_account_regression_preserves_chat_message_contact_and_member_visibility(self):
        first = import_account(self.accounts[0], self.store)
        second = import_account(self.accounts[1], self.store)
        self.assertEqual(first["chats"], 2)
        self.assertEqual(first["messages"], 4)
        self.assertEqual(first["contacts"], 1)
        self.assertGreaterEqual(first["members"], 1)
        self.assertEqual(first["media"], 1)
        with self.store.connection() as conn:
            messages = conn.execute("SELECT account_id, message_id FROM messages ORDER BY account_id").fetchall()
            members = conn.execute("SELECT account_id, chat_id, member_id FROM chat_members ORDER BY account_id").fetchall()
            incoming = conn.execute(
                "SELECT direction, author_json FROM messages WHERE account_id='alpha' AND message_id='private-incoming'"
            ).fetchone()
            outgoing = conn.execute(
                "SELECT direction, author_json FROM messages WHERE account_id='alpha' AND message_id='private-outgoing'"
            ).fetchone()
            image = conn.execute(
                "SELECT media_id, filename, mime_type FROM messages WHERE account_id='alpha' AND message_id='same-image'"
            ).fetchone()
            media = conn.execute("SELECT account_id, media_id, local_path FROM media ORDER BY account_id").fetchall()
        identities = {(row["account_id"], row["message_id"]) for row in messages}
        self.assertEqual(len(messages), 8)
        self.assertIn(("alpha", "same-message"), identities)
        self.assertIn(("beta", "same-message"), identities)
        self.assertIn(("alpha", "same-image"), identities)
        self.assertIn(("beta", "same-image"), identities)
        self.assertEqual({(row["account_id"], row["chat_id"], row["member_id"]) for row in members}, {("alpha", "same@chatroom", "member-a"), ("beta", "same@chatroom", "member-a")})
        self.assertEqual(incoming["direction"], "incoming")
        incoming_author = json.loads(incoming["author_json"])
        self.assertEqual(incoming_author["member_id"], "member-a")
        self.assertFalse(incoming_author["is_self"])
        self.assertEqual(outgoing["direction"], "outgoing")
        outgoing_author = json.loads(outgoing["author_json"])
        self.assertEqual(outgoing_author["member_id"], "self")
        self.assertTrue(outgoing_author["is_self"])
        self.assertEqual((image["media_id"], image["filename"], image["mime_type"]), ("same-image", "same-image.png", "image/png"))
        self.assertEqual({(row["account_id"], row["media_id"]) for row in media}, {("alpha", "same-image"), ("beta", "same-image")})
        self.assertTrue(all(Path(row["local_path"]).exists() for row in media))
        page = self.store.poll_events(after="0", limit=200)
        image_events = [
            event
            for event in page["events"]
            if event["event_type"] == "message.created"
            and event["payload"].get("message", {}).get("message_id") == "same-image"
        ]
        self.assertEqual(len(image_events), 2)
        self.assertTrue(all(event["payload"]["message"]["media_id"] == "same-image" for event in image_events))
        self.assertEqual(second["messages"], 4)
        self.assertEqual(second["media"], 1)
        with self.store.connection() as conn:
            before_chat_events = conn.execute(
                "SELECT COUNT(*) FROM events WHERE event_type='chat.updated'"
            ).fetchone()[0]
        import_account(self.accounts[0], self.store)
        import_account(self.accounts[1], self.store)
        with self.store.connection() as conn:
            after_chat_events = conn.execute(
                "SELECT COUNT(*) FROM events WHERE event_type='chat.updated'"
            ).fetchone()[0]
        self.assertEqual(after_chat_events, before_chat_events)


class StableMessageIdentityRegressionTest(unittest.TestCase):
    def setUp(self):
        self.temp_root = CORE_ROOT / ".tmp" / f"stable-message-{uuid.uuid4().hex}"
        self.temp_root.mkdir(parents=True)
        self.account = parse_account(
            {
                "account_id": "alpha",
                "display_name": "Alpha",
                "runtime_dir": str(self.temp_root / "runtime" / "alpha"),
            },
            root=self.temp_root,
        )
        self.raw_db = self.temp_root / "raw-message.db"
        with sqlite3.connect(self.raw_db) as conn:
            conn.executescript(
                """
                CREATE TABLE Msg_stable (
                    local_id INTEGER, server_id INTEGER, local_type INTEGER, sort_seq INTEGER,
                    real_sender_id INTEGER, create_time INTEGER, status INTEGER,
                    upload_status INTEGER, download_status INTEGER, server_seq INTEGER,
                    origin_source INTEGER, source TEXT, message_content TEXT,
                    compress_content TEXT, packed_info_data BLOB, WCDB_CT_message_content INTEGER
                );
                """
            )
            conn.execute(
                "INSERT INTO Msg_stable VALUES (100, 0, 1, 10, 0, 1725091200, 1, 0, 0, 0, 1, '', 'stable hello', '', NULL, 0)"
            )
        self.account.memory_db.parent.mkdir(parents=True, exist_ok=True)
        with sqlite3.connect(self.account.memory_db) as conn:
            init_memory_db(conn)
            ingest_chat(conn, "stable-chat", "Msg_stable", self.raw_db, {}, {})
        self.store = CoreStore(self.temp_root / "core.sqlite")
        self.store.upsert_account("alpha", "Alpha", state="online")

    def tearDown(self):
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def test_server_id_ack_updates_same_staging_and_core_logical_message(self):
        import_account(self.account, self.store)
        with sqlite3.connect(self.account.memory_db) as conn:
            first_staging = conn.execute(
                "SELECT message_uid, server_id, status FROM messages WHERE local_id=100"
            ).fetchone()
        with self.store.connection() as conn:
            first_core = conn.execute(
                "SELECT message_id FROM messages WHERE account_id='alpha' AND chat_id='stable-chat' AND source_local_id='100'"
            ).fetchone()
            created_before = conn.execute(
                "SELECT COUNT(*) FROM events WHERE event_type='message.created' AND account_id='alpha'"
            ).fetchone()[0]

        with sqlite3.connect(self.raw_db) as conn:
            conn.execute(
                "UPDATE Msg_stable SET server_id=987654321, status=2, server_seq=77 WHERE local_id=100"
            )
        with sqlite3.connect(self.account.memory_db) as conn:
            init_memory_db(conn)
            ingest_chat(conn, "stable-chat", "Msg_stable", self.raw_db, {}, {})
        second = import_account(self.account, self.store)

        with sqlite3.connect(self.account.memory_db) as conn:
            staging_rows = conn.execute(
                "SELECT message_uid, server_id, status, server_seq FROM messages WHERE local_id=100"
            ).fetchall()
        with self.store.connection() as conn:
            core_rows = conn.execute(
                """
                SELECT message_id, vendor_json FROM messages
                WHERE account_id='alpha' AND chat_id='stable-chat' AND source_local_id='100'
                """
            ).fetchall()
            created_after = conn.execute(
                "SELECT COUNT(*) FROM events WHERE event_type='message.created' AND account_id='alpha'"
            ).fetchone()[0]

        self.assertEqual(len(staging_rows), 1)
        self.assertEqual(staging_rows[0][0], first_staging[0])
        self.assertEqual(staging_rows[0][1:], (987654321, 2, 77))
        self.assertEqual(len(core_rows), 1)
        self.assertEqual(core_rows[0]["message_id"], first_core["message_id"])
        self.assertEqual(json.loads(core_rows[0]["vendor_json"])["source_server_id"], 987654321)
        self.assertEqual(created_after, created_before)
        self.assertEqual(second["messages"], 1)

    def test_existing_staging_duplicates_preserve_oldest_uid_and_migrate_media(self):
        with sqlite3.connect(self.account.memory_db) as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("DROP INDEX IF EXISTS idx_messages_source_identity")
            original = conn.execute("SELECT * FROM messages WHERE local_id=100").fetchone()
            identity = str(original["source_identity"])
            conn.execute(
                """
                INSERT INTO messages (
                    message_uid, source_identity, chat_username, chat_display_name, message_table,
                    source_message_db, local_id, server_id, local_type, base_type, app_subtype,
                    type_label, sort_seq, real_sender_id, create_time, status, upload_status,
                    download_status, server_seq, origin_source, source, message_content,
                    compress_content, content_sha256, packed_info_sha256, ingested_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "newer-duplicate-uid", identity, "stable-chat", "stable-chat", "Msg_stable",
                    str(self.raw_db), 100, 123456789, 1, 1, 0, "text", 10, 0, 1725091200,
                    2, 1, 0, 99, 1, "", "stable hello", "", "body-new", None,
                    "2026-09-02T00:00:00+00:00",
                ),
            )
            conn.execute(
                """
                INSERT INTO message_media (
                    message_uid, chat_username, local_id, media_type, media_path,
                    mime_type, status, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    "newer-duplicate-uid", "stable-chat", 100, "image", "media/dup.jpg",
                    "image/jpeg", "ready", "2026-09-02T00:00:00+00:00",
                ),
            )
            canonical_uid = str(original["message_uid"])
            init_memory_db(conn)
            rows = conn.execute(
                "SELECT message_uid, server_id, status, server_seq FROM messages WHERE source_identity=?",
                (identity,),
            ).fetchall()
            media = conn.execute("SELECT message_uid, status FROM message_media WHERE local_id=100").fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["message_uid"], canonical_uid)
        self.assertEqual((rows[0]["server_id"], rows[0]["status"], rows[0]["server_seq"]), (123456789, 2, 99))
        self.assertEqual([(row["message_uid"], row["status"]) for row in media], [(canonical_uid, "ready")])

    def test_existing_core_duplicates_collapse_to_oldest_message_id(self):
        self.store.upsert_chat(
            {"account_id": "alpha", "chat_id": "stable-chat", "type": "private", "display_name": "Stable"}
        )
        self.store.upsert_message(
            {
                "account_id": "alpha",
                "message_id": "canonical-old",
                "chat_id": "stable-chat",
                "type": "text",
                "direction": "outgoing",
                "created_at": "2026-09-01T00:00:00Z",
                "author": {"member_id": "self", "display_name": "Self", "is_self": True},
                "text": "old",
                "vendor_specific": {"source_local_id": 100, "source_server_id": 0, "source_message_table": "Msg_stable"},
            }
        )
        with self.store.connection() as conn:
            conn.execute("DROP INDEX IF EXISTS idx_messages_source_identity")
            canonical = conn.execute(
                "SELECT * FROM messages WHERE account_id='alpha' AND message_id='canonical-old'"
            ).fetchone()
            values = [canonical[name] for name in canonical.keys()]
            columns = list(canonical.keys())
            values[columns.index("message_id")] = "duplicate-new"
            values[columns.index("text")] = "new"
            values[columns.index("vendor_json")] = json.dumps(
                {"source_local_id": 100, "source_server_id": 999, "source_message_table": "Msg_stable"}
            )
            placeholders = ",".join("?" for _ in columns)
            conn.execute(
                f"INSERT INTO messages ({','.join(columns)}) VALUES ({placeholders})",
                tuple(values),
            )
        reopened = CoreStore(self.store.db_path)
        with reopened.connection() as conn:
            rows = conn.execute(
                "SELECT message_id, text, vendor_json FROM messages WHERE account_id='alpha' AND chat_id='stable-chat' AND source_local_id='100'"
            ).fetchall()
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["message_id"], "canonical-old")
        self.assertEqual(json.loads(rows[0]["vendor_json"])["source_server_id"], 999)


class AgentWechatRegistryTest(unittest.TestCase):
    def test_runtime_registry_maps_agent_provider_without_exposing_token_path(self):
        root = CORE_ROOT / ".tmp" / f"agent-registry-{uuid.uuid4().hex}"
        registry_path = root / "config" / "wechat-runtime" / "accounts.json"
        safe = "work-01234567"
        account = parse_runtime_account(
            {
                "id": "work",
                "display_name": "Work",
                "username": f"agent_{safe}",
                "uid": None,
                "display": "isolated",
                "home": f"/config/agent-wechat/{safe}/home",
                "enabled": True,
                "autostart": True,
                "legacy": False,
                "runtime_provider": "agent_wechat",
                "agent_wechat": {
                    "image": "ghcr.io/thisnick/agent-wechat:0.11.15",
                    "container_name": f"wechat-agent-{safe}",
                    "token_file": f"/config/agent-wechat/{safe}/auth-token",
                },
            },
            root=root,
            registry_path=registry_path,
        )
        self.assertEqual(account.runtime_provider, "agent_wechat")
        self.assertEqual(account.runtime["sender_driver"], "agent_wechat")
        self.assertEqual(account.runtime["agent_wechat_base_url"], f"http://wechat-agent-{safe}:6174")
        self.assertEqual(
            Path(account.runtime["agent_wechat_token_file"]),
            root / "config" / "agent-wechat" / safe / "auth-token",
        )
        self.assertNotIn("agent_wechat_token_file", account.public_runtime())
        self.assertTrue(account.public_runtime()["sender_capabilities"]["file"])
        legacy = parse_account(
            {
                "account_id": "legacy",
                "display_name": "Legacy",
                "runtime_dir": str(root / "runtime" / "legacy"),
                "runtime": {"runtime_provider": "legacy"},
            },
            root=root,
        )
        self.assertFalse(legacy.public_runtime()["sender_capabilities"]["file"])
        self.assertFalse(account.runtime["native_driver_enabled"])

    def test_native_driver_detection_is_fail_closed_by_default(self):
        with patch.dict("os.environ", {"WECHAT_NATIVE_DRIVER_SOCKET": ""}, clear=False):
            native = sender_capabilities()["drivers"]["native"]
        self.assertFalse(native["configured"])
        self.assertFalse(native["bridge_detected"])
        self.assertFalse(native["available"])
        self.assertFalse(native["text"])

    def test_agent_server_health_failure_degrades_core_account_state(self):
        root = CORE_ROOT / ".tmp" / f"agent-health-{uuid.uuid4().hex}"
        root.mkdir(parents=True)
        try:
            account = parse_account(
                {
                    "account_id": "alpha",
                    "display_name": "Alpha",
                    "runtime_dir": str(root / "runtime" / "alpha"),
                    "runtime": {
                        "runtime_provider": "agent_wechat",
                        "sender_driver": "agent_wechat",
                        "sender_enabled": True,
                    },
                },
                root=root,
            )
            registry = AccountRegistry([account], root / "accounts.json")
            store = CoreStore(root / "core.sqlite")
            service = CoreService(root=root, registry=registry, store=store)
            service._apply_runtime_status(
                {
                    "account_id": "alpha",
                    "display_name": "Alpha",
                    "runtime_provider": "agent_wechat",
                    "running": True,
                    "container_running": True,
                    "agent_server_healthy": False,
                    "runtime_health": "degraded",
                    "health_error": "health timeout",
                    "pids": [],
                    "windows": [],
                }
            )
            stored = store.account("alpha")
            self.assertIsNotNone(stored)
            assert stored is not None
            self.assertEqual(stored["state"], "degraded")
            self.assertFalse(stored["runtime"]["agent_server_healthy"])
            self.assertEqual(stored["runtime"]["health_error"], "health timeout")
        finally:
            shutil.rmtree(root, ignore_errors=True)


class AgentWechatKeyImportTest(unittest.TestCase):
    def setUp(self):
        self.temp_root = CORE_ROOT / ".tmp" / f"agent-keys-{uuid.uuid4().hex}"
        self.source_db = self.temp_root / "home" / "Documents" / "xwechat_files" / "wxid_alpha" / "db_storage"
        (self.source_db / "session").mkdir(parents=True)
        (self.source_db / "message").mkdir(parents=True)
        self.session_db = self.source_db / "session" / "session.db"
        self.message_db = self.source_db / "message" / "message_0.db"
        self.session_db.write_bytes(bytes.fromhex("00112233445566778899aabbccddeeff") + b"x" * 4096)
        self.message_db.write_bytes(bytes.fromhex("ffeeddccbbaa99887766554433221100") + b"y" * 4096)
        self.credentials = [
            {
                "account_dir": "wxid_alpha",
                "db_name": "session.db",
                "hex_key": "11" * 32,
                "verified_at": "2026-09-01T01:00:00Z",
            },
            {
                "account_dir": "wxid_alpha",
                "db_name": "message_0.db",
                "hex_key": "22" * 32,
                "verified_at": "2026-09-01T01:00:00Z",
            },
            # A newer unrelated account must never be selected when the live
            # db_storage path identifies wxid_alpha.
            {
                "account_dir": "wxid_beta",
                "db_name": "session.db",
                "hex_key": "33" * 32,
                "verified_at": "2026-09-01T02:00:00Z",
            },
        ]
        self.account = parse_account(
            {
                "account_id": "alpha",
                "display_name": "Alpha",
                "source_db_dir": str(self.source_db),
                "runtime_dir": str(self.temp_root / "runtime"),
                "keys_file": str(self.temp_root / "runtime" / "keys.json"),
                "runtime": {
                    "runtime_provider": "agent_wechat",
                },
            },
            root=self.temp_root,
        )

    def tearDown(self):
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def test_import_uses_matching_account_dir_and_existing_core_key_format(self):
        result = import_agent_wechat_keys(self.account, credentials=self.credentials)
        self.assertEqual(result["returncode"], 0)
        self.assertEqual(result["account_dir"], "wxid_alpha")
        payload = json.loads(self.account.keys_file.read_text(encoding="utf-8"))
        self.assertEqual(payload["session/session.db"]["enc_key"], "11" * 32)
        self.assertEqual(payload["session/session.db"]["salt"], "00112233445566778899aabbccddeeff")
        self.assertEqual(payload["message/message_0.db"]["enc_key"], "22" * 32)
        self.assertNotEqual(payload["session/session.db"]["enc_key"], "33" * 32)
        self.assertEqual(payload["_db_dir"], str(self.source_db))

    def test_image_media_credential_lands_in_media_config_not_db_keys(self):
        credentials = self.credentials + [
            {
                "account_dir": "wxid_alpha",
                "db_name": "_image_aes",
                "hex_key": "0a1b2c3d4e5f60718293a4b5c6d7e8f9",
                "verified_at": "2026-09-01T01:00:00Z",
            },
            {
                "account_dir": "wxid_alpha",
                "db_name": "_image_xor",
                "hex_key": "88",
                "verified_at": "2026-09-01T01:00:00Z",
            },
        ]
        result = import_agent_wechat_keys(self.account, credentials=credentials)
        self.assertEqual(result["returncode"], 0)
        self.assertTrue(result["media_key"]["present"])
        self.assertTrue(result["media_key"]["written"])
        config = json.loads(self.account.config_file.read_text(encoding="utf-8"))
        self.assertEqual(config["image_aes_key"], "0a1b2c3d4e5f60718293a4b5c6d7e8f9")
        self.assertEqual(config["image_xor_key"], "0x88")
        db_payload = json.loads(self.account.keys_file.read_text(encoding="utf-8"))
        self.assertNotIn("_image_aes", db_payload)
        self.assertNotIn("_image_xor", db_payload)

    def test_unverified_or_malformed_image_credential_is_ignored(self):
        credentials = self.credentials + [
            {
                "account_dir": "wxid_alpha",
                "db_name": "_image_aes",
                "hex_key": "0a1b2c3d4e5f60718293a4b5c6d7e8f9",
                "verified_at": "",
            },
            {
                "account_dir": "wxid_alpha",
                "db_name": "_image_aes",
                "hex_key": "tooshort",
                "verified_at": "2026-09-01T03:00:00Z",
            },
        ]
        result = import_agent_wechat_keys(self.account, credentials=credentials)
        self.assertEqual(result["returncode"], 0)
        self.assertFalse(result["media_key"]["present"])
        self.assertFalse(self.account.config_file.exists())

    def test_missing_image_credential_never_clobbers_existing_media_config(self):
        self.account.config_file.parent.mkdir(parents=True, exist_ok=True)
        self.account.config_file.write_text(
            json.dumps({"image_aes_key": "ff" * 16, "keep": True}) + "\n", encoding="utf-8"
        )
        result = import_agent_wechat_keys(self.account, credentials=self.credentials)
        self.assertEqual(result["returncode"], 0)
        self.assertFalse(result["media_key"]["present"])
        self.assertFalse(result["media_key"]["written"])
        config = json.loads(self.account.config_file.read_text(encoding="utf-8"))
        self.assertEqual(config, {"image_aes_key": "ff" * 16, "keep": True})


class AgentWechatSenderRoutingTest(unittest.TestCase):
    def setUp(self):
        self.temp_root = CORE_ROOT / ".tmp" / f"agent-sender-{uuid.uuid4().hex}"
        self.temp_root.mkdir(parents=True)
        self.store = CoreStore(self.temp_root / "core.sqlite")
        accounts = []
        for account_id, token in (("alpha", "token-alpha"), ("beta", "token-beta")):
            token_file = self.temp_root / f"{account_id}.token"
            token_file.write_text(token + "\n", encoding="utf-8")
            account = parse_account(
                {
                    "account_id": account_id,
                    "display_name": account_id.title(),
                    "runtime_dir": f"runtime/accounts/{account_id}",
                    "runtime": {
                        "runtime_provider": "agent_wechat",
                        "sender_driver": "agent_wechat",
                        "sender_enabled": True,
                        "agent_wechat_base_url": f"http://wechat-agent-{account_id}:6174",
                        "agent_wechat_token_file": str(token_file),
                    },
                },
                root=self.temp_root,
            )
            accounts.append(account)
            self.store.upsert_account(account_id, account_id.title(), state="online")
            self.store.upsert_chat(
                {
                    "account_id": account_id,
                    "chat_id": f"chat-{account_id}",
                    "type": "private",
                    "display_name": f"Chat {account_id}",
                }
            )
        self.registry = AccountRegistry(accounts, self.temp_root / "accounts.json")
        self.sender = AccountSender(self.registry, self.store, root=self.temp_root)

    def tearDown(self):
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def test_distinct_accounts_use_distinct_agent_hosts_tokens_and_chat_ids(self):
        self.store.queue_send("text", {"account_id": "alpha", "chat_id": "chat-alpha", "text": "hello-a"})
        self.store.queue_send("text", {"account_id": "beta", "chat_id": "chat-beta", "text": "hello-b"})
        calls: list[tuple[str, str, str, str]] = []
        calls_lock = threading.Lock()

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self, _limit=-1):
                return b'{"success":true}'

        def fake_urlopen(request, timeout=0):
            del timeout
            if "/api/chats/" in request.full_url and "/open?" in request.full_url:
                with calls_lock:
                    calls.append(
                        (
                            request.full_url,
                            str(request.get_header("Authorization") or ""),
                            "",
                            "",
                        )
                    )
                return Response()
            payload = json.loads(request.data.decode("utf-8"))
            with calls_lock:
                calls.append(
                    (
                        request.full_url,
                        str(request.get_header("Authorization") or ""),
                        str(payload.get("chatId") or ""),
                        str(payload.get("text") or ""),
                    )
                )
            return Response()

        with patch("core.sender.urllib.request.urlopen", side_effect=fake_urlopen):
            result = self.sender.process_pending()

        self.assertEqual(
            result,
            {"processed": 2, "submitted": 2, "sent": 0, "failed": 0, "uncertain": 0, "deferred": 0},
        )
        self.assertEqual(
            set(calls),
            {
                (
                    "http://wechat-agent-alpha:6174/api/chats/chat-alpha/open?clearUnreads=false",
                    "Bearer token-alpha",
                    "",
                    "",
                ),
                ("http://wechat-agent-alpha:6174/api/messages/send", "Bearer token-alpha", "chat-alpha", "hello-a"),
                (
                    "http://wechat-agent-beta:6174/api/chats/chat-beta/open?clearUnreads=false",
                    "Bearer token-beta",
                    "",
                    "",
                ),
                ("http://wechat-agent-beta:6174/api/messages/send", "Bearer token-beta", "chat-beta", "hello-b"),
            },
        )

    def test_manual_desktop_defers_only_that_account_without_attempting_upstream(self):
        alpha = self.store.queue_send(
            "text",
            {"account_id": "alpha", "chat_id": "chat-alpha", "text": "manual-busy"},
            idempotency_key="manual-busy-alpha",
        )
        beta = self.store.queue_send(
            "text",
            {"account_id": "beta", "chat_id": "chat-beta", "text": "beta-still-sends"},
            idempotency_key="manual-busy-beta",
        )
        calls: list[str] = []

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self, _limit=-1):
                return b'{"success":true}'

        @contextmanager
        def gui_lease(account_id):
            yield account_id != "alpha"

        def fake_urlopen(request, timeout=0):
            del timeout
            calls.append(request.full_url)
            return Response()

        with patch("core.sender.account_gui_lease", side_effect=gui_lease), patch(
            "core.sender.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            first = self.sender.process_pending()

        self.assertEqual(first["deferred"], 1)
        self.assertEqual(first["submitted"], 1)
        self.assertTrue(all("wechat-agent-beta" in url for url in calls))
        with self.store.connection() as conn:
            alpha_row = conn.execute(
                "SELECT status, attempt_count FROM outbox WHERE send_id=?", (alpha["send_id"],)
            ).fetchone()
            beta_row = conn.execute(
                "SELECT status, attempt_count FROM outbox WHERE send_id=?", (beta["send_id"],)
            ).fetchone()
        self.assertEqual((alpha_row["status"], alpha_row["attempt_count"]), ("accepted", 0))
        self.assertEqual((beta_row["status"], beta_row["attempt_count"]), ("submitted", 1))

        calls.clear()

        @contextmanager
        def available_gui_lease(_account_id):
            yield True

        with patch("core.sender.account_gui_lease", side_effect=available_gui_lease), patch(
            "core.sender.urllib.request.urlopen", side_effect=fake_urlopen
        ):
            second = self.sender.process_pending()

        self.assertEqual(second["submitted"], 1)
        self.assertEqual(second["deferred"], 0)
        self.assertTrue(all("wechat-agent-alpha" in url for url in calls))
        with self.store.connection() as conn:
            alpha_after = conn.execute(
                "SELECT status, attempt_count FROM outbox WHERE send_id=?", (alpha["send_id"],)
            ).fetchone()
        self.assertEqual((alpha_after["status"], alpha_after["attempt_count"]), ("submitted", 1))

    def test_agent_reply_is_rejected_before_upstream_call(self):
        self.store.queue_send(
            "text",
            {
                "account_id": "alpha",
                "chat_id": "chat-alpha",
                "text": "unsafe reply",
                "target_message_id": "message-1",
            },
        )
        with patch("core.sender.urllib.request.urlopen") as urlopen:
            result = self.sender.process_pending()
        self.assertEqual(result["failed"], 1)
        urlopen.assert_not_called()

    def test_agent_timeout_becomes_uncertain_and_is_never_auto_retried(self):
        self.store.queue_send(
            "text",
            {"account_id": "alpha", "chat_id": "chat-alpha", "text": "maybe-sent"},
            idempotency_key="uncertain-send",
        )
        class Response:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self, _limit=-1):
                return b'{"ok":true}'

        def fake_urlopen(request, timeout=0):
            del timeout
            if "/api/chats/" in request.full_url and "/open?" in request.full_url:
                return Response()
            raise TimeoutError("response timed out")

        with patch("core.sender.urllib.request.urlopen", side_effect=fake_urlopen):
            result = self.sender.process_pending()

        self.assertEqual(result["uncertain"], 1)
        self.assertEqual(result["failed"], 0)
        receipt = self.store.receipt_by_idempotency_key("uncertain-send")
        self.assertIsNotNone(receipt)
        assert receipt is not None
        self.assertEqual(receipt["status"], "uncertain")
        self.assertEqual(self.store.pending_sends(), [])
        page = self.store.poll_events(after="0", limit=200)
        updates = [
            item
            for item in page["events"]
            if item["event_type"] == "send.updated"
            and item["payload"].get("send", {}).get("send_id") == receipt["send_id"]
        ]
        final = updates[-1]["payload"]
        self.assertEqual(final["send"]["status"], "uncertain")
        self.assertEqual(final["error"]["code"], "agent_wechat_delivery_unknown")
        self.assertEqual(final["details"]["delivery_certainty"], "unknown")
        self.assertFalse(final["details"]["automatic_retry"])

    def test_agent_false_success_expires_from_submitted_to_uncertain_without_retry(self):
        receipt = self.store.queue_send(
            "text",
            {"account_id": "alpha", "chat_id": "chat-alpha", "text": "fsm-false-success"},
            idempotency_key="fsm-false-success",
        )

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self, _limit=-1):
                return b'{"success":true}'

        with patch("core.sender.urllib.request.urlopen", return_value=Response()) as first_urlopen:
            first = self.sender.process_pending()
        self.assertEqual(first["submitted"], 1)
        with self.store.connection() as conn:
            submitted = conn.execute(
                "SELECT status, attempt_count FROM outbox WHERE send_id=?", (receipt["send_id"],)
            ).fetchone()
            conn.execute(
                "UPDATE outbox SET updated_at='2000-01-01T00:00:00Z' WHERE send_id=?",
                (receipt["send_id"],),
            )
        self.assertEqual(submitted["status"], "submitted")
        self.assertEqual(submitted["attempt_count"], 1)

        self.assertEqual(self.store.expire_submitted_sends(max_age_seconds=5), 1)
        final = self.store.receipt_by_idempotency_key("fsm-false-success")
        self.assertIsNotNone(final)
        assert final is not None
        self.assertEqual(final["status"], "uncertain")
        self.assertEqual(final["delivery_certainty"], "unknown")
        self.assertFalse(final["automatic_retry"])
        self.assertEqual(self.store.pending_sends(), [])

        with patch("core.sender.urllib.request.urlopen") as second_urlopen:
            second = self.sender.process_pending()
        second_urlopen.assert_not_called()
        self.assertEqual(second["processed"], 0)
        self.assertEqual(first_urlopen.call_count, 2)
        with self.store.connection() as conn:
            attempt_count = conn.execute(
                "SELECT attempt_count FROM outbox WHERE send_id=?", (receipt["send_id"],)
            ).fetchone()[0]
        self.assertEqual(attempt_count, 1)

    def test_agent_image_and_file_payloads_preserve_media_metadata(self):
        image_path = self.temp_root / "photo.png"
        image_bytes = b"\x89PNG\r\n\x1a\nimage-payload"
        image_path.write_bytes(image_bytes)
        file_path = self.temp_root / "report.txt"
        file_bytes = b"report-payload"
        file_path.write_bytes(file_bytes)
        self.store.upsert_media(
            {
                "account_id": "alpha",
                "media_id": "image-1",
                "filename": "photo.png",
                "mime_type": "image/png",
                "local_path": str(image_path),
            }
        )
        self.store.upsert_media(
            {
                "account_id": "alpha",
                "media_id": "file-1",
                "filename": "report.txt",
                "mime_type": "text/plain",
                "local_path": str(file_path),
            }
        )
        self.store.queue_send(
            "image",
            {"account_id": "alpha", "chat_id": "chat-alpha", "media_id": "image-1"},
        )
        self.store.queue_send(
            "file",
            {"account_id": "alpha", "chat_id": "chat-alpha", "media_id": "file-1"},
        )
        payloads: list[dict[str, object]] = []

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self, _limit=-1):
                return b'{"success":true}'

        def fake_urlopen(request, timeout=0):
            del timeout
            if "/api/chats/" in request.full_url and "/open?" in request.full_url:
                return Response()
            payloads.append(json.loads(request.data.decode("utf-8")))
            return Response()

        with patch("core.sender.urllib.request.urlopen", side_effect=fake_urlopen):
            result = self.sender.process_pending()

        self.assertEqual(
            result,
            {"processed": 2, "submitted": 2, "sent": 0, "failed": 0, "uncertain": 0, "deferred": 0},
        )
        image_payload = next(item for item in payloads if "image" in item)
        file_payload = next(item for item in payloads if "file" in item)
        self.assertEqual(image_payload["chatId"], "chat-alpha")
        self.assertEqual(image_payload["image"]["data"], base64.b64encode(image_bytes).decode("ascii"))
        self.assertEqual(image_payload["image"]["mimeType"], "image/png")
        self.assertEqual(file_payload["chatId"], "chat-alpha")
        self.assertEqual(file_payload["file"]["data"], base64.b64encode(file_bytes).decode("ascii"))
        self.assertEqual(file_payload["file"]["filename"], "report.txt")

    def test_agent_chat_preopen_target_mismatch_fails_before_send_endpoint(self):
        self.store.queue_send(
            "text",
            {"account_id": "alpha", "chat_id": "chat-alpha", "text": "must-not-send"},
        )
        calls: list[str] = []

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self, _limit=-1):
                return b'{"ok":true,"username":"wrong-chat"}'

        def fake_urlopen(request, timeout=0):
            del timeout
            calls.append(request.full_url)
            return Response()

        with patch("core.sender.urllib.request.urlopen", side_effect=fake_urlopen):
            result = self.sender.process_pending()

        self.assertEqual(result["failed"], 1)
        self.assertEqual(
            calls,
            ["http://wechat-agent-alpha:6174/api/chats/chat-alpha/open?clearUnreads=false"],
        )


class SenderRoutingTest(unittest.TestCase):
    def setUp(self):
        self.temp_root = CORE_ROOT / ".tmp" / f"sender-test-{uuid.uuid4().hex}"
        self.temp_root.mkdir(parents=True)
        self.log_path = self.temp_root / "controller.jsonl"
        self.controller_path = self.temp_root / "fake_controller.py"
        self.controller_path.write_text(
            "import json\n"
            "import os\n"
            "import sys\n"
            "log_path = sys.argv[1]\n"
            "with open(log_path, 'a', encoding='utf-8') as f:\n"
            "    f.write(json.dumps({'display': os.environ.get('WECHAT_DISPLAY'), 'window_id': os.environ.get('WECHAT_WINDOW_ID'), 'args': sys.argv[2:]}) + '\\n')\n"
            "print(json.dumps({'ok': True}))\n",
            encoding="utf-8",
        )
        account = parse_account(
            {
                "account_id": "alpha",
                "display_name": "Alpha",
                "runtime_dir": "runtime/accounts/alpha",
                "runtime": {
                    "display": ":7",
                    "window_id": "12345",
                    "sender_enabled": True,
                    "controller_verifies_chat_target": True,
                    "controller_command": [sys.executable, str(self.controller_path), str(self.log_path)],
                },
            },
            root=self.temp_root,
        )
        self.registry = AccountRegistry([account], self.temp_root / "accounts.json")
        self.store = CoreStore(self.temp_root / "core.sqlite")
        self.store.upsert_account("alpha", "Alpha", state="online")
        self.store.upsert_chat(
            {"account_id": "alpha", "chat_id": "chat-1", "type": "private", "display_name": "Contact"}
        )
        self.sender = AccountSender(self.registry, self.store, root=self.temp_root)

    def tearDown(self):
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def _controller_calls(self):
        if not self.log_path.exists():
            return []
        return [json.loads(line) for line in self.log_path.read_text(encoding="utf-8").splitlines() if line]

    def test_sender_routes_display_window_and_account_to_controller(self):
        self.store.queue_send("text", {"account_id": "alpha", "chat_id": "chat-1", "text": "hello"})
        result = self.sender.process_pending()
        self.assertEqual(
            result,
            {"processed": 1, "submitted": 1, "sent": 0, "failed": 0, "uncertain": 0, "deferred": 0},
        )
        calls = self._controller_calls()
        self.assertEqual([call["args"][0] for call in calls], ["open", "paste", "submit"])
        self.assertTrue(all(call["display"] == ":7" for call in calls))
        self.assertTrue(all(call["window_id"] == "12345" for call in calls))
        self.assertEqual(self.store.pending_sends(), [])

    def test_unverified_native_reply_fails_without_touching_controller(self):
        self.store.queue_send(
            "text",
            {"account_id": "alpha", "chat_id": "chat-1", "text": "reply", "target_message_id": "msg-1"},
        )
        result = self.sender.process_pending()
        self.assertEqual(result["failed"], 1)
        self.assertEqual(self._controller_calls(), [])
        events = self.store.poll_events(after="0", limit=200)["events"]
        failures = [event for event in events if event["event_type"] == "send.updated" and event["payload"].get("error")]
        self.assertTrue(any("target_message_id" in event["payload"]["error"]["message"] for event in failures))

    def test_unverified_chat_target_fails_without_touching_controller(self):
        self.registry.require("alpha").runtime.pop("controller_verifies_chat_target")
        self.store.queue_send("text", {"account_id": "alpha", "chat_id": "chat-1", "text": "unsafe"})
        result = self.sender.process_pending()
        self.assertEqual(result["failed"], 1)
        self.assertEqual(self._controller_calls(), [])
        events = self.store.poll_events(after="0", limit=200)["events"]
        failures = [event for event in events if event["event_type"] == "send.updated" and event["payload"].get("error")]
        self.assertTrue(
            any("cannot verify the exact target chat" in event["payload"]["error"]["message"] for event in failures)
        )

    def test_multi_account_sender_refuses_global_window_discovery(self):
        unsafe = parse_account(
            {
                "account_id": "beta",
                "display_name": "Beta",
                "runtime_dir": "runtime/accounts/beta",
                "runtime": {
                    "display": ":7",
                    "sender_enabled": True,
                    "controller_verifies_chat_target": True,
                    "controller_command": [sys.executable, str(self.controller_path), str(self.log_path)],
                },
            },
            root=self.temp_root,
        )
        registry = AccountRegistry([self.registry.require("alpha"), unsafe], self.temp_root / "accounts.json")
        self.store.upsert_account("beta", "Beta", state="online")
        self.store.upsert_chat(
            {"account_id": "beta", "chat_id": "chat-2", "type": "private", "display_name": "Beta Contact"}
        )
        sender = AccountSender(registry, self.store, root=self.temp_root)
        self.store.queue_send("text", {"account_id": "beta", "chat_id": "chat-2", "text": "unsafe"})
        result = sender.process_pending()
        self.assertEqual(result["failed"], 1)
        self.assertEqual(self._controller_calls(), [])
        events = self.store.poll_events(after="0", limit=200)["events"]
        failures = [event for event in events if event["event_type"] == "send.updated" and event["payload"].get("error")]
        self.assertTrue(
            any("global WeChat window discovery is unsafe" in event["payload"]["error"]["message"] for event in failures)
        )

    def test_stale_sending_is_failed_instead_of_remaining_wedged(self):
        receipt = self.store.queue_send(
            "text",
            {"account_id": "alpha", "chat_id": "chat-1", "text": "possibly sent"},
        )
        self.store.transition_send(receipt["send_id"], "sending")
        with self.store.connection() as conn:
            conn.execute(
                "UPDATE outbox SET updated_at='2000-01-01T00:00:00Z' WHERE send_id=?",
                (receipt["send_id"],),
            )
        recovered = self.store.recover_stale_sends(max_age_seconds=30)
        self.assertEqual(recovered, 1)
        with self.store.connection() as conn:
            row = conn.execute("SELECT status, error, attempt_count FROM outbox WHERE send_id=?", (receipt["send_id"],)).fetchone()
        self.assertEqual(row["status"], "failed")
        self.assertIn("delivery state is unknown", row["error"])
        self.assertEqual(row["attempt_count"], 1)
        self.assertEqual(self.store.pending_sends(), [])


class KeyExtractionRoutingTest(unittest.TestCase):
    def test_scanner_command_requires_registered_pid_and_scopes_paths(self):
        root = CORE_ROOT / "key-extract-test-root"
        account = parse_account(
            {
                "account_id": "alpha",
                "source_db_dir": "config/alpha/db_storage",
                "keys_file": "runtime/accounts/alpha/keys.json",
                "runtime": {"pid": 4321},
            },
            root=root,
        )
        command = scanner_command(account, root=root)
        self.assertEqual(command[-2:], ["--pid", "4321"])
        self.assertIn(str(root / "config" / "alpha" / "db_storage"), command)
        missing_pid = parse_account({"account_id": "beta"}, root=root)
        with self.assertRaises(RegistryError):
            scanner_command(missing_pid, root=root)

    def test_scanner_command_accepts_runtime_pid_list_from_multi_account_runtime(self):
        root = CORE_ROOT / "key-extract-test-root"
        account = parse_account(
            {
                "account_id": "work.prod",
                "source_db_dir": "config/work/db_storage",
                "keys_file": "runtime/accounts/work.prod/keys.json",
                "runtime": {"pids": [4321, "4322", 4321]},
            },
            root=root,
        )
        command = scanner_command(account, root=root)
        self.assertEqual(command[-4:], ["--pid", "4321", "--pid", "4322"])


class MediaSyncRoutingTest(unittest.TestCase):
    def test_core_media_sync_disables_remote_sticker_downloads(self):
        account = parse_account(
            {
                "account_id": "alpha",
                "source_db_dir": "config/alpha/db_storage",
            },
            root=CORE_ROOT / "media-routing-test-root",
        )
        self.assertFalse(media_args(account).download_stickers)


def build_v2_dat(plain: bytes, aes_key_hex: str, head_len: int, xor_size: int, xor_byte: int) -> bytes:
    """Build a synthetic WeChat V2 ``.dat`` for decoder regression tests."""

    from Crypto.Cipher import AES
    from Crypto.Util.Padding import pad

    key = aes_key_hex.encode("ascii")[:16]
    aligned = head_len + (16 - head_len % 16) if head_len % 16 else head_len + 16
    head = plain[:head_len]
    tail = plain[len(plain) - xor_size:]
    middle = plain[head_len:len(plain) - xor_size]
    body = AES.new(key, AES.MODE_ECB).encrypt(pad(head, 16))
    assert len(body) == aligned
    return (
        b"\x07\x08V2\x08\x07"
        + struct.pack("<LL", head_len, xor_size)
        + b"\x01"
        + body
        + middle
        + bytes(b ^ xor_byte for b in tail)
    )


class MediaDatV2XorDerivationTest(unittest.TestCase):
    """The trailing XOR byte is account-specific and must be recoverable."""

    AES_KEY_HEX = "0123456789abcdef0123456789abcdef"

    def test_jpeg_recovers_non_default_xor_byte(self):
        plain = b"\xff\xd8\xff\xe0" + b"B" * 60 + b"\xff\xd9"
        data = build_v2_dat(plain, self.AES_KEY_HEX, 32, 2, 0xEE)
        decrypted, fmt = media_sync.decrypt_v2(data, self.AES_KEY_HEX, 0x88)
        self.assertEqual(fmt, "jpg")
        self.assertEqual(decrypted, plain)

    def test_jpeg_configured_xor_byte_still_used_first(self):
        plain = b"\xff\xd8\xff\xe0" + b"C" * 60 + b"\xff\xd9"
        data = build_v2_dat(plain, self.AES_KEY_HEX, 32, 2, 0x88)
        decrypted, fmt = media_sync.decrypt_v2(data, self.AES_KEY_HEX, 0x88)
        self.assertEqual((fmt, decrypted), ("jpg", plain))

    def test_png_recovers_non_default_xor_byte(self):
        plain = b"\x89PNG\r\n\x1a\n" + b"D" * 60 + b"\x49\x45\x4e\x44\xae\x42\x60\x82"
        data = build_v2_dat(plain, self.AES_KEY_HEX, 32, 8, 0x5A)
        decrypted, fmt = media_sync.decrypt_v2(data, self.AES_KEY_HEX, 0x88)
        self.assertEqual(fmt, "png")
        self.assertEqual(decrypted, plain)

    def test_wrong_aes_key_never_yields_an_image(self):
        plain = b"\xff\xd8\xff\xe0" + b"E" * 60 + b"\xff\xd9"
        data = build_v2_dat(plain, self.AES_KEY_HEX, 32, 2, 0xEE)
        decrypted, fmt = media_sync.decrypt_v2(data, "ffffffffffffffffffffffffffffffff", 0x88)
        self.assertIsNone(decrypted)
        self.assertIsNone(fmt)

    def test_derivation_requires_known_plaintext_tail(self):
        # A BMP has no deterministic EOF, so no byte may be invented for it.
        self.assertIsNone(
            media_sync.derive_xor_byte(b"\x00\x00" + bytes(range(2, 40)), b"BM" + b"\x00" * 30, 2)
        )


class SessionOnlyIngestTest(unittest.TestCase):
    def test_session_without_message_table_remains_visible_as_chat(self):
        with sqlite3.connect(":memory:") as conn:
            init_memory_db(conn)
            inserted = ingest_session_chats(
                conn,
                {"contact-a": {"type": 1, "last_timestamp": 123, "sort_timestamp": 456}},
                {"contact-a": {"display_name": "Contact A"}},
                set(),
                Path("session.db"),
            )
            row = conn.execute(
                "SELECT username, display_name, message_table FROM chats WHERE username=?",
                ("contact-a",),
            ).fetchone()
        self.assertEqual(inserted, 1)
        self.assertEqual(row, ("contact-a", "Contact A", ""))


class AccountStatusEventTest(unittest.TestCase):
    def test_timestamp_only_sync_updates_do_not_emit_duplicate_status_events(self):
        temp_root = CORE_ROOT / ".tmp" / f"status-event-test-{uuid.uuid4().hex}"
        temp_root.mkdir(parents=True)
        try:
            store = CoreStore(temp_root / "core.sqlite")
            store.upsert_account(
                "alpha",
                "Alpha",
                state="online",
                runtime={"display": ":1"},
                sync={"ok": True, "started_at": "2026-08-31T00:00:00Z", "elapsed_seconds": 1.0},
            )
            first = store.poll_events(after="0", limit=20, account_id="alpha")
            store.upsert_account(
                "alpha",
                "Alpha",
                state="online",
                runtime={"display": ":1"},
                sync={"ok": True, "started_at": "2026-08-31T00:01:00Z", "elapsed_seconds": 2.0},
            )
            second = store.poll_events(after="0", limit=20, account_id="alpha")
            self.assertEqual(len(first["events"]), 1)
            self.assertEqual(len(second["events"]), 1)
        finally:
            shutil.rmtree(temp_root, ignore_errors=True)


class ControllerLoginGuardTest(unittest.TestCase):
    def test_login_window_is_not_treated_as_chat_ready(self):
        self.assertFalse(chat_window_ready({"width": 280, "height": 380}))
        self.assertTrue(chat_window_ready({"width": 960, "height": 640}))

    def test_chat_search_is_never_shortened_and_open_fails_closed(self):
        self.assertEqual(chat_search_query("文件传输助手"), "文件传输助手")
        self.assertEqual(chat_search_query("PT站看片狂魔小群"), "PT站看片狂魔小群")
        with self.assertRaisesRegex(RuntimeError, "无法验证搜索结果"):
            open_chat("文件传输助手", 0.1)


class RegistryIsolationTest(unittest.TestCase):
    def test_account_ids_match_runtime_charset_and_default_paths_are_isolated(self):
        root = CORE_ROOT / "registry-test-root"
        first = parse_account({"account_id": "work.prod"}, root=root)
        second = parse_account({"account_id": "personal-2"}, root=root)
        self.assertEqual(first.account_id, "work.prod")
        self.assertNotEqual(first.runtime_dir, second.runtime_dir)
        self.assertNotEqual(first.memory_db, second.memory_db)
        self.assertNotEqual(first.media_dir, second.media_dir)
        with self.assertRaises(RegistryError):
            parse_account({"account_id": "bad/account"}, root=root)

    def test_package_a_registry_translates_home_and_discovers_live_source_db(self):
        temp_root = CORE_ROOT / ".tmp" / f"runtime-registry-{uuid.uuid4().hex}"
        registry_path = temp_root / "config" / "wechat-runtime" / "accounts.json"
        home = temp_root / "config" / "wechat-accounts" / "work" / "home"
        source_db = home / "Documents" / "xwechat_files" / "wxid_work" / "db_storage"
        source_db.mkdir(parents=True)
        registry_path.parent.mkdir(parents=True, exist_ok=True)
        registry_path.write_text(
            json.dumps(
                {
                    "version": 1,
                    "accounts": [
                        {
                            "id": "work.prod",
                            "username": "wx_work_prod",
                            "uid": 20000,
                            "display": ":1",
                            "home": "/config/wechat-accounts/work/home",
                            "enabled": True,
                            "legacy": False,
                        }
                    ],
                }
            ),
            encoding="utf-8",
        )
        try:
            registry = load_registry(registry_path, root=temp_root)
            account = registry.require("work.prod")
            self.assertEqual(account.runtime["source_home"], str(home))
            self.assertEqual(account.runtime["display_lock"], "/run/wechat-runtime/locks/display-_1.lock")
            resolved = resolve_runtime_account(account)
            self.assertEqual(resolved.source_db_dir, source_db)
            self.assertEqual(resolved.wechat_base_dir, source_db.parent)
        finally:
            shutil.rmtree(temp_root, ignore_errors=True)

    def test_runtime_pid_discovery_filters_by_uid_and_wechat_process(self):
        temp_root = CORE_ROOT / ".tmp" / f"fake-proc-{uuid.uuid4().hex}"
        try:
            wechat = temp_root / "101"
            other = temp_root / "102"
            wechat.mkdir(parents=True)
            other.mkdir(parents=True)
            (wechat / "comm").write_text("wechat\n", encoding="utf-8")
            (wechat / "cmdline").write_bytes(b"/usr/bin/wechat\x00")
            (other / "comm").write_text("python\n", encoding="utf-8")
            (other / "cmdline").write_bytes(b"python\x00worker.py\x00")
            uid = int(wechat.stat().st_uid)
            self.assertEqual(account_processes(uid, proc_root=temp_root), [101])
        finally:
            shutil.rmtree(temp_root, ignore_errors=True)


class LegacySyncRecoveryReuseTest(unittest.TestCase):
    def test_reindex_recovery_primitive_is_reusable_without_decrypt_dependencies(self):
        temp_root = CORE_ROOT / ".tmp" / f"repair-test-{uuid.uuid4().hex}"
        temp_root.mkdir(parents=True)
        try:
            db_path = temp_root / "memory.sqlite"
            with sqlite_connection(db_path) as conn:
                conn.execute("CREATE TABLE messages (id INTEGER PRIMARY KEY, body TEXT)")
                conn.execute("CREATE INDEX idx_messages_body ON messages(body)")
                conn.execute("INSERT INTO messages(body) VALUES ('hello')")
            result = repair_memory_indexes(db_path)
            self.assertTrue(result["ok"])
            self.assertEqual(result["integrity_check"], "ok")
        finally:
            shutil.rmtree(temp_root, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
