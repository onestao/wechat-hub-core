from __future__ import annotations

import base64
import json
import shutil
import sqlite3
import sys
import threading
import unittest
import urllib.error
import urllib.request
import uuid
from contextlib import contextmanager
from pathlib import Path


CORE_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CORE_ROOT))

from core.app import CoreService, create_server  # noqa: E402
from core.account_worker import media_args  # noqa: E402
from core.key_extract import scanner_command  # noqa: E402
from core.normalize import import_account  # noqa: E402
from core.registry import AccountRegistry, RegistryError, load_registry, parse_account  # noqa: E402
from core.runtime_bridge import account_processes, resolve_runtime_account  # noqa: E402
from core.sender import AccountSender  # noqa: E402
from core.store import CoreStore  # noqa: E402
from agent_console.wechat_controller import chat_window_ready  # noqa: E402
from memory.memory_ingest import ingest_session_chats, init_memory_db  # noqa: E402
from memory.sync_repair import repair_memory_indexes  # noqa: E402


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

    def test_accounts_chats_and_account_scoped_message_identity(self):
        status, health = self.request("/health")
        self.assertEqual(status, 200)
        self.assertEqual(health["contract_version"], 1)
        self.assertEqual(health["accounts"], 2)
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
        self.assertEqual(result, {"processed": 1, "sent": 1, "failed": 0, "deferred": 0})
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

    def test_multi_account_sender_refuses_global_window_discovery(self):
        unsafe = parse_account(
            {
                "account_id": "beta",
                "display_name": "Beta",
                "runtime_dir": "runtime/accounts/beta",
                "runtime": {
                    "display": ":7",
                    "sender_enabled": True,
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
