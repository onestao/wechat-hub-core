"""Regression tests for upstream login status = unknown handling in Core.

The patched agent-wechat upstream returns status=unknown when the WeChat PID
is alive but the current UI cannot be identified.  These tests pin the Core
compatibility contract for that fresh-status semantics:

* Runtime status sync with wechat_login_status=unknown must never flip an
  account into a fresh logged_in claim: a previously online account keeps its
  existing state, an inactive/offline/degraded account only advances to the
  safe "starting" state.
* An unknown observation must not queue or dispatch any send and must not
  fabricate a "sent" receipt.
* A transient unknown must not clear account identity (display_name, username,
  registered flag) or delete persisted credentials (decrypt keys file,
  agent-wechat token file).
* The /v1/runtime/accounts/<id>/login projection must surface
  auth_status=unknown verbatim and must never report login success.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import threading
import unittest
import urllib.request
import uuid
from pathlib import Path


CORE_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CORE_ROOT))

from core.app import CoreService, create_server  # noqa: E402
from core.registry import AccountRegistry, parse_account  # noqa: E402
from core.sender import AccountSender  # noqa: E402
from core.store import CoreStore  # noqa: E402


def unknown_runtime_status(account_id: str, *, agent_server_healthy: bool = True) -> dict:
    return {
        "account_id": account_id,
        "display_name": account_id.title(),
        "runtime_provider": "agent_wechat",
        "running": True,
        "container_running": True,
        "agent_server_healthy": agent_server_healthy,
        "runtime_health": "healthy" if agent_server_healthy else "degraded",
        "wechat_login_status": "unknown",
        "logged_in_user": "",
        "pids": [],
        "windows": [],
    }


def logged_in_runtime_status(account_id: str) -> dict:
    status = unknown_runtime_status(account_id)
    status["wechat_login_status"] = "logged_in"
    status["logged_in_user"] = f"wxid_{account_id}"
    return status


class UnknownAuthStatusCoreTest(unittest.TestCase):
    def setUp(self):
        self.root = CORE_ROOT / ".tmp" / f"unknown-status-{uuid.uuid4().hex}"
        self.root.mkdir(parents=True)
        self.account = parse_account(
            {
                "account_id": "alpha",
                "display_name": "Alpha",
                "runtime_dir": str(self.root / "runtime" / "alpha"),
                "runtime": {
                    "runtime_provider": "agent_wechat",
                    "sender_driver": "agent_wechat",
                    "sender_enabled": True,
                    "username": "agent_wxid_alpha",
                },
            },
            root=self.root,
        )
        self.registry = AccountRegistry([self.account], self.root / "accounts.json")
        self.store = CoreStore(self.root / "core.sqlite")
        self.service = CoreService(root=self.root, registry=self.registry, store=self.store)

    def tearDown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    def outbox_count(self) -> int:
        with sqlite3.connect(self.root / "core.sqlite") as conn:
            return int(conn.execute("SELECT COUNT(*) FROM outbox").fetchone()[0])

    def test_previous_online_then_unknown_keeps_state_without_fresh_login_claim(self):
        self.service._apply_runtime_status(logged_in_runtime_status("alpha"))
        stored = self.store.account("alpha")
        self.assertEqual(stored["state"], "online")

        self.service._apply_runtime_status(unknown_runtime_status("alpha"))
        stored = self.store.account("alpha")
        # Existing design: a transient unknown keeps the previous online state
        # instead of inventing a fresh logged_in/login_required transition.
        self.assertEqual(stored["state"], "online")
        self.assertEqual(stored["runtime"]["wechat_login_status"], "unknown")
        self.assertEqual(stored["runtime"]["logged_in_user"], "")
        self.assertTrue(stored["runtime"]["agent_server_healthy"])
        self.assertNotIn(stored["state"], {"stopped", "degraded", "error", "login_required"})

    def test_previous_inactive_then_unknown_maps_to_starting_not_online(self):
        for previous_state in ("offline", "degraded", "stopped"):
            with self.subTest(previous_state=previous_state):
                self.store.upsert_account("alpha", "Alpha", state=previous_state)
                self.service._apply_runtime_status(unknown_runtime_status("alpha"))
                stored = self.store.account("alpha")
                self.assertEqual(stored["state"], "starting")
                self.assertEqual(stored["runtime"]["wechat_login_status"], "unknown")
                self.assertNotEqual(stored["state"], "online")

    def test_unknown_status_does_not_queue_or_send_messages(self):
        self.service._apply_runtime_status(logged_in_runtime_status("alpha"))
        self.service._apply_runtime_status(unknown_runtime_status("alpha"))
        self.assertEqual(self.outbox_count(), 0)
        sender = AccountSender(self.registry, self.store, root=self.root)
        result = sender.process_pending()
        self.assertEqual(
            {key: result[key] for key in ("processed", "submitted", "sent", "failed", "uncertain", "deferred")},
            {"processed": 0, "submitted": 0, "sent": 0, "failed": 0, "uncertain": 0, "deferred": 0},
        )
        with sqlite3.connect(self.root / "core.sqlite") as conn:
            statuses = [row[0] for row in conn.execute("SELECT status FROM outbox").fetchall()]
        self.assertNotIn("sent", statuses)
        self.assertNotIn("submitted", statuses)

    def test_unknown_status_preserves_identity_and_credential_files(self):
        keys_file = self.root / "runtime" / "alpha" / "wechat-decrypt" / "keys" / "all_keys.json"
        keys_file.parent.mkdir(parents=True, exist_ok=True)
        keys_file.write_text('{"session.db": "11" * 32}', encoding="utf-8")
        token_file = self.root / "config" / "agent-wechat" / "alpha" / "auth-token"
        token_file.parent.mkdir(parents=True, exist_ok=True)
        token_file.write_text("agent-token-secret", encoding="utf-8")

        # Bring the account online first, then observe a transient unknown.
        self.service._apply_runtime_status(logged_in_runtime_status("alpha"))
        self.service._apply_runtime_status(unknown_runtime_status("alpha"))

        stored = self.store.account("alpha")
        self.assertIsNotNone(stored)
        assert stored is not None
        self.assertEqual(stored["display_name"], "Alpha")
        self.assertTrue(stored["runtime"]["registered"])
        self.assertEqual(stored["runtime"]["username"], "agent_wxid_alpha")
        self.assertEqual(keys_file.read_text(encoding="utf-8"), '{"session.db": "11" * 32}')
        self.assertEqual(token_file.read_text(encoding="utf-8"), "agent-token-secret")

    def test_login_status_unknown_is_reported_verbatim_and_never_success(self):
        class FakeRuntimeControl:
            def __init__(self):
                self.requests = []

            @property
            def available(self):
                return True

            def request(self, action, **payload):
                self.requests.append((action, dict(payload)))
                if action != "login_status":
                    raise AssertionError(f"unexpected runtime action during unknown status: {action}")
                return {
                    "login": {
                        "account_id": "alpha",
                        "display_name": "Alpha",
                        "runtime_provider": "agent_wechat",
                        "running": True,
                        "container_running": True,
                        "agent_server_healthy": True,
                        "runtime_health": "healthy",
                        "snapshot_available": False,
                        "auth_status": "unknown",
                        "logged_in_user": "",
                        "window_title": "agent-wechat",
                        "windows": [],
                        "login_flow_state": "idle",
                        "login_flow_status": "",
                        "login_flow_error": "",
                    }
                }

        control = FakeRuntimeControl()
        self.store.upsert_account("alpha", "Alpha", state="online")
        service = CoreService(root=self.root, registry=self.registry, store=self.store, runtime_control=control)

        login = service.runtime_login_status("alpha")
        self.assertEqual(login["auth_status"], "unknown")
        self.assertEqual(login["logged_in_user"], "")
        # Existing design: core_state=online is retained while auth_status=unknown.
        self.assertEqual(login["state"], "online")
        self.assertEqual(login["core_state"], "online")
        self.assertEqual(login["runtime_health"], "healthy")
        self.assertEqual(control.requests, [("login_status", {"account_id": "alpha"})])

        self.store.upsert_account("alpha", "Alpha", state="offline")
        login = service.runtime_login_status("alpha")
        self.assertEqual(login["auth_status"], "unknown")
        self.assertEqual(login["state"], "starting")

    def test_runtime_accounts_list_with_unknown_applies_safe_state_over_http(self):
        class ListRuntimeControl:
            def __init__(self):
                self.requests = []

            @property
            def available(self):
                return True

            def request(self, action, **payload):
                self.requests.append((action, dict(payload)))
                if action != "list":
                    raise AssertionError(f"unexpected runtime action during unknown status: {action}")
                return {"accounts": [unknown_runtime_status("alpha")]}

        control = ListRuntimeControl()
        service = CoreService(root=self.root, registry=self.registry, store=self.store, runtime_control=control)
        server = create_server("127.0.0.1", 0, service)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            base_url = f"http://127.0.0.1:{server.server_port}"
            with urllib.request.urlopen(f"{base_url}/v1/runtime/accounts", timeout=3) as response:
                self.assertEqual(response.status, 200)
                payload = json.loads(response.read())
            self.assertEqual(len(payload["accounts"]), 1)
            self.assertEqual(payload["accounts"][0]["wechat_login_status"], "unknown")
            self.assertEqual(payload["accounts"][0]["logged_in_user"], "")
            self.assertEqual(control.requests, [("list", {})])
            stored = service.store.account("alpha")
            self.assertEqual(stored["state"], "starting")
            self.assertEqual(stored["runtime"]["wechat_login_status"], "unknown")
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == "__main__":
    unittest.main()
