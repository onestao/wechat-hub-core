"""P0-0 regression: AccountRegistry public API and the Console -> WeChat send chain.

Why this file exists
--------------------
Two defects shipped together in the Factory Fresh deployment and neither was
covered by the existing suite:

1. ``AgentWechatSenderDriver`` resolved its account through
   ``resolve_runtime_account``.  For an ``agent_wechat`` account that helper
   rebuilds the runtime from ``canonical_runtime_projection`` -- the *public*
   shape -- which deliberately strips Core-internal keys including
   ``agent_wechat_token_file``.  Every send therefore failed with
   ``agent-wechat token file is unavailable`` before any HTTP call.
   ``AgentWechatSenderRoutingTest`` could not catch it because its fixture has
   no ``runtime_bridge`` key, so ``resolve_runtime_account`` short-circuits and
   returns the account unchanged.  The fixtures here use the *runtime-shaped*
   registry that production actually writes.

2. ``/health`` reported the static legacy capability block at the top level, so
   a deployment whose only account was an online, logged-in, sender-enabled
   AgentWechat account still advertised ``text: false``.

Covers gates T1-T12 of the P0-0 taskbook.
"""

from __future__ import annotations

import json
import os
import re
import shutil
import sys
import tempfile
import threading
import time
import unittest
import uuid
from pathlib import Path
from unittest.mock import patch


CORE_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CORE_ROOT))

_TEST_GUI_LEASE_DIR = tempfile.TemporaryDirectory()
os.environ.setdefault("WECHAT_GUI_LEASE_DIR", _TEST_GUI_LEASE_DIR.name)

from core.registry import (  # noqa: E402
    AccountConfig,
    AccountRegistry,
    load_registry,
    parse_account,
)
from core.runtime_bridge import resolve_runtime_account  # noqa: E402
from core.sender import (  # noqa: E402
    AccountSender,
    OutboxLoop,
    account_send_readiness,
    classify_send_failure,
    effective_sender_capabilities,
)
from core.store import CoreStore, utc_now  # noqa: E402


RESOURCE_KEY = "arasial-11d4b2d9"
BOUND_WXID = "wxid_7ugft7xlkf5a22_4117"


def _agent_entry(
    account_id: str = "arasial",
    *,
    resource_key: str = RESOURCE_KEY,
    token_file: str = "",
    home: str = "",
    enabled: bool = True,
) -> dict:
    """One entry in the runtime-shaped (package A) registry Core reads in production."""

    return {
        "id": account_id,
        "display_name": account_id,
        "runtime_alias": account_id,
        "resource_key": resource_key,
        "instance_uuid": "1f0043f2-6945-4b08-876d-4580ccd47c2f",
        "runtime_provider": "agent_wechat",
        "enabled": enabled,
        "home": home,
        "agent_wechat": {
            "container_name": f"wechat-agent-{resource_key}",
            "token_file": token_file,
        },
    }


def _legacy_entry(account_id: str = "default") -> dict:
    return {
        "account_id": account_id,
        "display_name": account_id,
        "runtime_dir": f"runtime/accounts/{account_id}",
        "runtime": {"display": ":1", "sender_enabled": True},
    }


def _ok_response(body: bytes = b'{"success":true}'):
    class _Response:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def read(self, _limit=-1):
            return body

    return _Response()


class RuntimeShapedFixture(unittest.TestCase):
    """Writes a production-shaped registry: ``config/wechat-runtime/accounts.json``."""

    def setUp(self) -> None:
        self.temp_root = CORE_ROOT / ".tmp" / f"p0-send-{uuid.uuid4().hex}"
        self.temp_root.mkdir(parents=True)
        self.config_root = self.temp_root / "config"
        self.registry_path = self.config_root / "wechat-runtime" / "accounts.json"
        self.registry_path.parent.mkdir(parents=True, exist_ok=True)
        self.store = CoreStore(self.temp_root / "core.sqlite")

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def write_registry(self, entries: list[dict]) -> AccountRegistry:
        self.registry_path.write_text(
            json.dumps({"version": 2, "accounts": entries}), encoding="utf-8"
        )
        return load_registry(self.registry_path, root=self.temp_root)

    def write_token(self, resource_key: str = RESOURCE_KEY, value: str = "tok-123\n") -> Path:
        token = self.config_root / "agent-wechat" / resource_key / "auth-token"
        token.parent.mkdir(parents=True, exist_ok=True)
        token.write_text(value, encoding="utf-8")
        return token

    def ready_runtime(self, **overrides) -> dict:
        runtime = {
            "runtime_provider": "agent_wechat",
            "sender_driver": "agent_wechat",
            "sender_enabled": True,
            "enabled": True,
            "running": True,
            "container_running": True,
            "agent_server_healthy": True,
            "wechat_login_status": "logged_in",
            "logged_in_user": BOUND_WXID,
            "sender_capabilities": {
                "driver": "agent_wechat",
                "text": True,
                "image": True,
                "file": True,
            },
        }
        runtime.update(overrides)
        return runtime

    def agent_account(self, account_id: str = "arasial", **runtime_overrides) -> AccountConfig:
        return parse_account(
            {
                "account_id": account_id,
                "display_name": account_id,
                "runtime_dir": f"runtime/accounts/{account_id}",
                "runtime": self.ready_runtime(**runtime_overrides),
            },
            root=self.temp_root,
        )


class RegistryPublicApiTest(unittest.TestCase):
    """T1 / T2 / T3 - the public enumeration API is the only supported surface."""

    def setUp(self) -> None:
        self.temp_root = CORE_ROOT / ".tmp" / f"p0-registry-{uuid.uuid4().hex}"
        self.temp_root.mkdir(parents=True)
        self.accounts = [
            parse_account(
                {"account_id": name, "display_name": name, "runtime_dir": f"runtime/{name}"},
                root=self.temp_root,
            )
            for name in ("alpha", "beta")
        ]
        self.registry = AccountRegistry(self.accounts, self.temp_root / "accounts.json")

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def test_t1_public_enumeration_and_snapshot_api(self):
        # T1: a stable, thread-safe, read-only public enumeration/snapshot API.
        self.assertEqual({row.account_id for row in self.registry.all()}, {"alpha", "beta"})
        self.assertEqual(self.registry.get("alpha").account_id, "alpha")
        self.assertIsNone(self.registry.get("missing"))
        with self.assertRaises(Exception):
            self.registry.require("missing")

        # The snapshot must be a copy: a caller cannot mutate registry state.
        snapshot = self.registry.all()
        snapshot.clear()
        self.assertEqual(len(self.registry.all()), 2)

        # replace_from keeps callers' reference stable while swapping the snapshot.
        replacement = AccountRegistry(
            [
                parse_account(
                    {"account_id": "gamma", "display_name": "gamma", "runtime_dir": "runtime/gamma"},
                    root=self.temp_root,
                )
            ],
            self.temp_root / "accounts.json",
        )
        reference = self.registry
        self.registry.replace_from(replacement)
        self.assertIs(reference, self.registry)
        self.assertEqual([row.account_id for row in self.registry.all()], ["gamma"])

    def test_t2_no_caller_references_registry_accounts_attribute(self):
        # T2: the attribute does not exist, so any caller still using it is a
        # latent AttributeError.  Assert both halves of that statement.
        self.assertFalse(hasattr(AccountRegistry, "accounts"))
        self.assertFalse(hasattr(AccountRegistry([], Path("accounts.json")), "accounts"))
        self.assertTrue(hasattr(AccountRegistry([], Path("accounts.json")), "_accounts"))

        offenders: list[str] = []
        for path in sorted((CORE_ROOT / "core").rglob("*.py")):
            if "tests" in path.parts:
                continue
            text = path.read_text(encoding="utf-8")
            for match in re.finditer(r"\bregistry\.accounts\b|\bself\.registry\.accounts\b", text):
                offenders.append(f"{path.relative_to(CORE_ROOT)}:{text[:match.start()].count(chr(10)) + 1}")
        self.assertEqual(offenders, [], f"callers still use the removed registry.accounts API: {offenders}")

    def test_t3_no_caller_depends_on_the_private_accounts_field(self):
        # T3: no module outside registry.py may reach into ``_accounts``.
        offenders: list[str] = []
        for path in sorted((CORE_ROOT / "core").rglob("*.py")):
            if "tests" in path.parts or path.name == "registry.py":
                continue
            text = path.read_text(encoding="utf-8")
            for match in re.finditer(r"\._accounts\b", text):
                offenders.append(f"{path.relative_to(CORE_ROOT)}:{text[:match.start()].count(chr(10)) + 1}")
        self.assertEqual(offenders, [], f"callers depend on the private AccountRegistry._accounts: {offenders}")


class SendCapabilityAggregationTest(RuntimeShapedFixture):
    """T4 / T12 - the deployment capability must reflect the live account set."""

    def test_t4_one_ready_agent_wechat_account_reports_effective_text(self):
        account = self.agent_account()
        readiness = account_send_readiness(account)
        self.assertTrue(readiness["send_ready"], readiness)
        self.assertEqual(readiness["send_blocked_reason"], "")

        caps = effective_sender_capabilities([account])
        self.assertTrue(caps["text"])
        self.assertTrue(caps["image"])
        self.assertTrue(caps["file"])
        self.assertEqual(caps["effective_from_accounts"], ["arasial"])
        self.assertEqual(caps["accounts_send_ready"], 1)

    def test_readiness_follows_authoritative_login_state(self):
        cases = [
            ({"wechat_login_status": "logged_out", "logged_in_user": ""}, "wechat_not_logged_in"),
            ({"wechat_login_status": "unknown", "logged_in_user": ""}, "wechat_ready_unknown"),
            ({"running": False, "container_running": False}, "wechat_client_unavailable"),
            ({"agent_server_healthy": False}, "wechat_client_unavailable"),
            ({"sender_enabled": False}, "sender_disabled"),
        ]
        for overrides, expected in cases:
            with self.subTest(reason=expected):
                row = account_send_readiness(self.agent_account(**overrides))
                self.assertFalse(row["send_ready"])
                self.assertEqual(row["send_blocked_reason"], expected)
                self.assertTrue(row["send_blocked_message"])
                self.assertFalse(effective_sender_capabilities([self.agent_account(**overrides)])["text"])

    def test_t12_removing_a_legacy_default_account_keeps_the_valid_sender(self):
        ready = self.agent_account()
        legacy = parse_account(_legacy_entry(), root=self.temp_root)

        with_legacy = effective_sender_capabilities([legacy, ready])
        without_legacy = effective_sender_capabilities([ready])
        self.assertTrue(with_legacy["text"])
        self.assertTrue(without_legacy["text"])
        self.assertEqual(without_legacy["effective_from_accounts"], ["arasial"])

        # A legacy account alone is not send capable and must not be invented.
        only_legacy = effective_sender_capabilities([legacy])
        self.assertFalse(only_legacy["text"])
        self.assertEqual(only_legacy["accounts_send_ready"], 0)


class AgentWechatDispatchTest(RuntimeShapedFixture):
    """T5 - T11: the outbox worker and the real dispatch path."""

    def setUp(self) -> None:
        super().setUp()
        self.token = self.write_token()
        self.registry = self.write_registry(
            [
                _agent_entry(
                    token_file=f"/config/agent-wechat/{RESOURCE_KEY}/auth-token",
                    home=f"/config/agent-wechat/{RESOURCE_KEY}/home",
                )
            ]
        )
        self.account = self.registry.require("arasial")
        self.store.upsert_account("arasial", "Arasial", state="online")
        self.store.upsert_chat(
            {"account_id": "arasial", "chat_id": "chat-target", "type": "group", "display_name": "Target"}
        )
        self.sender = AccountSender(self.registry, self.store, root=self.temp_root)

    # -- helpers ---------------------------------------------------------

    def _queue(self, text: str = "hello") -> str:
        receipt = self.store.queue_send(
            "text", {"account_id": "arasial", "chat_id": "chat-target", "text": text}
        )
        return str(receipt["send_id"])

    # -- T5: registry hot reload ----------------------------------------

    def test_t5_sender_enumerates_a_hot_reloaded_registry(self):
        self.write_registry(
            [
                _agent_entry(
                    token_file=f"/config/agent-wechat/{RESOURCE_KEY}/auth-token",
                    home=f"/config/agent-wechat/{RESOURCE_KEY}/home",
                ),
                _agent_entry(
                    "second",
                    resource_key="second-abc12345",
                    token_file="/config/agent-wechat/second-abc12345/auth-token",
                    home="/config/agent-wechat/second-abc12345/home",
                ),
            ]
        )
        replacement = load_registry(self.registry_path, root=self.temp_root)
        self.registry.replace_from(replacement)
        # The sender holds the same registry object; enumeration must reflect the swap.
        self.assertEqual({row.account_id for row in self.registry.all()}, {"arasial", "second"})
        self.assertEqual(self.sender.process_pending()["processed"], 0)

    # -- T6 / T7 / T9: dispatch reaches the driver and converges ----------

    def test_t6_t7_t9_request_reaches_dispatch_and_converges_to_sent(self):
        send_id = self._queue("p0-echo")
        calls: list[str] = []

        def fake_urlopen(request, timeout=0):
            del timeout
            calls.append(request.full_url)
            return _ok_response()

        with patch("core.sender.urllib.request.urlopen", side_effect=fake_urlopen):
            result = self.sender.process_pending()

        self.assertEqual(result["submitted"], 1, result)
        self.assertEqual(result["deferred"], 0, result)
        # T6: the request reached the AgentWechat dispatch plane with the exact target.
        self.assertIn("http://wechat-agent-arasial-11d4b2d9:6174/api/chats/chat-target/open?clearUnreads=false", calls)
        self.assertIn("http://wechat-agent-arasial-11d4b2d9:6174/api/messages/send", calls)

        # T7: queued left queued.
        status = self.store.send_status(send_id)
        self.assertEqual(status["status"], "submitted")
        self.assertNotIn(status["status"], {"accepted", "queued"})

        # T9: a unique outgoing echo converges the send to sent.
        self.store.upsert_message(
            {
                "account_id": "arasial",
                "message_id": "echo-1",
                "chat_id": "chat-target",
                "type": "text",
                "direction": "outgoing",
                "created_at": utc_now(),
                "author": {"member_id": "self", "display_name": "Arasial", "is_self": True},
                "text": "p0-echo",
            }
        )
        sent = self.store.send_status(send_id)
        self.assertEqual(sent["status"], "sent")
        self.assertEqual(sent["echo_message_id"], "echo-1")

    # -- T8: a dispatch failure converges to a readable, retryable failure --

    def test_t8_dispatch_error_converges_to_failed_with_user_message(self):
        send_id = self._queue("will-fail")

        def failing_urlopen(request, timeout=0):
            del request, timeout
            raise OSError("Connection refused")

        with patch("core.sender.urllib.request.urlopen", side_effect=failing_urlopen):
            result = self.sender.process_pending()

        self.assertEqual(result["failed"], 1, result)
        status = self.store.send_status(send_id)
        self.assertEqual(status["status"], "failed")
        self.assertEqual(status["error_code"], "wechat_unavailable")
        self.assertTrue(status["user_message"])
        # The product-facing reason never leaks the internal exception text.
        self.assertNotIn("OSError", status["user_message"])
        self.assertNotIn("Connection refused", status["user_message"])
        # The operator-facing reason stays on the receipt while the user-facing
        # message is the product vocabulary.
        self.assertIn("chat pre-open failed before submission", status["details"]["failure"]["reason"])

    def test_failure_classification_is_stable(self):
        self.assertEqual(classify_send_failure(RuntimeError("agent-wechat token file is unavailable for x")), "sender_unavailable")
        self.assertEqual(classify_send_failure(TimeoutError("timed out")), "send_timeout")
        self.assertEqual(classify_send_failure(RuntimeError("target chat is no longer present in normalized Core data")), "target_unavailable")
        self.assertEqual(classify_send_failure(RuntimeError("agent-wechat send returned HTTP 500: boom")), "wechat_unavailable")
        self.assertEqual(classify_send_failure(RuntimeError("something else entirely")), "sender_failed")

    # -- T10: restart / lease recovery must not duplicate an effect -------

    def test_t10_stale_sending_row_is_failed_not_resent(self):
        send_id = self._queue("no-duplicate")
        self.store.transition_send(send_id, "queued")
        self.store.transition_send(send_id, "sending")
        with self.store.connection() as conn:
            conn.execute(
                "UPDATE outbox SET updated_at='2020-01-01T00:00:00Z' WHERE send_id=?", (send_id,)
            )

        recovered = self.store.recover_stale_sends(max_age_seconds=1)
        self.assertEqual(recovered, 1)
        status = self.store.send_status(send_id)
        self.assertEqual(status["status"], "failed")
        self.assertEqual(status["details"]["recovery"]["reason"], "sending_lease_expired")

        calls: list[str] = []

        def fake_urlopen(request, timeout=0):
            del timeout
            calls.append(request.full_url)
            return _ok_response()

        with patch("core.sender.urllib.request.urlopen", side_effect=fake_urlopen):
            self.sender.process_pending()
        self.assertEqual(calls, [], "a recovered (possibly delivered) send must never be re-dispatched")

    def test_submitted_row_is_never_redispatched(self):
        send_id = self._queue("already-submitted")
        self.store.transition_send(send_id, "submitted")
        calls: list[str] = []

        def fake_urlopen(request, timeout=0):
            del timeout
            calls.append(request.full_url)
            return _ok_response()

        with patch("core.sender.urllib.request.urlopen", side_effect=fake_urlopen):
            self.sender.process_pending()
        self.assertEqual(calls, [])
        self.assertEqual(self.store.send_status(send_id)["status"], "submitted")

    # -- T11: registry hot reload while the loop is running ---------------

    def test_t11_registry_hot_reload_while_outbox_loop_runs(self):
        loop = OutboxLoop(self.sender, 0.05)
        loop.start()
        try:
            deadline = time.monotonic() + 2.0
            while loop.liveness()["cycle_count"] < 3 and time.monotonic() < deadline:
                time.sleep(0.02)
            for _ in range(5):
                self.write_registry(
                    [
                        _agent_entry(
                            token_file=f"/config/agent-wechat/{RESOURCE_KEY}/auth-token",
                            home=f"/config/agent-wechat/{RESOURCE_KEY}/home",
                        )
                    ]
                )
                self.registry.replace_from(load_registry(self.registry_path, root=self.temp_root))
                time.sleep(0.02)
        finally:
            loop.stop()

        liveness = loop.liveness()
        self.assertTrue(liveness["alive"] or liveness["cycle_count"] > 0)
        self.assertEqual(liveness["last_error"], "")
        self.assertEqual(liveness["consecutive_failures"], 0)
        self.assertGreater(liveness["cycle_count"], 0)


class AgentWechatTokenSurvivesResolutionTest(RuntimeShapedFixture):
    """The exact P0-0 root cause: the resolved account must keep its token path."""

    def test_resolved_account_keeps_core_internal_runtime_fields(self):
        token = self.write_token()
        registry = self.write_registry(
            [
                _agent_entry(
                    token_file=f"/config/agent-wechat/{RESOURCE_KEY}/auth-token",
                    home=f"/config/agent-wechat/{RESOURCE_KEY}/home",
                )
            ]
        )
        account = registry.require("arasial")
        self.assertEqual(account.runtime.get("runtime_bridge"), "agent-wechat-v1")
        self.assertTrue(account.runtime.get("agent_wechat_token_file"))

        resolved = resolve_runtime_account(account)
        self.assertEqual(resolved.runtime.get("agent_wechat_token_file"), str(token))
        self.assertTrue(Path(resolved.runtime["agent_wechat_token_file"]).is_file())

        # The public/persisted projection must still strip Core-internal keys.
        public = account.public_runtime()
        self.assertNotIn("agent_wechat_token_file", public)
        self.assertNotIn("controller_command", public)

    def test_send_succeeds_for_a_runtime_shaped_account(self):
        token = self.write_token()
        registry = self.write_registry(
            [
                _agent_entry(
                    token_file=f"/config/agent-wechat/{RESOURCE_KEY}/auth-token",
                    home=f"/config/agent-wechat/{RESOURCE_KEY}/home",
                )
            ]
        )
        store = self.store
        store.upsert_account("arasial", "Arasial", state="online")
        store.upsert_chat(
            {"account_id": "arasial", "chat_id": "chat-target", "type": "group", "display_name": "Target"}
        )
        sender = AccountSender(registry, store, root=self.temp_root)
        receipt = store.queue_send(
            "text", {"account_id": "arasial", "chat_id": "chat-target", "text": "hello"}
        )
        auth: list[str] = []

        def fake_urlopen(request, timeout=0):
            del timeout
            auth.append(str(request.get_header("Authorization") or ""))
            return _ok_response()

        with patch("core.sender.urllib.request.urlopen", side_effect=fake_urlopen):
            result = sender.process_pending()

        self.assertEqual(result["failed"], 0, store.send_status(receipt["send_id"]))
        self.assertEqual(result["submitted"], 1)
        self.assertTrue(auth)
        self.assertEqual(auth[0], f"Bearer {token.read_text(encoding='utf-8').strip()}")


class OutboxLoopLivenessTest(RuntimeShapedFixture):
    """The outbox loop must survive an exception instead of dying silently."""

    def test_loop_survives_process_pending_exception(self):
        self.registry = self.write_registry([])
        sender = AccountSender(self.registry, self.store, root=self.temp_root)
        loop = OutboxLoop(sender, 0.02)
        state = {"calls": 0}

        def boom():
            state["calls"] += 1
            if state["calls"] <= 2:
                raise RuntimeError("store is locked")
            return {"processed": 0, "submitted": 0, "sent": 0, "failed": 0, "uncertain": 0, "deferred": 0}

        with patch.object(sender, "process_pending", side_effect=boom):
            loop.start()
            try:
                deadline = time.monotonic() + 3.0
                while state["calls"] < 4 and time.monotonic() < deadline:
                    time.sleep(0.02)
            finally:
                loop.stop()

        self.assertGreaterEqual(state["calls"], 4, "the loop died after an exception")
        liveness = loop.liveness()
        self.assertGreaterEqual(liveness["cycle_count"], 4)
        self.assertEqual(liveness["last_error"], "")
        self.assertEqual(liveness["consecutive_failures"], 0)


if __name__ == "__main__":
    unittest.main()
