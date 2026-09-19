"""Core consumer-control management API (P1-1 / P1-4).

Core is the authenticated management authority in front of the Runtime control
plane: the Console never touches the Docker socket, and the Runtime remains the
single lifecycle owner.  These tests pin the API surface plus the two things
that are easy to get wrong:

* a just-started consumer must be given its governed Core checkpoint, otherwise
  Core refuses every poll with ``missing_bootstrap_provenance`` and the consumer
  silently ingests nothing while looking healthy;
* a mode switch must never reset an existing consumer checkpoint.
"""

from __future__ import annotations

import json
import os
import shutil
import sys
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any


CORE_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CORE_ROOT))

_TEST_GUI_LEASE_DIR = tempfile.TemporaryDirectory()
os.environ.setdefault("WECHAT_GUI_LEASE_DIR", _TEST_GUI_LEASE_DIR.name)

from core.app import CoreService, create_server  # noqa: E402
from core.registry import AccountRegistry  # noqa: E402
from core.store import CoreStore  # noqa: E402


def _consumer_entry(consumer: str, *, running: bool, configured: bool = True) -> dict[str, Any]:
    return {
        "consumer": consumer,
        "display_name": consumer.upper(),
        "summary": "",
        "container_name": f"wechat-hub-{consumer}",
        "container_id": f"id-{consumer}" if configured else "",
        "image": f"ghcr.io/onestao/wechat-hub-{consumer}@sha256:" + "a" * 64,
        "current_image": "",
        "image_present": configured,
        "provisioned": configured,
        "configured": configured,
        "configuration_detail": "" if configured else "尚未配置",
        "state": "running" if running else ("stopped" if configured else "not_provisioned"),
        "running": running,
        "started_at": "",
        "finished_at": "",
        "restart_count": 0,
        "exit_code": 0 if configured else None,
        "can_start": configured,
        "blocked_reason": "" if configured else "尚未配置",
        "last_error": "",
    }


class FakeRuntimeControl:
    """Stands in for the Runtime control socket."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, dict[str, Any]]] = []
        self.running: str = ""
        self.efb_configured = False

    def _snapshot(self) -> dict[str, Any]:
        statuses = {
            "efb": _consumer_entry("efb", running=self.running == "efb", configured=self.efb_configured),
            "agent": _consumer_entry("agent", running=self.running == "agent"),
        }
        return {
            "mode": self.running or "disabled",
            "desired_mode": self.running or "disabled",
            "modes": ["disabled", "efb", "agent"],
            "mutual_exclusion": True,
            "consumers": statuses,
            "runtime": {"available": True},
        }

    def request(self, action: str, **payload: Any) -> dict[str, Any]:
        self.calls.append((action, dict(payload)))
        if action == "consumers":
            return {"consumers": self._snapshot()}
        if action == "consumers_mode":
            mode = str(payload.get("mode") or "disabled")
            self.running = "" if mode == "disabled" else mode
            return {"consumers": self._snapshot()}
        if action == "consumer_action":
            consumer = str(payload.get("consumer") or "")
            if str(payload.get("operation")) == "start":
                self.running = consumer
            elif self.running == consumer:
                self.running = ""
            return {"consumers": self._snapshot()}
        raise AssertionError(f"unexpected action: {action}")


class ConsumerControlApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_root = CORE_ROOT / ".tmp" / f"consumer-api-{uuid.uuid4().hex}"
        self.temp_root.mkdir(parents=True)
        self.store = CoreStore(self.temp_root / "core.sqlite")
        self.registry = AccountRegistry([], self.temp_root / "accounts.json")
        self.runtime = FakeRuntimeControl()
        self.service = CoreService(
            root=self.temp_root,
            registry=self.registry,
            store=self.store,
            runtime_control=self.runtime,
        )
        self.server = create_server("127.0.0.1", 0, self.service)
        self.base_url = f"http://127.0.0.1:{self.server.server_port}"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        shutil.rmtree(self.temp_root, ignore_errors=True)

    def request(self, path: str, method: str = "GET", payload: dict | None = None) -> tuple[int, Any]:
        body = json.dumps(payload).encode("utf-8") if payload is not None else None
        headers = {"Content-Type": "application/json"} if body else {}
        request = urllib.request.Request(self.base_url + path, data=body, method=method, headers=headers)
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                raw = response.read().decode("utf-8")
                return response.status, (json.loads(raw) if raw else {})
        except urllib.error.HTTPError as exc:
            raw = exc.read().decode("utf-8")
            return exc.code, (json.loads(raw) if raw else {})

    # -- routes -----------------------------------------------------------

    def test_get_consumers_returns_the_runtime_snapshot(self) -> None:
        status, payload = self.request("/v1/consumers")
        self.assertEqual(status, 200)
        self.assertEqual(self.runtime.calls, [("consumers", {})])
        self.assertEqual(payload["mode"], "disabled")
        self.assertTrue(payload["mutual_exclusion"])
        self.assertEqual(set(payload["consumers"]), {"efb", "agent"})

    def test_set_mode_validates_and_passes_through(self) -> None:
        status, payload = self.request("/v1/consumers/mode", method="POST", payload={"mode": "agent"})
        self.assertEqual(status, 200, payload)
        self.assertEqual(payload["mode"], "agent")
        self.assertTrue(payload["consumers"]["agent"]["running"])
        self.assertEqual(self.runtime.calls[-1], ("consumers_mode", {"mode": "agent"}))

        status, payload = self.request("/v1/consumers/mode", method="POST", payload={"mode": "nope"})
        self.assertEqual(status, 400)
        self.assertEqual(payload["error"]["code"], "invalid_request")

    def test_consumer_action_start_and_stop(self) -> None:
        status, payload = self.request("/v1/consumers/agent/start", method="POST", payload={})
        self.assertEqual(status, 200, payload)
        self.assertEqual(self.runtime.calls[-1], ("consumer_action", {"consumer": "agent", "operation": "start"}))
        self.assertTrue(payload["consumers"]["agent"]["running"])

        status, payload = self.request("/v1/consumers/agent/stop", method="POST", payload={})
        self.assertEqual(status, 200)
        self.assertFalse(payload["consumers"]["agent"]["running"])

    def test_unknown_consumer_and_operation_are_404(self) -> None:
        status, payload = self.request("/v1/consumers/bogus/start", method="POST", payload={})
        self.assertEqual(status, 404)
        self.assertEqual(payload["error"]["code"], "not_found")

        status, payload = self.request("/v1/consumers/agent/restart", method="POST", payload={})
        self.assertEqual(status, 404)

    def test_bootstrap_route_is_not_shadowed(self) -> None:
        status, payload = self.request(
            "/v1/consumers/bootstrap", method="POST", payload={"consumer_id": "wechat-console", "mode": "at_head"}
        )
        self.assertEqual(status, 200, payload)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["consumer_id"], "wechat-console")

    # -- checkpoint provisioning ------------------------------------------

    def test_starting_the_agent_provisions_its_core_checkpoint(self) -> None:
        self.assertIsNone(self.store.get_checkpoint("wechat-agent"))

        status, payload = self.request("/v1/consumers/agent/start", method="POST", payload={})
        self.assertEqual(status, 200, payload)
        self.assertEqual(
            payload["core_provisioning"],
            {"consumer_id": "wechat-agent", "provisioned": True, "initial_cursor": 0, "idempotent": False},
        )
        checkpoint = self.store.get_checkpoint("wechat-agent")
        self.assertIsNotNone(checkpoint)
        provenance = self.store.get_bootstrap_provenance("wechat-agent")
        assert provenance is not None
        self.assertEqual(provenance["bootstrap_mode"], "at_head")

    def test_mode_switch_provisions_and_never_resets_an_existing_checkpoint(self) -> None:
        self.request("/v1/consumers/agent/start", method="POST", payload={})
        # Advance the agent's own ingestion position (an account status event
        # moves the stream head so a non-zero checkpoint is legal).
        self.store.upsert_account("probe", "Probe", state="online")
        self.store.checkpoint_consumer("wechat-agent", 1)

        self.request("/v1/consumers/mode", method="POST", payload={"mode": "efb"})
        self.request("/v1/consumers/mode", method="POST", payload={"mode": "agent"})

        checkpoint = self.store.get_checkpoint("wechat-agent")
        assert checkpoint is not None
        self.assertEqual(
            checkpoint["processed_through_cursor"],
            1,
            "a mode switch must never rewind a consumer's ingestion position",
        )

    def test_efb_provisioning_uses_the_registered_efb_consumer_identity(self) -> None:
        self.runtime.efb_configured = True
        status, payload = self.request("/v1/consumers/efb/start", method="POST", payload={})
        self.assertEqual(status, 200, payload)
        self.assertEqual(payload["core_provisioning"]["consumer_id"], "efb-linux-wechat:wechat.linux")
        self.assertIsNotNone(self.store.get_checkpoint("efb-linux-wechat:wechat.linux"))

    def test_stopping_a_consumer_does_not_provision(self) -> None:
        status, payload = self.request("/v1/consumers/agent/stop", method="POST", payload={})
        self.assertEqual(status, 200)
        self.assertNotIn("core_provisioning", payload)
        self.assertIsNone(self.store.get_checkpoint("wechat-agent"))

    # -- install status ----------------------------------------------------

    def test_install_status_reports_business_states_only(self) -> None:
        status, payload = self.request("/v1/install/status")
        self.assertEqual(status, 200, payload)
        self.assertEqual(payload["core"]["state"], "ready")
        self.assertEqual(payload["runtime"]["state"], "ready")
        # No WeChat account yet on a factory-fresh deployment.
        self.assertEqual(payload["wechat"]["state"], "not_configured")
        # EFB has no Telegram configuration, Agent is provisionable but stopped.
        self.assertEqual(payload["efb"]["state"], "not_configured")
        self.assertFalse(payload["efb"]["can_start"])
        self.assertEqual(payload["agent"]["state"], "stopped")
        self.assertTrue(payload["agent"]["can_start"])
        self.assertEqual(payload["consumer_mode"], "disabled")
        self.assertTrue(payload["mutual_exclusion"])

    def test_install_status_reports_running_agent(self) -> None:
        self.request("/v1/consumers/agent/start", method="POST", payload={})
        status, payload = self.request("/v1/install/status")
        self.assertEqual(status, 200)
        self.assertEqual(payload["agent"]["state"], "running")
        self.assertEqual(payload["consumer_mode"], "agent")


if __name__ == "__main__":
    unittest.main()
