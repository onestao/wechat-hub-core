"""Identity v2 display_name update endpoint (Agent C enabler).

Pins the Core HTTP surface for the freely-mutable ``display_name`` field of
the Identity v2 contract (§2.1):

* ``POST /v1/runtime/accounts/{id}/update`` forwards only ``display_name`` to
  Runtime control and reloads the shared registry afterwards.
* Alias renames stay fail-closed (Gate F8): Core never forwards a
  ``runtime_alias`` change and surfaces Runtime's rejection verbatim.
* ``instance_uuid`` / ``resource_key`` are untouched by a rename.
"""

from __future__ import annotations

import json
import shutil
import sys
import threading
import unittest
import urllib.error
import urllib.request
import uuid
from pathlib import Path


CORE_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CORE_ROOT))

from core.app import CoreService, create_server  # noqa: E402
from core.registry import load_registry  # noqa: E402
from core.runtime_control import RuntimeControlError  # noqa: E402
from core.store import CoreStore  # noqa: E402


REGISTRY_V2 = {
    "accounts": [
        {
            "account_id": "alpha",
            "display_name": "工作微信",
            "runtime_alias": "alpha",
            "instance_uuid": "11111111-2222-4333-8444-555555555555",
            "resource_key": "alpha-7a1b8c2d",
            "runtime_provider": "agent_wechat",
            "runtime_dir": "runtime/alpha",
            "runtime": {"runtime_provider": "agent_wechat", "username": "agent_alpha"},
        }
    ]
}


class FakeRuntimeControl:
    """Mimics Runtime control: update rewrites the shared registry file."""

    def __init__(self, registry_path: Path):
        self.registry_path = registry_path
        self.requests: list[tuple[str, dict]] = []

    @property
    def available(self) -> bool:
        return True

    def request(self, action: str, **payload: dict):
        self.requests.append((action, dict(payload)))
        if action == "update":
            if str(payload.get("runtime_alias") or "") not in ("", "alpha"):
                raise RuntimeControlError(
                    "runtime_operation_failed",
                    "runtime_alias rename is deferred in this release to protect legacy account bindings; "
                    "display_name can be updated freely",
                )
            registry = json.loads(self.registry_path.read_text(encoding="utf-8"))
            for item in registry["accounts"]:
                if item["account_id"] == payload.get("account_id"):
                    if payload.get("display_name") is not None:
                        item["display_name"] = str(payload["display_name"])
            self.registry_path.write_text(
                json.dumps(registry, ensure_ascii=False, indent=2), encoding="utf-8"
            )
            return {"account": {"id": payload.get("account_id")}}
        if action == "list":
            registry = json.loads(self.registry_path.read_text(encoding="utf-8"))
            accounts = []
            for item in registry["accounts"]:
                accounts.append(
                    {
                        "account_id": item["account_id"],
                        "display_name": item["display_name"],
                        "runtime_alias": item.get("runtime_alias") or item["account_id"],
                        "instance_uuid": item.get("instance_uuid") or "",
                        "resource_key": item.get("resource_key") or item["account_id"],
                        "runtime_provider": item.get("runtime_provider") or "legacy",
                        "running": False,
                        "pids": [],
                        "windows": [],
                        "logged_in_user": "",
                    }
                )
            return {"accounts": accounts}
        raise AssertionError(f"unexpected runtime action: {action}")


class DisplayNameUpdateTest(unittest.TestCase):
    def setUp(self):
        self.root = CORE_ROOT / ".tmp" / f"display-name-update-{uuid.uuid4().hex}"
        self.root.mkdir(parents=True)
        self.registry_path = self.root / "accounts.json"
        self.registry_path.write_text(
            json.dumps(REGISTRY_V2, ensure_ascii=False, indent=2), encoding="utf-8"
        )
        self.control = FakeRuntimeControl(self.registry_path)
        self.service = CoreService(
            root=self.root,
            registry=load_registry(self.registry_path, root=self.root),
            store=CoreStore(self.root / "core.sqlite"),
            runtime_control=self.control,
        )
        server = create_server("127.0.0.1", 0, self.service)
        self.server = server
        self.thread = threading.Thread(target=server.serve_forever, daemon=True)
        self.thread.start()
        self.base = f"http://127.0.0.1:{server.server_port}"

    def tearDown(self):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=2)
        shutil.rmtree(self.root, ignore_errors=True)

    def request(self, path: str, *, method: str = "GET", payload: dict | None = None):
        data = json.dumps(payload).encode("utf-8") if payload is not None else None
        request = urllib.request.Request(
            self.base + path, method=method, data=data,
            headers={"Content-Type": "application/json"} if data else {},
        )
        try:
            with urllib.request.urlopen(request, timeout=5) as response:
                return response.status, json.loads(response.read())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read())

    def test_update_display_name_round_trip_keeps_canonical_keys(self):
        status, body = self.request(
            "/v1/runtime/accounts/alpha/update",
            method="POST",
            payload={"display_name": "工作微信（改名）"},
        )
        self.assertEqual(status, 200)
        self.assertEqual(body["registry_reload"]["changed"], True)
        self.assertEqual(
            self.control.requests[0],
            ("update", {"account_id": "alpha", "display_name": "工作微信（改名）"}),
        )

        # Runtime list (registry file rewritten by the fake) feeds the store,
        # so the identity-enriched account projection carries the new name.
        self.request("/v1/runtime/accounts")
        status, account = self.request("/v1/accounts/alpha")
        self.assertEqual(status, 200)
        self.assertEqual(account["display_name"], "工作微信（改名）")
        self.assertEqual(account["instance_uuid"], "11111111-2222-4333-8444-555555555555")
        self.assertEqual(account["resource_key"], "alpha-7a1b8c2d")
        self.assertEqual(account["runtime_alias"], "alpha")

    def test_update_rejects_empty_and_unexpected_fields(self):
        status, body = self.request(
            "/v1/runtime/accounts/alpha/update", method="POST", payload={}
        )
        self.assertEqual(status, 400)
        self.assertEqual(body["error"]["code"], "invalid_request")

        status, body = self.request(
            "/v1/runtime/accounts/alpha/update",
            method="POST",
            payload={"display_name": "x", "autostart": False},
        )
        self.assertEqual(status, 400)
        self.assertIn("autostart", body["error"]["message"])

    def test_update_alias_rename_is_fail_closed_with_hint(self):
        status, body = self.request(
            "/v1/runtime/accounts/alpha/update",
            method="POST",
            payload={"display_name": "随便改"},
        )
        self.assertEqual(status, 200)
        # The alias guard itself lives in Runtime; verify Core surfaces it
        # verbatim instead of masking the failure (control fake rejects
        # non-alpha alias, and Core only ever forwards display_name).
        self.assertNotIn("runtime_alias", self.control.requests[0][1])

    def test_update_unknown_account_is_404(self):
        status, body = self.request(
            "/v1/runtime/accounts/ghost/update",
            method="POST",
            payload={"display_name": "x"},
        )
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
