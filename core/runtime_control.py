"""Client for Runtime's private Unix-socket account lifecycle control plane."""

from __future__ import annotations

import json
import socket
from pathlib import Path
from typing import Any


class RuntimeControlError(RuntimeError):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


class RuntimeControlClient:
    def __init__(self, socket_path: str | Path, *, timeout: float = 20.0) -> None:
        self.socket_path = Path(socket_path)
        self.timeout = max(0.5, float(timeout))

    @property
    def configured(self) -> bool:
        return bool(str(self.socket_path))

    @property
    def available(self) -> bool:
        return self.socket_path.exists()

    def request(self, action: str, **payload: Any) -> dict[str, Any]:
        request = {"action": action, **payload}
        data = (json.dumps(request, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
        try:
            with socket.socket(socket.AF_UNIX, socket.SOCK_STREAM) as client:
                client.settimeout(self.timeout)
                client.connect(str(self.socket_path))
                client.sendall(data)
                chunks: list[bytes] = []
                total = 0
                while True:
                    chunk = client.recv(65536)
                    if not chunk:
                        break
                    chunks.append(chunk)
                    total += len(chunk)
                    if total > 3 * 1024 * 1024:
                        raise RuntimeControlError("invalid_runtime_response", "Runtime control response is too large")
                    if b"\n" in chunk:
                        break
        except RuntimeControlError:
            raise
        except (OSError, TimeoutError) as exc:
            raise RuntimeControlError("runtime_management_unavailable", str(exc)) from exc

        raw = b"".join(chunks).split(b"\n", 1)[0]
        try:
            response = json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise RuntimeControlError("invalid_runtime_response", "Runtime control returned invalid JSON") from exc
        if not isinstance(response, dict):
            raise RuntimeControlError("invalid_runtime_response", "Runtime control response must be an object")
        if not response.get("ok"):
            error = response.get("error") if isinstance(response.get("error"), dict) else {}
            raise RuntimeControlError(
                str(error.get("code") or "runtime_operation_failed"),
                str(error.get("message") or "Runtime operation failed"),
            )
        result = response.get("result")
        if not isinstance(result, dict):
            raise RuntimeControlError("invalid_runtime_response", "Runtime control result must be an object")
        return result
