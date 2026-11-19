"""JSON-RPC 2.0 client stub over the project's HTTP/REST transport."""
import time
import uuid

import requests


class RpcError(RuntimeError):
    def __init__(self, code, message, data=None):
        super().__init__(f"RPC {code}: {message}")
        self.code, self.message, self.data = code, message, data


class RpcClient:
    """Client stub that serializes method calls and validates server responses."""

    def __init__(self, endpoint="http://127.0.0.1:5100/rpc/call", timeout=5.0, session=None):
        self.endpoint = endpoint.rstrip("/")
        self.timeout = timeout
        self.session = session or requests.Session()

    def call(self, method, params=None, request_id=None):
        request_id = request_id or str(uuid.uuid4())
        envelope = {"jsonrpc": "2.0", "id": request_id,
                    "method": method, "params": params or {}}
        started = time.perf_counter()
        try:
            response = self.session.post(self.endpoint, json=envelope, timeout=self.timeout)
            response.raise_for_status()
            message = response.json()
        except requests.RequestException as exc:
            raise RpcError(-32098, f"transport failure: {exc}") from exc
        except ValueError as exc:
            raise RpcError(-32700, "server returned invalid JSON") from exc
        if message.get("jsonrpc") != "2.0" or message.get("id") != request_id:
            raise RpcError(-32600, "invalid or mismatched response envelope", message)
        if "error" in message:
            error = message["error"]
            raise RpcError(error.get("code", -32000), error.get("message", "remote error"),
                           error.get("data"))
        return {"result": message.get("result"),
                "latency_ms": (time.perf_counter() - started) * 1000,
                "message_count": message.get("meta", {}).get("message_count"),
                "message_bytes": message.get("meta", {}).get("message_bytes"),
                "request_id": request_id}
