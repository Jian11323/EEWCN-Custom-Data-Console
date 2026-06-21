"""Management WebSocket authentication helpers."""

from __future__ import annotations

import json
import os
from typing import Any, Optional, Set


def get_mgmt_token() -> str:
    return os.environ.get("FUSED_MGMT_TOKEN", "").strip()


def auth_required() -> bool:
    return bool(get_mgmt_token())


class ManagementAuth:
    """Track authenticated management WebSocket connections."""

    def __init__(self) -> None:
        self._token = get_mgmt_token()
        self._authenticated: Set[Any] = set()

    @property
    def required(self) -> bool:
        return bool(self._token)

    def mark_authenticated(self, websocket: Any) -> None:
        self._authenticated.add(websocket)

    def is_authenticated(self, websocket: Any) -> bool:
        if not self.required:
            return True
        return websocket in self._authenticated

    def forget(self, websocket: Any) -> None:
        self._authenticated.discard(websocket)

    def try_authenticate(self, websocket: Any, data: dict) -> Optional[dict]:
        """Handle auth message; return response dict or None if not an auth message."""
        if data.get("type") == "auth" or data.get("command") == "auth":
            token = (data.get("token") or "").strip()
            if token and token == self._token:
                self.mark_authenticated(websocket)
                return {"type": "auth_result", "success": True, "message": "认证成功"}
            return {"type": "auth_result", "success": False, "message": "认证失败：token 无效"}
        return None

    def token_from_params(self, params: dict) -> Optional[str]:
        token = params.get("token")
        if token is not None:
            return str(token).strip()
        return None

    def check_token_in_params(self, websocket: Any, params: dict) -> bool:
        token = self.token_from_params(params)
        if token and token == self._token:
            self.mark_authenticated(websocket)
            return True
        return False

    async def reject_unauthenticated(self, websocket: Any, is_json: bool) -> None:
        payload = {
            "type": "error",
            "message": "需要认证：请先发送 {\"type\":\"auth\",\"token\":\"...\"}",
        }
        if is_json:
            await websocket.send(json.dumps(payload, ensure_ascii=False))
        else:
            await websocket.send("ERROR:需要认证")
