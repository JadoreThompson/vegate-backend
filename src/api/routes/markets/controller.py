import asyncio
import json
from fastapi import WebSocket

from config import BARS_WS_TOKEN
from .exc import WsAuthenticationError


async def authenticate_ws(ws: WebSocket):
    try:
        msg = await asyncio.wait_for(ws.receive_text(), timeout=5.0)
        payload = json.loads(msg)
        if payload.get("token") == BARS_WS_TOKEN:
            await ws.send_text(json.dumps({"type": "message", "message": "connected"}))
            return
        raise WsAuthenticationError("Invalid payload")
    except asyncio.TimeoutError:
        raise WsAuthenticationError("Message timeout")
    except json.JSONDecodeError:
        raise WsAuthenticationError("Malformed payload received")
