import asyncio
import json
from fastapi import APIRouter, WebSocket, WebSocketDisconnect
from fastapi.websockets import WebSocketState
from pydantic import ValidationError

from .connection_manager import ConnectionManager
from .controller import authenticate_ws
from .models import SubscribeRequest, UnsubscribeRequest
from .exc import WsAuthenticationError


router = APIRouter(prefix="/markets", tags=["Markets"])
conn_manager = ConnectionManager()
asyncio.get_running_loop().create_task(conn_manager.initialise())


@router.websocket("/crypto/bars")
async def crypto_market_ws(ws: WebSocket):
    await ws.accept()

    code = 1000
    reason = None

    try:
        await authenticate_ws(ws)

        while True:
            msg = await ws.receive_text()
            data = json.loads(msg)
            action = data.get("action")

            if action == "subscribe":
                request = SubscribeRequest(**data)
                conn_manager.subscribe(ws, request)
            else:
                request = UnsubscribeRequest(**data)
                conn_manager.unsubscribe(ws, request)

    except (RuntimeError, WebSocketDisconnect):
        pass
    except (ValidationError, json.JSONDecodeError):
        code = 1007
        reason = "Invalid payload"
    except WsAuthenticationError as e:
        code = 1008
        reason = str(e)
    finally:
        if ws.client_state == WebSocketState.CONNECTED:
            await ws.close(code=code, reason=reason)


@router.websocket("/stocks/bars")
async def stocks_market_ws(ws: WebSocket): ...
