import uvicorn
from fastapi import FastAPI, HTTPException, Depends, Request
from fastapi.responses import JSONResponse

from module.broker.client import BrokerClientException
from .exception import InvalidSessionException
from .schema import (
    BalanceResponse,
    CreateSessionRequest,
    CreateSessionResponse,
    EquityResponse,
    ModifyOrderRequest,
    OrderResponse,
    PlaceOrderRequest,
    PositionResponse,
    SuccessResponse,
)
from .service import OMSService

_app = FastAPI()


def get_bearer_token(request: Request) -> str:
    """
    Extracts Bearer token from Authorization header.
    """
    val = request.headers.get("Authorization", "")
    if not val:
        raise HTTPException(status_code=401, detail="Authorization header missing")

    bearer = val.lower().replace("bearer ", "").strip()
    if not bearer:
        raise HTTPException(status_code=401, detail="Invalid Authorization format")

    return bearer


class OMSServer:

    def __init__(self, oms_service: OMSService, uvicorn_kw: dict):
        self._oms_service = oms_service
        self._uvicorn_kw = uvicorn_kw
        self._register_routes()
        self._register_exception_handlers()

    def run(self):
        uvicorn.run(_app, **self._uvicorn_kw)

    def _register_routes(self):

        @_app.post("/v1/session", response_model=CreateSessionResponse)
        async def create_session(body: CreateSessionRequest):
            token = await self._oms_service.create_session(body.deployment_id)
            return {"token": token}

        @_app.delete("/v1/session")
        async def close_session(token: str = Depends(get_bearer_token)):
            await self._oms_service.close_session(token)
            return {"status": "closed"}

        @_app.get("/v1/balance", response_model=BalanceResponse)
        async def get_balance(token: str = Depends(get_bearer_token)):
            return {"balance": await self._oms_service.get_balance(token)}

        @_app.get("/v1/equity", response_model=EquityResponse)
        async def get_equity(token: str = Depends(get_bearer_token)):
            return {"equity": await self._oms_service.get_equity(token)}

        @_app.get("/v1/position", response_model=PositionResponse)
        async def get_position(symbol: str, token: str = Depends(get_bearer_token)):
            return {"balance": await self._oms_service.get_position(token, symbol)}

        @_app.post("/v1/orders")
        async def place_order(
            body: PlaceOrderRequest, token: str = Depends(get_bearer_token)
        ):
            return await self._oms_service.place_order(token, body)

        @_app.patch("/v1/orders/{order_id}")
        async def modify_order(
            order_id: str,
            body: ModifyOrderRequest,
            token: str = Depends(get_bearer_token),
        ):
            return await self._oms_service.modify_order(
                token,
                order_id,
                limit_price=body.limit_price,
                stop_price=body.stop_price,
            )

        @_app.delete("/v1/orders/{order_id}", response_model=SuccessResponse)
        async def cancel_order(order_id: str, token: str = Depends(get_bearer_token)):
            return {"success": await self._oms_service.cancel_order(token, order_id)}

        @_app.delete("/v1/orders", response_model=SuccessResponse)
        async def cancel_all_orders(token: str = Depends(get_bearer_token)):
            return {"success": await self._oms_service.cancel_all_orders(token)}

        @_app.get("/v1/orders/{order_id}", response_model=OrderResponse)
        async def get_order(order_id: str, token: str = Depends(get_bearer_token)):
            return await self._oms_service.get_order(token, order_id)

        @_app.get("/v1/orders", response_model=list[OrderResponse])
        async def get_orders(token: str = Depends(get_bearer_token)):
            return await self._oms_service.get_orders(token)

        @_app.get("/v1/health")
        async def health_check():
            return {"status": "ok"}

    def _register_exception_handlers(self):

        def _response(status_code: int, message: str):
            return JSONResponse(status_code=status_code, content={"error": message})

        @_app.exception_handler(HTTPException)
        async def handle_http_exception(req: Request, exc: HTTPException):
            return _response(exc.status_code, exc.detail)

        @_app.exception_handler(BrokerClientException)
        async def handle_broker_client_exception(
            req: Request, exc: BrokerClientException
        ):
            return _response(400, str(exc))

        @_app.exception_handler(InvalidSessionException)
        async def handle_invalid_session_exception(
            req: Request, exc: InvalidSessionException
        ):
            return _response(401, str(exc))
