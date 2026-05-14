import uvicorn
from fastapi import FastAPI, HTTPException, Depends
from uuid import UUID

from service.oms.server.model import (
    BalanceResponse,
    CreateSessionRequest,
    CreateSessionResponse,
    EquityResponse,
    ModifyOrderRequest,
    OrderResponse,
    PlaceOrderRequest,
    SuccessResponse,
)
from service.oms.service import OMSService

_app = FastAPI()


def get_bearer_token(authorization: str = None) -> str:
    """
    Extracts Bearer token from Authorization header.
    """
    if not authorization:
        raise HTTPException(status_code=401, detail="Missing Authorization header")

    parts = authorization.split(" ")
    if len(parts) != 2 or parts[0].lower() != "bearer":
        raise HTTPException(status_code=401, detail="Invalid Authorization format")

    return parts[1]


class OMSServer:

    def __init__(self, oms_service: OMSService, uvicorn_kw: dict):
        self._oms_service = oms_service
        self._uvicorn_kw = uvicorn_kw
        self._register_routes()

    def run(self):
        uvicorn.run(_app, **self._uvicorn_kw)

    def _register_routes(self):

        @_app.post("/session", response_model=CreateSessionResponse)
        async def create_session(body: CreateSessionRequest):
            try:
                deployment_id = UUID(body.deployment_id)
                token = await self._oms_service.create_session(deployment_id)
                return {"token": token}
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

        @_app.delete("/session")
        def close_session(token: str = Depends(get_bearer_token)):
            try:
                self._oms_service.close_session(token)
                return {"status": "closed"}
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

        @_app.get("/balance", response_model=BalanceResponse)
        def get_balance(token: str = Depends(get_bearer_token)):
            try:
                return {"balance": self._oms_service.get_balance(token)}
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

        @_app.get("/equity", response_model=EquityResponse)
        def get_equity(token: str = Depends(get_bearer_token)):
            try:
                return {"equity": self._oms_service.get_equity(token)}
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

        @_app.post("/orders")
        def place_order(
            body: PlaceOrderRequest, token: str = Depends(get_bearer_token)
        ):
            try:
                return self._oms_service.place_order(token, body)
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

        @_app.patch("/orders/{order_id}")
        def modify_order(
            order_id: str,
            body: ModifyOrderRequest,
            token: str = Depends(get_bearer_token),
        ):
            try:
                return self._oms_service.modify_order(
                    token,
                    order_id,
                    limit_price=body.limit_price,
                    stop_price=body.stop_price,
                )
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

        @_app.delete("/orders/{order_id}", response_model=SuccessResponse)
        def cancel_order(order_id: str, token: str = Depends(get_bearer_token)):
            try:
                return {"success": self._oms_service.cancel_order(token, order_id)}
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

        @_app.delete("/orders", response_model=SuccessResponse)
        def cancel_all_orders(token: str = Depends(get_bearer_token)):
            try:
                return {"success": self._oms_service.cancel_all_orders(token)}
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

        @_app.get("/orders/{order_id}", response_model=OrderResponse)
        def get_order(order_id: str, token: str = Depends(get_bearer_token)):
            try:
                return self._oms_service.get_order(token, order_id)
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))

        @_app.get("/orders", response_model=list[OrderResponse])
        def get_orders(token: str = Depends(get_bearer_token)):
            try:
                return self._oms_service.get_orders(token)
            except Exception as e:
                raise HTTPException(status_code=400, detail=str(e))
