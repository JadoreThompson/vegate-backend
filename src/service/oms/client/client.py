import json
from uuid import UUID

import requests

from models import Order
from service.oms.broker_client.base import BrokerClient
from service.oms.broker_client.model import OrderRequest
from service.oms.client.exception import OMSClientException
from service.oms.server.model import PlaceOrderRequest


class OMSClient(BrokerClient):

    def __init__(self, base_url: str):
        self._base_url = base_url.rstrip("/")
        self._token: str | None = None
        self._client = requests.Session()

    def create_session(self, deployment_id: UUID) -> None:
        response = self._client.post(
            f"{self._base_url}/session", headers={"Content-Type": "application/json"}, json={"deployment_id": str(deployment_id)}
        )
        self._raise_for_status(response)
        self._token = response.json()["token"]

    def close_session(self) -> None:
        response = self._client.delete(
            f"{self._base_url}/session", headers=self._auth_header()
        )
        self._raise_for_status(response)
        self._token = None

    def get_balance(self) -> float:
        response = self._client.get(
            f"{self._base_url}/balance", headers=self._auth_header()
        )
        self._raise_for_status(response)
        return response.json()["balance"]

    def get_equity(self) -> float:
        response = self._client.get(
            f"{self._base_url}/equity", headers=self._auth_header()
        )
        self._raise_for_status(response)
        return response.json()["equity"]

    def place_order(self, order: OrderRequest, candle_ts: int) -> Order:
        response = self._client.post(
            f"{self._base_url}/orders",
            json=PlaceOrderRequest(
                order=order,
                candle_ts=candle_ts,
            ).model_dump(mode="json"),
            headers=self._auth_header(),
        )
        self._raise_for_status(response)
        return Order.model_validate(response.json())

    def modify_order(
        self,
        order_id: UUID,
        limit_price: float | None = None,
        stop_price: float | None = None,
    ) -> Order:
        response = self._client.patch(
            f"{self._base_url}/orders/{order_id}",
            json={
                "limit_price": limit_price,
                "stop_price": stop_price,
            },
            headers=self._auth_header(),
        )
        self._raise_for_status(response)
        return Order.model_validate(response.json())

    def cancel_order(self, order_id: UUID) -> bool:
        response = self._client.delete(
            f"{self._base_url}/orders/{order_id}", headers=self._auth_header()
        )
        self._raise_for_status(response)
        return response.json()["success"]

    def cancel_all_orders(self) -> bool:
        response = self._client.delete(
            f"{self._base_url}/orders", headers=self._auth_header()
        )
        self._raise_for_status(response)
        return response.json()["success"]

    def get_order(self, order_id: UUID) -> Order:
        response = self._client.get(
            f"{self._base_url}/orders/{order_id}", headers=self._auth_header()
        )
        self._raise_for_status(response)
        return Order.model_validate(response.json())

    def get_orders(self) -> list[Order]:
        response = self._client.get(
            f"{self._base_url}/orders", headers=self._auth_header()
        )
        self._raise_for_status(response)
        return [Order.model_validate(o) for o in response.json()]

    def close(self) -> None:
        self._client.close()

    def disconnect(self):
        return self.close()

    def _auth_header(self) -> dict[str, str]:
        if self._token is None:
            raise RuntimeError("No active session - call create_session() first")

        return {
            "Authorization": f"Bearer {self._token}",
        }
    
    def _raise_for_status(self, response: requests.Response):
        if not response.ok:
            data = None
            try:
                data = response.json()
            except json.JSONDecodeError:
                pass
            raise OMSClientException(f"{response.status_code} client error - {data}")
