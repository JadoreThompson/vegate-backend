import json
from uuid import UUID

import requests

from .schema import PlaceOrderRequest
from ..schema import Order, OrderRequest


class OMSClientException(Exception):
    pass


class OMSClient:

    def __init__(self, base_url: str):
        super().__init__()
        self._base_url = base_url.rstrip("/")
        self._token: str | None = None
        self._client = requests.Session()

    def create_session(self, deployment_id: UUID) -> None:
        response = self._client.post(
            f"{self._base_url}/session",
            headers={"Content-Type": "application/json"},
            json={"deployment_id": str(deployment_id)},
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
        """
        Retrieve the cash balance for your account

        Returns:
            float: Current cash balance
        """        
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

    def get_position(self, symbol):
        response = self._client.get(
            f"{self._base_url}/position?symbol={symbol}",
            headers=self._auth_header(),
        )
        self._raise_for_status(response)
        return response.json()["balance"]

    def place_order(self, request: OrderRequest, candle_ts: int) -> Order:
        response = self._client.post(
            f"{self._base_url}/orders",
            json=PlaceOrderRequest(order=request, candle_ts=candle_ts).model_dump(
                mode="json"
            ),
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
        """Raise an exception if the HTTP response indicates an error.

        Attempts to extract the response body as JSON and includes it in the
        exception message when available. If the response body cannot be parsed
        as JSON, only the status code is included.

        Args:
            response: The HTTP response returned by the OMS API.

        Raises:
            OMSClientException: If the response status code indicates a client
                or server error.
        """
        if not response.ok:
            data = None
            try:
                data = response.json()
            except json.JSONDecodeError:
                pass
            raise OMSClientException(f"{response.status_code} client error - {data}")
