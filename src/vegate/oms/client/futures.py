import json
import time
from uuid import UUID

import requests

from .exception import FuturesOMSClientException, FuturesOMSClientRetryExhausted
from ..schema import FuturesOrder, FuturesOrderRequest


class FuturesOMSClient:

    def __init__(self, base_url: str, *, retry_delay: int = 5, retry_attempts: int = 5):
        self._base_url = base_url.rstrip("/")
        self._token: str | None = None
        self._client = requests.Session()
        self._retry_delay = retry_delay
        self._retry_attempts = retry_attempts

    def create_session(self, deployment_id: UUID) -> None:
        response = self._request_with_retry(
            "POST",
            f"{self._base_url}/session",
            headers={"Content-Type": "application/json"},
            json={"deployment_id": str(deployment_id)},
        )
        self._token = response.json()["token"]

    def close_session(self) -> None:
        self._request_with_retry(
            "DELETE",
            f"{self._base_url}/session",
            headers=self._auth_header(),
        )
        self._token = None

    def get_balance(self) -> float:
        response = self._request_with_retry(
            "GET",
            f"{self._base_url}/balance", headers=self._auth_header()
        )
        return response.json()["balance"]

    def get_equity(self) -> float:
        response = self._request_with_retry(
            "GET",
            f"{self._base_url}/equity", headers=self._auth_header()
        )
        return response.json()["equity"]

    def get_position(self, symbol: str) -> float:
        response = self._request_with_retry(
            "GET",
            f"{self._base_url}/position?symbol={symbol}",
            headers=self._auth_header(),
        )
        return response.json()["balance"]

    def get_positions(self) -> list[dict]:
        response = self._request_with_retry(
            "GET",
            f"{self._base_url}/positions",
            headers=self._auth_header(),
        )
        return response.json()

    def place_order(self, request: FuturesOrderRequest) -> FuturesOrder:
        response = self._request_with_retry(
            "POST",
            f"{self._base_url}/orders",
            json=request.model_dump(mode="json"),
            headers=self._auth_header(),
        )
        return FuturesOrder.model_validate(response.json())

    def modify_order(
        self,
        order_id: UUID,
        limit_price: float | None = None,
        stop_price: float | None = None,
        take_profit: float | None = None,
        stop_loss: float | None = None,
    ) -> FuturesOrder:
        response = self._request_with_retry(
            "PATCH",
            f"{self._base_url}/orders/{order_id}",
            json={
                "limit_price": limit_price,
                "stop_price": stop_price,
                "take_profit": take_profit,
                "stop_loss": stop_loss,
            },
            headers=self._auth_header(),
        )
        return FuturesOrder.model_validate(response.json())

    def cancel_order(self, order_id: UUID) -> bool:
        response = self._request_with_retry(
            "DELETE",
            f"{self._base_url}/orders/{order_id}",
            headers=self._auth_header(),
        )
        return response.json()["success"]

    def cancel_all_orders(self) -> bool:
        response = self._request_with_retry(
            "DELETE",
            f"{self._base_url}/orders",
            headers=self._auth_header(),
        )
        return response.json()["success"]

    def get_order(self, order_id: UUID) -> FuturesOrder:
        response = self._request_with_retry(
            "GET",
            f"{self._base_url}/orders/{order_id}",
            headers=self._auth_header(),
        )
        return FuturesOrder.model_validate(response.json())

    def get_orders(self) -> list[FuturesOrder]:
        response = self._request_with_retry(
            "GET",
            f"{self._base_url}/orders",
            headers=self._auth_header(),
        )
        return [FuturesOrder.model_validate(o) for o in response.json()]

    def set_leverage(self, symbol: str, leverage: int) -> None:
        self._request_with_retry(
            "POST",
            f"{self._base_url}/leverage",
            json={"symbol": symbol, "leverage": leverage},
            headers=self._auth_header(),
        )

    def get_leverage(self, symbol: str) -> int:
        response = self._request_with_retry(
            "GET",
            f"{self._base_url}/leverage?symbol={symbol}",
            headers=self._auth_header(),
        )
        return response.json()["leverage"]

    def close(self) -> None:
        self._client.close()

    def disconnect(self) -> None:
        self.close()

    def _auth_header(self) -> dict[str, str]:
        if self._token is None:
            raise RuntimeError("No active session - call create_session() first")
        return {"Authorization": f"Bearer {self._token}"}

    def _raise_for_status(self, response: requests.Response) -> None:
        if not response.ok:
            data = None
            try:
                data = response.json()
            except json.JSONDecodeError:
                pass
            raise FuturesOMSClientException(f"{response.status_code} client error - {data}")

    def _request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        last_exception: Exception | None = None
        for attempt in range(1, self._retry_attempts + 1):
            try:
                response = self._client.request(method, url, **kwargs)
                if response.status_code < 500:
                    self._raise_for_status(response)
                    return response
            except FuturesOMSClientException:
                raise
            except Exception as e:
                last_exception = e
            if attempt < self._retry_attempts:
                time.sleep(self._retry_delay)
        raise FuturesOMSClientRetryExhausted(
            f"Failed after {self._retry_attempts} attempts: {last_exception}"
        )
