import logging
from datetime import datetime
from typing import Generator

import requests

from module.broker.enums import BrokerType
from module.markets.enums import MarketType, Timeframe
from module.markets.schema import OHLC
from .exception import HistoricalDataClientException


class HistoricalDataClient:

    def __init__(self, base_url: str):
        self._base_url = base_url.rstrip("/")
        self._client = requests.Session()
        self._name = self.__class__.__name__
        self._logger = logging.getLogger(self._name)

    def fetch(
        self,
        symbol: str,
        market_type: MarketType,
        broker_type: BrokerType,
        timeframe: Timeframe,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
    ) -> Generator[OHLC, None, None]:
        page = 1
        limit = 200

        while True:
            params = {
                "symbol": symbol,
                "market_type": market_type,
                "broker_type": broker_type,
                "timeframe": timeframe,
                "page": page,
                "limit": limit
            }

            if start_date is not None:
                params["start_date"] = start_date
            if end_date is not None:
                params["end_date"] = end_date

            response = self._client.get(
                f"{self._base_url}/markets/bars",
                params=params,
            )
            self._raise_for_status(response)

            body = response.json()
            for c in body.get("data", []):
                yield OHLC(
                    open=c["open"],
                    high=c["high"],
                    low=c["low"],
                    close=c["close"],
                    volume=c["volume"],
                    timestamp=c["timestamp"],
                    timeframe=Timeframe(c["timeframe"]),
                    symbol=c["symbol"],
                    broker=BrokerType(c["broker"]),
                    market_type=MarketType(c["market_type"]),
                )

            if not body.get("has_next", False):
                break
            page += 1

    def close(self):
        self._client.close()

    def _raise_for_status(self, response: requests.Response):
        if not response.ok:
            data = None
            try:
                data = response.json()
            except Exception:
                pass
            raise HistoricalDataClientException(
                f"{response.status_code} client error - {data}"
            )
