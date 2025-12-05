from alpaca.trading.client import TradingClient
from .base import BaseOHLCVLoader


class AlpacaOHLCVLoader(BaseOHLCVLoader):
    def __init__(self, client: TradingClient):
        super().__init__()
        self._client = client

    def yield_historic_ohlcv(self, symbol, start_date, end_date):
        return super().yield_historic_ohlcv(symbol, start_date, end_date)
