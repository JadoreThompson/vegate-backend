from datetime import datetime
from decimal import Decimal
from engine.enums import Timeframe


class OHLCV:
    def __init__(
        self,
        symbol: str,
        timestamp: datetime,
        open: Decimal,
        high: Decimal,
        low: Decimal,
        close: Decimal,
        volume: int,
        timeframe: Timeframe,
    ):
        self.symbol = symbol
        self.timestamp = timestamp
        self.open = open
        self.high = high
        self.low = low
        self.close = close
        self.volume = volume
        self.timeframe = timeframe

    def __repr__(self) -> str:
        return (
            f"OHLCV(symbol={self.symbol}, timestamp={self.timestamp}, "
            f"open={self.open}, high={self.high}, low={self.low}, "
            f"close={self.close}, volume={self.volume}, "
            f"timeframe={self.timeframe})"
        )
