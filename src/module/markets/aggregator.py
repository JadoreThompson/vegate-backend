from vegate.markets.enums import Timeframe
from vegate.markets.schema import OHLC as OHLCSchema


class CandleAggregator:
    """
    Aggregates 1m bars into higher timeframe candles.
    """

    def __init__(self, timeframe: Timeframe):
        self._timeframe = timeframe
        self._bucket_seconds = timeframe.get_seconds()

        self._open: float | None = None
        self._high: float | None = None
        self._low: float | None = None
        self._close: float | None = None
        self._volume: float | None = None
        self._bucket_end: int | None = None
        self._started: bool = False
        self._symbol: str = ""
        self._broker = None
        self._market_type = None

    @property
    def timeframe(self) -> Timeframe:
        return self._timeframe

    def add_bar(self, bar: OHLCSchema) -> OHLCSchema | None:
        """Feed a 1m bar into the pipeline.

        Returns a *completed* candle when a bucket boundary is crossed,
        or ``None`` otherwise.
        """
        if self._timeframe == Timeframe.m1 and bar.timeframe == Timeframe.m1:
            return bar

        t = bar.timestamp
        bucket_end = ((t // self._bucket_seconds) + 1) * self._bucket_seconds

        if not self._started:
            if t % self._bucket_seconds != 0:
                return None
            self._started = True
            self._start_bucket(bar, bucket_end)
            return None

        if bucket_end > self._bucket_end:
            completed = self._finalize_candle()
            self._start_bucket(bar, bucket_end)
            return completed

        self._accumulate(bar)
        return None

    def reset(self) -> None:
        self._open = None
        self._high = None
        self._low = None
        self._close = None
        self._volume = None
        self._bucket_end = None
        self._started = False
        self._symbol = ""
        self._broker = None
        self._market_type = None

    def _start_bucket(self, bar: OHLCSchema, bucket_end: int) -> None:
        self._open = bar.open
        self._high = bar.high
        self._low = bar.low
        self._close = bar.close
        self._volume = bar.volume
        self._bucket_end = bucket_end
        self._symbol = bar.symbol
        self._broker = bar.broker
        self._market_type = bar.market_type

    def _accumulate(self, bar: OHLCSchema) -> None:
        self._high = max(self._high, bar.high)
        self._low = min(self._low, bar.low)
        self._close = bar.close
        self._volume += bar.volume

    def _finalize_candle(self) -> OHLCSchema:
        return OHLCSchema(
            open=self._open,
            high=self._high,
            low=self._low,
            close=self._close,
            volume=self._volume,
            timestamp=self._bucket_end,
            timeframe=self._timeframe,
            symbol=self._symbol,
            broker=self._broker,
            market_type=self._market_type,
        )
