import logging
from abc import abstractmethod
from collections import deque
from datetime import datetime
from typing import Generator

from engine.core import OHLCV, Timeframe
from .ohlcv_view import OHLCVView


class BaseOHLCVLoader:
    def __init__(
        self, symbol: str, timeframe: Timeframe, *, history_bars: int | None = None
    ):
        self._symbol = symbol
        self._timeframe = timeframe

        if history_bars is not None:
            self._history_bars = deque(maxlen=history_bars)
            self._ohlcv_view = OHLCVView(self._history_bars)
        else:
            self._history_bars = None
            self._ohlcv_view = None

        self._logger = logging.getLogger(self.__class__.__name__)

    @property
    def symbol(self):
        return self._symbol

    @property
    def timeframe(self):
        return self._timeframe

    @property
    def history(self):
        return self._ohlcv_view

    @abstractmethod
    def yield_historic_ohlcv(
        self, start_date: datetime, end_date: datetime
    ) -> Generator[OHLCV, None, None]: ...

    @abstractmethod
    def yield_ohlcv(self) -> Generator[OHLCV, None, None]: ...

    @abstractmethod
    def load_historic_olhcv(self, start_date: datetime, end_date: datetime) -> None: ...
