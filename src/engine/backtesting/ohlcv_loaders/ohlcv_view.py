from engine.core import OHLCV


class OHLCVView:
    def __init__(self, candles: list[OHLCV]):
        self._candles = candles

    def __iter__(self):
        return iter(self._candles)
    
    def __getitem__(self, idx: int):
        return self._candles[idx]