from sqlalchemy.orm import Session

from utils.db import smaker_sync
from .base import BaseOHLCVLoader


class DBOHLCVLoader(BaseOHLCVLoader):
    def __init__(self):
        super().__init__()
        self._db_sess: Session = smaker_sync()

    def yield_historic_ohlcv(self, symbol, start_date, end_date): ...
