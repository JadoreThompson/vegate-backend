import json
import os
from typing import Generator

from config import OHLC_LOG_FOLDER
from enums import BrokerType, Timeframe
from .record import LogRecord, LogRecordType, OHLCLoadCompleteRecord, OHLCLoadStartRecord


class WALogger:
    def __init__(self, broker_type: BrokerType, symbol: str, timeframe: Timeframe):
        self._broker_type = broker_type
        self._symbol = symbol
        self._timeframe = timeframe
        self._fname = os.path.join(
            OHLC_LOG_FOLDER, broker_type.value, symbol, timeframe.value
        ) + ".log"

    @property
    def broker_type(self) -> BrokerType:
        return self._broker_type

    @property
    def symbol(self) -> str:
        return self._symbol

    @property
    def timeframe(self) -> Timeframe:
        return self._timeframe
    
    @property
    def fname(self) -> str:
        return self._fname

    def log(self, record: LogRecord) -> None:
        """Append a log record to the WAL file."""
        with open(self._fname, "a") as f:
            f.write(record.model_dump_json() + "\n")

    def read_logs(self) -> Generator[LogRecord]:
        """Read log records from the WAL file."""
        if not os.path.exists(self._fname):
            return

        with open(self._fname, "r") as f:
            for line in f:
                data = json.loads(line.strip())
                record_type = data.get("type")
                if record_type == LogRecordType.OHLC_LOAD_START:
                    yield OHLCLoadStartRecord(**data)
                elif record_type == LogRecordType.OHLC_LOAD_COMPLETE:
                    yield OHLCLoadCompleteRecord(**data)
                else:
                    raise ValueError(f"Unknown log record type: {record_type} in line: {line.strip()}")
