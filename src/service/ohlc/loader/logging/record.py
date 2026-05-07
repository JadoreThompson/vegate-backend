from enum import Enum
from pydantic import BaseModel


class LogRecordType(str, Enum):
    OHLC_LOAD_START = "ohlc_load_start"
    OHLC_LOAD_COMPLETE = "ohlc_load_complete"


class LogRecord(BaseModel):
    type: LogRecordType
    params: dict


class OHLCLoadStartRecord(LogRecord):
    type: LogRecordType = LogRecordType.OHLC_LOAD_START


class OHLCLoadCompleteRecord(LogRecord):
    type: LogRecordType = LogRecordType.OHLC_LOAD_COMPLETE
    count: int