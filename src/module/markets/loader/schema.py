from dataclasses import dataclass
from datetime import datetime

@dataclass
class OHLCLoadResult:
    count: int
    start_date: datetime
    end_date: datetime
