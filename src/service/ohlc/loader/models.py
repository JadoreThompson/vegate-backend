from dataclasses import dataclass
from datetime import datetime


@dataclass(slots=True, frozen=True)
class LoadResult:
    start_date: datetime | None
    end_date: datetime | None
    count: int
