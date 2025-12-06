from datetime import datetime, UTC
from uuid import uuid4


def get_uuid():
    return uuid4()


def get_datetime():
    return datetime.now(UTC)
