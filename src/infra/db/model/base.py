from datetime import datetime
from uuid import UUID

from sqlalchemy import DateTime, UUID as SaUUID
from sqlalchemy.orm import DeclarativeBase, mapped_column

from utils import get_datetime, get_uuid


def uuid_pk(**kw):
    """Helper function for UUID primary key columns."""
    return mapped_column(SaUUID(as_uuid=True), primary_key=True, default=get_uuid, **kw)


def datetime_tz(**kw):
    """Helper function for timezone-aware datetime columns."""
    if "nullable" not in kw:
        kw["nullable"] = False

    return mapped_column(DateTime(timezone=True), default=get_datetime, **kw)


class Base(DeclarativeBase):
    pass
