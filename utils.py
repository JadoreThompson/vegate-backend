from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker

from config import DB_ENGINE


smaker = sessionmaker(DB_ENGINE, class_=AsyncSession, autocommit=False, autoflush=False)


def get_datetime():
    return datetime.now(UTC)


@asynccontextmanager
async def get_db_sess() -> AsyncGenerator[AsyncSession, None]:
    async with smaker.begin() as s:
        try:
            yield s
        except:
            await s.rollback()
            raise
