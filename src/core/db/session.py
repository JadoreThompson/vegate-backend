from contextlib import asynccontextmanager, contextmanager
from typing import AsyncGenerator, Generator

from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.asyncio import AsyncSession

from .client import DB_ENGINE, DB_ENGINE_SYNC

smaker = sessionmaker(bind=DB_ENGINE, class_=AsyncSession, expire_on_commit=False)
smaker_sync = sessionmaker(bind=DB_ENGINE_SYNC, class_=Session, expire_on_commit=False)


@asynccontextmanager
async def get_db_session() -> AsyncGenerator[AsyncSession, None]:
    global smaker

    async with smaker.begin() as session:
        try:
            yield session
        except:
            await session.rollback()
            raise


@contextmanager
def get_db_sess_sync() -> Generator[Session, None, None]:
    global smaker_sync

    with smaker_sync.begin() as sess:
        try:
            yield sess
        except:
            sess.rollback()
            raise
