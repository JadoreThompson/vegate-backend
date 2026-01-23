import os
import configparser
from contextlib import asynccontextmanager, contextmanager
import shutil
from typing import AsyncGenerator, Generator

from sqlalchemy import create_engine
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker, Session

from config import (
    DB_HOST_CREDS,
    DB_NAME,
    DB_PASSWORD,
    DB_USERNAME,
    DB_USER_CREDS,
    PARENT_PATH,
)


DB_ENGINE = create_async_engine(
    f"postgresql+asyncpg://{DB_USER_CREDS}@{DB_HOST_CREDS}/{DB_NAME}"
)
DB_ENGINE_SYNC = create_engine(
    f"postgresql+psycopg2://{DB_USER_CREDS}@{DB_HOST_CREDS}/{DB_NAME}"
)

smaker = sessionmaker(DB_ENGINE, class_=AsyncSession, autocommit=False, autoflush=False)
smaker_sync = sessionmaker(
    DB_ENGINE_SYNC, class_=Session, autocommit=False, autoflush=False
)


@asynccontextmanager
async def get_db_sess() -> AsyncGenerator[AsyncSession, None]:
    async with smaker.begin() as s:
        try:
            yield s
        except:
            await s.rollback()
            raise


@contextmanager
def get_db_sess_sync() -> Generator[Session, None, None]:
    with smaker_sync.begin() as s:
        try:
            yield s
        except:
            s.rollback()
            raise


def write_db_url_to_alembic_ini() -> None:
    db_password = DB_PASSWORD.replace("%", "%%")
    db_url = (
        f"postgresql+psycopg2://{DB_USERNAME}:{db_password}@{DB_HOST_CREDS}/{DB_NAME}"
    )

    fpath = os.path.join(PARENT_PATH, "alembic.ini")
    if not os.path.exists(fpath):
        example_fpath = os.path.join(PARENT_PATH, "alembic.ini.example")
        if not os.path.exists(example_fpath):
            raise FileNotFoundError(
                f"Failed to find alembic.ini and alembic.ini.exmaple at"
                f"{fpath} and {example_fpath}"
            )
        shutil.copyfile(example_fpath, fpath)

    config = configparser.ConfigParser()
    config.read(fpath, encoding="utf-8")

    config.set("alembic", "sqlalchemy.url", db_url)

    with open(fpath, "w", encoding="utf-8") as f:
        config.write(f)
