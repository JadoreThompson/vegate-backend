import os
import configparser
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import sessionmaker

from config import DB_ENGINE, DB_HOST_CREDS, DB_NAME, DB_USER_CREDS


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


def write_db_url_to_alembic_ini(alembic_ini_path: str = "alembic.ini") -> None:
    db_url = f"postgresql+psycopg2://{DB_USER_CREDS}@{DB_HOST_CREDS}/{DB_NAME}"

    safe_db_url = db_url.replace("%", "%%")

    if not os.path.exists(alembic_ini_path):
        raise FileNotFoundError(f"{alembic_ini_path} not found")

    config = configparser.ConfigParser()
    config.read(alembic_ini_path, encoding="utf-8")

    if "alembic" not in config.sections():
        config.add_section("alembic")

    config.set("alembic", "sqlalchemy.url", safe_db_url)

    with open(alembic_ini_path, "w", encoding="utf-8") as f:
        config.write(f)
