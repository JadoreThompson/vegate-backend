from datetime import datetime

from sqlalchemy import insert

from infra.db import get_db_session
from infra.db.model import User


async def create_user(username: str) -> User:
    async with get_db_session() as db_sess:
        user = await db_sess.scalar(
            insert(User)
            .values(
                username=username,
                email=f"{username}@email.com",
                password="password",
                authenticated_at=datetime(year=2024, month=1, day=1),
            )
            .returning(User)
        )
        await db_sess.commit()

    return user