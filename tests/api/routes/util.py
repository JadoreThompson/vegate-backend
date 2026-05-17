from datetime import datetime, UTC

from sqlalchemy import insert

from enums import BrokerType, Timeframe, MarketType
from infra.db import get_db_session, get_db_sess_sync
from infra.db.model import User, OHLC


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


def seed_candles(n: int = 100):
    symbol_broker_tfs = (("AAPL", BrokerType.ALPACA, Timeframe.m1), ("AAPL", BrokerType.ALPACA, Timeframe.m5))

    with get_db_sess_sync() as db_sess:
        for symbol, broker, tf in symbol_broker_tfs:
            candles = [
                OHLC(
                    source=broker,
                    symbol=symbol,
                    timeframe=tf,
                    market_type=MarketType.STOCKS,
                    open=100.0,
                    high=100.0,
                    low=100.0,
                    close=100.0,
                    volume=10.0,
                    timestamp=int(datetime(year=2026, month=1, day=((1 + i) % 30) + 1, tzinfo=UTC).timestamp()),
                )
                for i in range(n)
            ]
            db_sess.add_all(candles)
        db_sess.flush()
        db_sess.commit()
