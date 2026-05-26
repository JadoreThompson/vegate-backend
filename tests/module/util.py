from datetime import datetime, UTC

from sqlalchemy import insert
from sqlalchemy.dialects.postgresql import insert as pg_insert

from module.broker.enums import BrokerType
from module.markets.enums import MarketType, Timeframe
from core.db import get_db_sess_sync, get_db_session
from module.markets.model import OHLC, Instrument
from module.user.model import User


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


def seed_candles():
    symbol_broker_tfs = (
        ("AAPL", BrokerType.ALPACA, MarketType.STOCKS, Timeframe.m1),
        ("AAPL", BrokerType.ALPACA, MarketType.STOCKS, Timeframe.m5),
    )

    with get_db_sess_sync() as db_sess:
        for symbol, broker, market_type, tf in symbol_broker_tfs:
            instrument_id = db_sess.scalar(
                pg_insert(Instrument)
                .values(
                    symbol=symbol,
                    native_symbol=symbol,
                    broker_type=broker,
                    market_type=market_type,
                )
                .on_conflict_do_nothing(
                    index_elements=["symbol", "market_type", "broker_type"]
                )
                .returning(Instrument.id)
            )
            print("Instrument id:", instrument_id)
            if instrument_id is None:
                continue

            candles = [
                OHLC(
                    timeframe=tf,
                    instrument_id=instrument_id,
                    open=100.0,
                    high=100.0,
                    low=100.0,
                    close=100.0,
                    volume=10.0,
                    timestamp=int(
                        datetime(
                            year=2026, month=1, day=((1 + i) % 30) + 1, tzinfo=UTC
                        ).timestamp()
                    ),
                )
                for i in range(30)
            ]
            db_sess.add_all(candles)

        # db_sess.flush()
        db_sess.commit()
