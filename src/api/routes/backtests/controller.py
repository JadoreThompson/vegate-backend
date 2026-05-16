from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession

from enums import BacktestStatus
from infra.db.model import Backtest, Orders, Strategy
from infra.db.model.ohlc import OHLC
from .model import CreateBacktestRequest


async def create_backtest(
    user_id: UUID, data: CreateBacktestRequest, db_sess: AsyncSession
) -> Backtest:
    """Create a new backtest."""
    # Verify the strategy exists and belongs to the user
    strategy = await db_sess.scalar(
        select(Strategy).where(
            Strategy.strategy_id == data.strategy_id,
            Strategy.user_id == user_id,
        )
    )
    if not strategy:
        raise HTTPException(404, "Strategy not found")
    
    res = await db_sess.execute(select(exists()).where(OHLC.symbol == data.symbol, OHLC.market_type == data.market_type, OHLC.timeframe == data.timeframe))
    exists = res.scalar()
    if not exists:
        raise HTTPException(404, f"Data for {data.symbol} on timeframe {data.timeframe} not available.")

    new_backtest = Backtest(
        strategy_id=data.strategy_id,
        symbol=data.symbol,
        broker=data.broker,
        starting_balance=data.starting_balance,
        start_date=data.start_date,
        end_date=data.end_date,
        timeframe=data.timeframe,
        market_type=data.market_type,
    )
    db_sess.add(new_backtest)
    await db_sess.flush()
    await db_sess.refresh(new_backtest)
    return new_backtest


async def get_backtest(backtest_id: UUID, db_sess: AsyncSession) -> Backtest | None:
    """Get a backtest by ID."""
    return await db_sess.scalar(select(Backtest).where(Backtest.id == backtest_id))


async def list_backtests(
    user_id: UUID,
    db_sess: AsyncSession,
    status: list[BacktestStatus] | None = None,
    symbols: list[str] | None = None,
    offset: int = 0,
    limit: int = 100,
) -> list[Backtest]:
    """List all backtests for a user with pagination."""
    stmt = (
        select(Backtest)
        .join(Strategy)
        .where(Strategy.user_id == user_id)
        .offset(offset)
        .limit(limit)
        .order_by(Backtest.created_at.desc())
    )

    if status is not None:
        stmt = stmt.where(Backtest.status.in_(status))
    if symbols is not None:
        stmt = stmt.where(Backtest.symbol.in_(symbols))

    result = await db_sess.execute(stmt)
    return list(result.scalars().all())


async def delete_backtest(
    user_id: UUID, backtest_id: UUID, db_sess: AsyncSession
) -> bool:
    """Delete a backtest. Returns True if deleted, False if not found."""
    backtest = await get_backtest(backtest_id, db_sess)
    if not backtest:
        return False

    # Verify ownership through strategy relationship
    strategy = await db_sess.scalar(
        select(Strategy).where(Strategy.strategy_id == backtest.strategy_id)
    )
    if not strategy or strategy.user_id != user_id:
        return False

    await db_sess.delete(backtest)
    return True


async def get_backtest_orders(
    user_id: UUID,
    backtest_id: UUID,
    db_sess: AsyncSession,
    offset: int = 0,
    limit: int = 100,
) -> list[Orders]:
    """Get all orders for a backtest with pagination."""
    # First verify the backtest exists and user has access
    backtest = await get_backtest(backtest_id, db_sess)
    if not backtest:
        raise HTTPException(404, "Backtest not found")

    # Verify ownership through strategy relationship
    strategy = await db_sess.scalar(
        select(Strategy).where(Strategy.strategy_id == backtest.strategy_id)
    )
    if not strategy or strategy.user_id != user_id:
        raise HTTPException(404, "Backtest not found")
    # Get orders for this backtest
    result = await db_sess.execute(
        select(Orders)
        .where(Orders.backtest_id == backtest_id)
        .offset(offset)
        .limit(limit)
        .order_by(Orders.submitted_at.asc())
    )
    return list(result.scalars().all())
