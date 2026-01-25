from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from enums import BacktestStatus
from infra.db.models import Backtests, Orders, Strategies
from .models import BacktestCreate


async def create_backtest(
    user_id: UUID, data: BacktestCreate, db_sess: AsyncSession
) -> Backtests:
    """Create a new backtest."""
    # Verify the strategy exists and belongs to the user
    strategy = await db_sess.scalar(
        select(Strategies).where(
            Strategies.strategy_id == data.strategy_id,
            Strategies.user_id == user_id,
        )
    )
    if not strategy:
        raise HTTPException(404, "Strategy not found")

    new_backtest = Backtests(
        strategy_id=data.strategy_id,
        symbol=data.symbol,
        starting_balance=data.starting_balance,
        start_date=data.start_date,
        end_date=data.end_date,
        timeframe=data.timeframe,
    )
    db_sess.add(new_backtest)
    await db_sess.flush()
    await db_sess.refresh(new_backtest)
    return new_backtest


async def get_backtest(backtest_id: UUID, db_sess: AsyncSession) -> Backtests | None:
    """Get a backtest by ID."""
    return await db_sess.scalar(
        select(Backtests).where(Backtests.backtest_id == backtest_id)
    )


async def list_backtests(
    user_id: UUID,
    db_sess: AsyncSession,
    status: list[BacktestStatus] | None = None,
    symbols: list[str] | None = None,
    offset: int = 0,
    limit: int = 100,
) -> list[Backtests]:
    """List all backtests for a user with pagination."""
    stmt = (
        select(Backtests)
        .join(Strategies)
        .where(Strategies.user_id == user_id)
        .offset(offset)
        .limit(limit)
        .order_by(Backtests.created_at.desc())
    )

    if status is not None:
        stmt = stmt.where(Backtests.status.in_(status))
    if symbols is not None:
        stmt = stmt.where(Backtests.symbol.in_(symbols))

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
        select(Strategies).where(Strategies.strategy_id == backtest.strategy_id)
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
        select(Strategies).where(Strategies.strategy_id == backtest.strategy_id)
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
