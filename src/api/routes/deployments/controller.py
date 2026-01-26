import logging
from datetime import datetime, timedelta
from typing import NamedTuple
from uuid import UUID

import numpy as np
from fastapi import HTTPException
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from api.shared.models import PerformanceMetrics
from config import REDIS_DEPLOYMENT_EVENTS_KEY
from enums import BrokerType, Timeframe, DeploymentStatus
from events.deployment import DeploymentStopEvent
from infra.db.models import (
    AccountSnapshots,
    BrokerConnections,
    Orders,
    Strategies,
    StrategyDeployments,
)
from infra.redis import REDIS_CLIENT
from models import EquityCurvePoint
from utils import get_datetime
from .models import DeployStrategyRequest


logger = logging.getLogger("deployments.controller")


class Point(NamedTuple):
    timestamp: datetime
    quantity: float


def calculate_sharpe_ratio(
    equity_curve: list[tuple[datetime, float]],
    risk_free_rate: float = 0.0,
    periods_per_year: int = 252,
) -> float:
    """
    Calculate the Sharpe ratio from an equity curve.

    The Sharpe ratio measures risk-adjusted return by comparing excess returns
    to volatility. Higher values indicate better risk-adjusted performance.

    Args:
        equity_curve: list of (timestamp, equity) tuples
        risk_free_rate: Annual risk-free rate (default: 0.0)
        periods_per_year: Number of periods per year for annualization (default: 252 for daily)

    Returns:
        Annualized Sharpe ratio

    Example:
        equity_curve = [
            (datetime(2024, 1, 1), 100000),
            (datetime(2024, 1, 2), 101000),
            (datetime(2024, 1, 3), 100500)
        ]
        sharpe = calculate_sharpe_ratio(equity_curve)
    """
    if len(equity_curve) < 2:
        logger.warning("Insufficient data for Sharpe ratio calculation")
        return 0.0

    try:
        # Extract equity values and convert Decimal to float
        equity_values = [equity for _, equity in equity_curve]

        # Calculate period returns
        returns = []
        for i in range(1, len(equity_values)):
            ret = (equity_values[i] - equity_values[i - 1]) / equity_values[i - 1]
            returns.append(ret)

        if not returns:
            return 0.0

        returns_array = np.array(returns)

        # Calculate mean and std of returns
        mean_return = np.mean(returns_array)
        std_return = np.std(returns_array, ddof=1)  # Sample std

        if std_return == 0 or np.isnan(std_return):
            logger.warning("Zero or invalid standard deviation in returns")
            return 0.0

        # Calculate daily risk-free rate
        daily_rf = risk_free_rate / periods_per_year

        # Calculate Sharpe ratio and annualize
        sharpe = ((mean_return - daily_rf) / std_return) * np.sqrt(periods_per_year)

        return float(sharpe)

    except Exception as e:
        logger.error(f"Error calculating Sharpe ratio: {e}", exc_info=True)
        return 0.0


def calculate_max_drawdown(
    equity_curve: list[tuple[datetime, float]],
    cash_curve: list[tuple[datetime, float]],
) -> tuple[float, float]:
    """
    Calculate maximum drawdown from an equity curve.

    Drawdown is the peak-to-trough decline in equity. Maximum drawdown
    represents the largest loss from a peak. When cash_curve is provided,
    it is used to understand the actual cash balance at each point in time.

    Args:
        equity_curve: list of (timestamp, equity) tuples
        cash_curve: Optional list of (timestamp, cash) tuples for cash balance tracking

    Returns:
        tuple of (max_drawdown_dollars, max_drawdown_percent)

    Example:
        equity_curve = [
            (datetime(2024, 1, 1), 100000),
            (datetime(2024, 1, 2), 95000),
            (datetime(2024, 1, 3), 98000)
        ]
        cash_curve = [
            (datetime(2024, 1, 1), 100000),
            (datetime(2024, 1, 2), 50000),
            (datetime(2024, 1, 3), 50000)
        ]
        dd_dollars, dd_percent = calculate_max_drawdown(equity_curve, cash_curve)
    """

    try:
        # Extract equity values and convert Decimal to float
        equity_values = [equity for _, equity in equity_curve]

        # If cash curve is provided, use it for additional context
        # This allows tracking actual cash balance at each point in time
        cash_values = None
        if cash_curve:
            cash_values = [cash for _, cash in cash_curve]
            if len(cash_values) != len(equity_values):
                logger.warning(
                    "Cash curve length doesn't match equity curve, ignoring cash data"
                )
                cash_values = None

        peak = equity_values[0]
        max_dd = 0.0
        max_dd_pct = 0.0

        for i, equity in enumerate(equity_values):
            # Update peak
            if equity > peak:
                peak = equity

            # Calculate drawdown from peak
            dd = peak - equity
            dd_pct = (dd / peak * 100) if peak > 0 else 0.0
            # Log cash balance at drawdown points if available
            if cash_values and dd > 0:
                cash_at_time = cash_values[i]
                logger.debug(
                    f"Drawdown: ${dd:.2f} ({dd_pct:.2f}%), Cash: ${cash_at_time:.2f}"
                )

            # Update max drawdown
            if dd > max_dd:
                max_dd = dd
                max_dd_pct = dd_pct

        return -max_dd, -max_dd_pct

    except Exception as e:
        logger.error(f"Error calculating max drawdown: {e}", exc_info=True)
        return 0.0, 0.0


async def create_deployment(
    user_id: UUID,
    strategy_id: UUID,
    data: DeployStrategyRequest,
    db_sess: AsyncSession,
) -> StrategyDeployments:
    """
    Create a new deployment for a strategy.

    Verifies that both the strategy and broker connection exist and belong to the user,
    then creates a new deployment record with PENDING status.
    """
    # Verify the strategy exists and belongs to the user
    strategy = await db_sess.scalar(
        select(Strategies).where(
            Strategies.strategy_id == strategy_id,
            Strategies.user_id == user_id,
        )
    )
    if not strategy:
        raise HTTPException(404, "Strategy not found")

    # Verify the broker connection exists and belongs to the user
    broker_connection = await db_sess.scalar(
        select(BrokerConnections).where(
            BrokerConnections.connection_id == data.broker_connection_id,
            BrokerConnections.user_id == user_id,
        )
    )
    if not broker_connection:
        raise HTTPException(404, "Broker connection not found")

    # Create the deployment
    new_deployment = StrategyDeployments(
        strategy_id=strategy_id,
        broker_connection_id=data.broker_connection_id,
        symbol=data.symbol,
        timeframe=data.timeframe,
        status=DeploymentStatus.PENDING,
    )

    db_sess.add(new_deployment)
    await db_sess.flush()
    await db_sess.refresh(new_deployment)

    return new_deployment


async def get_deployment(
    deployment_id: UUID, db_sess: AsyncSession
) -> StrategyDeployments | None:
    """Get a deployment by ID."""
    return await db_sess.scalar(
        select(StrategyDeployments).where(
            StrategyDeployments.deployment_id == deployment_id
        )
    )


async def list_strategy_deployments(
    user_id: UUID,
    strategy_id: UUID,
    db_sess: AsyncSession,
    offset: int = 0,
    limit: int = 100,
) -> list[StrategyDeployments]:
    """
    List all deployments for a specific strategy with pagination.

    Returns deployments ordered by creation date (newest first).
    """
    # First verify the strategy exists and belongs to the user
    strategy = await db_sess.scalar(
        select(Strategies).where(
            Strategies.strategy_id == strategy_id,
            Strategies.user_id == user_id,
        )
    )
    if not strategy:
        raise HTTPException(404, "Strategy not found")

    # Get deployments for this strategy
    result = await db_sess.execute(
        select(StrategyDeployments)
        .where(StrategyDeployments.strategy_id == strategy_id)
        .offset(offset)
        .limit(limit)
        .order_by(StrategyDeployments.created_at.desc())
    )
    return list(result.scalars().all())


async def list_all_deployments(
    user_id: UUID,
    db_sess: AsyncSession,
    offset: int = 0,
    limit: int = 100,
    status: DeploymentStatus | None = None,
) -> list[StrategyDeployments]:
    """
    List all deployments for a user with optional status filter and pagination.

    Returns deployments ordered by creation date (newest first).
    """
    query = (
        select(StrategyDeployments)
        .join(Strategies)
        .where(Strategies.user_id == user_id)
    )

    # Apply status filter if provided
    if status is not None:
        query = query.where(StrategyDeployments.status == status)

    query = (
        query.offset(offset)
        .limit(limit)
        .order_by(StrategyDeployments.created_at.desc())
    )

    result = await db_sess.execute(query)
    return list(result.scalars().all())


async def stop_deployment(
    user_id: UUID,
    deployment_id: UUID,
    db_sess: AsyncSession,
) -> StrategyDeployments:
    """
    Stop a running deployment.

    Updates the deployment status to STOPPED and sets the stopped_at timestamp.
    Can only stop deployments that are currently RUNNING or PENDING.
    """
    deployment = await get_deployment(deployment_id, db_sess)
    if not deployment:
        raise HTTPException(404, "Deployment not found")

    strategy = await db_sess.scalar(
        select(Strategies).where(Strategies.strategy_id == deployment.strategy_id)
    )

    if not strategy or strategy.user_id != user_id:
        raise HTTPException(404, "Deployment not found")

    if deployment.status == DeploymentStatus.STOP_REQUESTED:
        raise HTTPException(400, "Deployment has already been requested to stop")

    if deployment.status == DeploymentStatus.STOPPED:
        raise HTTPException(400, "Deployment is already stopped")

    if deployment.status == DeploymentStatus.ERROR:
        raise HTTPException(400, "Cannot stop a deployment in ERROR state")

    deployment.status = DeploymentStatus.STOP_REQUESTED

    await db_sess.flush()
    await db_sess.refresh(deployment)

    event = DeploymentStopEvent(deployment_id=deployment_id)
    await REDIS_CLIENT.publish(REDIS_DEPLOYMENT_EVENTS_KEY, event.model_dump_json())

    return deployment


async def get_deployment_orders(
    user_id: UUID,
    deployment_id: UUID,
    db_sess: AsyncSession,
    offset: int = 0,
    limit: int = 100,
) -> list[Orders]:
    """
    Get all orders for a deployment with pagination.

    Returns orders ordered by submission time (oldest first).
    """

    deployment = await get_deployment(deployment_id, db_sess)
    if not deployment:
        raise HTTPException(404, "Deployment not found")

    strategy = await db_sess.scalar(
        select(Strategies).where(Strategies.strategy_id == deployment.strategy_id)
    )
    if not strategy or strategy.user_id != user_id:
        raise HTTPException(404, "Deployment not found")

    result = await db_sess.execute(
        select(Orders)
        .where(Orders.deployment_id == deployment_id)
        .offset(offset)
        .limit(limit)
        .order_by(Orders.submitted_at.asc())
    )
    return list(result.scalars().all())


async def build_weekly_equity_graph(
    deployment: StrategyDeployments,
    broker: BrokerType,
    db_sess: AsyncSession,
) -> list[EquityCurvePoint]:
    """Build a weekly equity graph using account snapshots.

    Returns 6 data points spanning the last 7 days from equity snapshots.
    """
    # Get equity snapshots from the last 7 days
    now = get_datetime()
    week_ago = now - timedelta(days=7)

    result = await db_sess.execute(
        select(AccountSnapshots)
        .where(
            AccountSnapshots.deployment_id == deployment.deployment_id,
            AccountSnapshots.snapshot_type == "equity",
            AccountSnapshots.timestamp >= week_ago,
        )
        .order_by(AccountSnapshots.timestamp.asc())
    )
    snapshots = list(result.scalars().all())

    # If no snapshots, return default graph with starting balance
    if not snapshots:
        starting_balance = deployment.starting_balance or 0
        return [
            EquityCurvePoint(timestamp=now - timedelta(days=7), value=starting_balance),
            EquityCurvePoint(timestamp=now - timedelta(days=6), value=starting_balance),
            EquityCurvePoint(timestamp=now - timedelta(days=5), value=starting_balance),
            EquityCurvePoint(timestamp=now - timedelta(days=3), value=starting_balance),
            EquityCurvePoint(timestamp=now - timedelta(days=1), value=starting_balance),
            EquityCurvePoint(timestamp=now, value=starting_balance),
        ]

    # Define 6 target timestamps across the week
    timestamps = [
        week_ago,
        week_ago + timedelta(days=1.4),  # ~1.4 days
        week_ago + timedelta(days=2.8),  # ~2.8 days
        week_ago + timedelta(days=4.7),  # ~4.7 days
        week_ago + timedelta(days=6),  # ~6 days
        now,
    ]

    equity_graph: list[EquityCurvePoint] = []

    # For each target timestamp, find the closest snapshot
    for target_time in timestamps:
        # Find the snapshot closest to (but not after) this timestamp
        closest_snapshot = None
        min_diff = float("inf")

        for snapshot in snapshots:
            time_diff = (target_time - snapshot.timestamp).total_seconds()
            # Only consider snapshots at or before the target time
            if time_diff >= 0 and time_diff < min_diff:
                min_diff = time_diff
                closest_snapshot = snapshot

        if closest_snapshot:
            equity_graph.append(
                EquityCurvePoint(timestamp=target_time, value=closest_snapshot.value)
            )
        else:
            # If no snapshot found before this time, use starting balance
            starting_balance = deployment.starting_balance or 0
            equity_graph.append(
                EquityCurvePoint(timestamp=target_time, value=starting_balance)
            )

    return equity_graph


async def get_price_points(
    source: BrokerType,
    symbol: str,
    timeframe: Timeframe,
    start_date: datetime,
    end_date: datetime,
    db_sess: AsyncSession,
    n: int = 5,
) -> tuple[int, float]:
    sd = int(start_date.timestamp())
    ed = int(end_date.timestamp())

    query = f"""
    WITH time_points AS (
        SELECT
            generate_series(
                {sd},
                {ed},
                ({ed} - {sd}) / {n}
            ) AS target_time
    ),
    nearest_candles AS (
        SELECT DISTINCT ON (tp.target_time)
            tp.target_time,
            candle.close,
            candle.timestamp,
            ABS(candle.timestamp - tp.target_time) AS time_diff
        FROM time_points tp
        CROSS JOIN ohlc_levels candle
        WHERE 
            candle.timestamp BETWEEN {sd} AND {ed} 
            AND source = '{source.value}'
            AND symbol = '{symbol}'
            AND timeframe = '{timeframe.value}'
        ORDER BY tp.target_time, ABS(candle.timestamp - tp.target_time)
    )
    SELECT 
        target_time,
        close
    FROM nearest_candles
    ORDER BY target_time;
    """

    res = await db_sess.execute(text(query))

    return list(res.all())


async def calculate_deployment_metrics(
    deployment: StrategyDeployments,
    broker: BrokerType,
    db_sess: AsyncSession,
) -> PerformanceMetrics:
    """Calculate deployment metrics using account snapshots.

    Uses the AccountSnapshots table to get equity and balance data,
    which provides accurate tracking of account state over time.
    """
    total_trades = await db_sess.scalar(
        select(func.count()).where(Orders.deployment_id == deployment.deployment_id)
    )

    starting_balance = deployment.starting_balance or 0
    default_metrics = PerformanceMetrics(
        realised_pnl=0.0,
        unrealised_pnl=0.0,
        total_return_pct=0.0,
        sharpe_ratio=0.0,
        max_drawdown=0.0,
        total_trades=total_trades,
    )

    if not starting_balance:
        return default_metrics

    # Get equity snapshots ordered by timestamp
    equity_result = await db_sess.execute(
        select(AccountSnapshots)
        .where(
            AccountSnapshots.deployment_id == deployment.deployment_id,
            AccountSnapshots.snapshot_type == "equity",
        )
        .order_by(AccountSnapshots.timestamp.asc())
    )
    equity_snapshots = list(equity_result.scalars().all())

    # Get balance snapshots ordered by timestamp
    balance_result = await db_sess.execute(
        select(AccountSnapshots)
        .where(
            AccountSnapshots.deployment_id == deployment.deployment_id,
            AccountSnapshots.snapshot_type == "balance",
        )
        .order_by(AccountSnapshots.timestamp.asc())
    )
    balance_snapshots = list(balance_result.scalars().all())

    if not equity_snapshots or not balance_snapshots:
        return default_metrics

    # Build equity curve from snapshots (tuples for internal calculations)
    equity_curve_tuples = [
        (snapshot.timestamp, snapshot.value) for snapshot in equity_snapshots
    ]

    # Get latest equity and balance values
    latest_equity = equity_snapshots[-1].value
    latest_balance = balance_snapshots[-1].value

    # Calculate P&L
    realised_pnl = latest_balance - starting_balance
    unrealised_pnl = latest_equity - latest_balance
    total_return_pct = (latest_equity - starting_balance) / starting_balance

    # Calculate Sharpe ratio from equity curve
    if len(equity_curve_tuples) >= 2:
        sharpe_ratio = calculate_sharpe_ratio(equity_curve_tuples)
    else:
        sharpe_ratio = 0.0

    # Calculate max drawdown from equity curve
    if len(equity_curve_tuples) >= 2:
        _, max_dd_pct = calculate_max_drawdown(equity_curve_tuples, [])
    else:
        max_dd_pct = 0.0

    # Convert equity curve to EquityCurvePoint objects for API response
    equity_curve = [
        EquityCurvePoint(timestamp=timestamp, value=value)
        for timestamp, value in equity_curve_tuples
    ]

    return PerformanceMetrics(
        realised_pnl=realised_pnl,
        unrealised_pnl=unrealised_pnl,
        sharpe_ratio=sharpe_ratio,
        total_return_pct=total_return_pct,
        max_drawdown=max_dd_pct,
        total_trades=total_trades,
        equity_curve=equity_curve,
    )


async def get_deployment_with_metrics(
    user_id: UUID,
    deployment_id: UUID,
    db_sess: AsyncSession,
) -> tuple[StrategyDeployments, PerformanceMetrics]:
    """
    Get a deployment with its calculated metrics.

    This is a convenience function that retrieves a deployment and calculates
    its metrics in a single call.

    Args:
        user_id: User ID for authorization
        deployment_id: Deployment ID to retrieve
        db_sess: Database session

    Returns:
        Tuple of (deployment, metrics)

    Raises:
        HTTPException: If deployment not found or user not authorized
    """
    deployment = await get_deployment(deployment_id, db_sess)
    if not deployment:
        raise HTTPException(404, "Deployment not found")

    # Verify user owns this deployment
    strategy = await db_sess.scalar(
        select(Strategies).where(Strategies.strategy_id == deployment.strategy_id)
    )
    if not strategy or strategy.user_id != user_id:
        raise HTTPException(404, "Deployment not found")

    broker_conn = await db_sess.get(BrokerConnections, deployment.broker_connection_id)
    if broker_conn is None:
        raise HTTPException(400, "Broker connection couldn't be found")

    metrics = await calculate_deployment_metrics(
        deployment, BrokerType(broker_conn.broker), db_sess
    )

    return deployment, metrics
