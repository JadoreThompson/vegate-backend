import logging
from datetime import datetime, timedelta
from typing import NamedTuple
from uuid import UUID

from fastapi import HTTPException
from sqlalchemy import func, select, text
from sqlalchemy.ext.asyncio import AsyncSession

from api.shared.models import PerformanceMetrics
from config import REDIS_DEPLOYMENT_EVENTS_KEY
from core.enums import DeploymentEventType, StrategyDeploymentStatus
from core.events import DeploymentEvent
from db_models import BrokerConnections, Orders, Strategies, StrategyDeployments, Ticks
from engine.backtesting.metrics import (
    calculate_sharpe_ratio,
    calculate_max_drawdown,
    calculate_total_return,
)
from engine.backtesting.types import EquityCurve
from engine.enums import BrokerType, OrderSide, OrderStatus, Timeframe
from services import PriceService
from utils.redis import REDIS_CLIENT
from utils.utils import get_datetime
from .models import DeployStrategyRequest


logger = logging.getLogger("deployments.controller")


class Point(NamedTuple):
    timestamp: datetime
    quantity: float


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
        market_type=data.market_type,
        symbol=data.symbol,
        timeframe=data.timeframe,
        status=StrategyDeploymentStatus.PENDING,
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
    status: StrategyDeploymentStatus | None = None,
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

    if deployment.status == StrategyDeploymentStatus.STOP_REQUESTED:
        raise HTTPException(400, "Deployment has already been requested to stop")

    if deployment.status == StrategyDeploymentStatus.STOPPED:
        raise HTTPException(400, "Deployment is already stopped")

    if deployment.status == StrategyDeploymentStatus.ERROR:
        raise HTTPException(400, "Cannot stop a deployment in ERROR state")

    deployment.status = StrategyDeploymentStatus.STOP_REQUESTED

    await db_sess.flush()
    await db_sess.refresh(deployment)

    event = DeploymentEvent(type=DeploymentEventType.STOP, deployment_id=deployment_id)
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
) -> EquityCurve:
    result = await db_sess.execute(
        select(Orders)
        .where(
            Orders.deployment_id == deployment.deployment_id,
            Orders.status.in_((OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED)),
        )
        .order_by(Orders.filled_at.asc())
    )
    orders = list(result.scalars().all())

    if not orders or not deployment.starting_balance:
        now = get_datetime()
        starting_balance = deployment.starting_balance or 0
        return [
            (now - timedelta(days=7), starting_balance),
            (now - timedelta(days=6), starting_balance),
            (now - timedelta(days=5), starting_balance),
            (now - timedelta(days=3), starting_balance),
            (now - timedelta(days=1), starting_balance),
            (now, starting_balance),
        ]

    now = get_datetime()
    week_ago = now - timedelta(days=7)
    timestamps = [
        week_ago,
        week_ago + timedelta(days=1.4),  # ~1.4 days
        week_ago + timedelta(days=2.8),  # ~2.8 days
        week_ago + timedelta(days=4.7),  # ~4.7 days
        week_ago + timedelta(days=6),  # ~6 days
        now,
    ]

    equity_graph: EquityCurve = []
    starting_balance = deployment.starting_balance

    for timestamp in timestamps:
        # Calculate equity at this timestamp
        cash = starting_balance
        positions: dict[str, dict] = {}  # symbol -> {quantity, avg_price}
        realised_pnl = 0

        # Process all orders up to this timestamp
        for order in orders:
            if order.filled_at > timestamp:
                break

            symbol = order.symbol
            side = order.side
            quantity = order.filled_quantity
            price = order.avg_fill_price

            if not price or not quantity:
                continue

            # Initialize position if it doesn't exist
            if symbol not in positions:
                positions[symbol] = {"quantity": 0, "avg_price": 0}

            position = positions[symbol]

            if side == OrderSide.BUY:
                cash -= quantity * price

                total_cost = position["quantity"] * position["avg_price"]
                new_cost = quantity * price
                new_quantity = position["quantity"] + quantity

                if new_quantity > 0:
                    position["avg_price"] = (total_cost + new_cost) / new_quantity
                position["quantity"] = new_quantity

            elif side == OrderSide.SELL:
                cash += quantity * price

                if position["quantity"] > 0:
                    sold_quantity = min(quantity, position["quantity"])
                    pnl = (price - position["avg_price"]) * sold_quantity
                    realised_pnl += pnl
                    position["quantity"] -= sold_quantity

                    if position["quantity"] == 0:
                        position["avg_price"] = 0

        # Calculate unrealised PnL using tick data at this timestamp
        unrealised_pnl = 0
        for symbol, position in positions.items():
            if position["quantity"] > 0:
                # Get price from ticks table at this timestamp
                timestamp_ms = int(timestamp.timestamp() * 1000)

                # Find the closest tick at or before this timestamp
                tick_result = await db_sess.execute(
                    select(Ticks)
                    .where(
                        Ticks.source == broker,
                        Ticks.symbol == symbol,
                        Ticks.market_type == deployment.market_type,
                        Ticks.timestamp <= timestamp_ms,
                    )
                    .order_by(Ticks.timestamp.desc())
                    .limit(1)
                )
                tick = tick_result.scalar_one_or_none()

                if tick:
                    market_price = tick.price
                else:
                    # Fallback to most recent order price if no tick data
                    market_price = position["avg_price"]
                    for order in reversed(orders):
                        if (
                            order.symbol == symbol
                            and order.avg_fill_price
                            and order.filled_at <= timestamp
                        ):
                            market_price = order.avg_fill_price
                            break

                unrealised_pnl += (market_price - position["avg_price"]) * position[
                    "quantity"
                ]

        # Calculate total equity
        equity = cash + unrealised_pnl
        equity_graph.append((timestamp, equity))

    return equity_graph


async def calculate_deployment_metrics(
    deployment: StrategyDeployments,
    broker: BrokerType,
    db_sess: AsyncSession,
) -> PerformanceMetrics:
    """
    Calculate performance metrics for a deployment from its orders.

    This function computes real-time metrics by analyzing all filled orders
    and calculating:
    - Realised PnL from closed positions
    - Unrealised PnL from open positions
    - Total return percentage
    - Sharpe ratio from equity curve
    - Maximum drawdown
    - Total trade count
    - Equity curve over time

    Args:
        deployment: The deployment to calculate metrics for
        db_sess: Database session

    Returns:
        PerformanceMetrics object with calculated values
    """
    # Get all filled and partially filled orders for this deployment, ordered by fill time
    trade_count = await db_sess.scalar(
        select(
            func.count(Orders).where(Orders.deployment_id == deployment.deployment_id)
        )
    )
    result = await db_sess.execute(
        select(Orders)
        .where(
            Orders.deployment_id == deployment.deployment_id,
            Orders.status.in_((OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED)),
        )
        .order_by(Orders.filled_at.asc())
    )
    orders = list(result.scalars().all())

    cash = deployment.starting_balance or 0
    default_metrics = PerformanceMetrics(
        realised_pnl=0.0,
        unrealised_pnl=0.0,
        total_return_pct=0.0,
        sharpe_ratio=0.0,
        max_drawdown=0.0,
        total_trades=0,
    )

    if not cash:
        return default_metrics

    if not orders:
        return default_metrics

    # Track positions and PnL
    asset_balances = {}
    positions: dict[str, dict] = {}
    equity_curve: EquityCurve = []
    realised_pnl = 0

    # Add initial point to equity curve (starting cash with no positions)
    if orders:
        equity_curve.append((orders[0].filled_at, cash))

    # Process each order to build equity curve and calculate PnL
    for order in orders:
        symbol = order.symbol
        side = order.side
        quantity = order.filled_quantity
        price = order.avg_fill_price

        if not price or not quantity:
            continue

        # Initialize position if it doesn't exist
        if symbol not in positions:
            cur_price = await PriceService.get_price(broker, symbol)
            if cur_price is None:
                continue
            positions[symbol] = {
                "quantity": 0,
                "avg_price": 0,
                "cur_price": cur_price,
                "unfilled_qty": 0.0,
            }

        position = positions[symbol]

        if side == OrderSide.BUY:
            cash -= quantity * price

            if order.status == OrderStatus.FILLED:
                total_cost = position["quantity"] * position["avg_price"]
                new_cost = quantity * price
                new_quantity = position["quantity"] + quantity

                if new_quantity > 0:
                    position["avg_price"] = (total_cost + new_cost) / new_quantity
                position["quantity"] = new_quantity
            else:
                position["unfilled_qty"] += order.quantity - order.filled_quantity

        elif side == OrderSide.SELL:
            # Selling: increase cash, decrease position
            cash += quantity * price

            # Calculate realised PnL for this trade
            if order.status == OrderStatus.FILLED:
                if position["quantity"] > 0:
                    sold_quantity = min(quantity, position["quantity"])
                    pnl = (price - position["avg_price"]) * sold_quantity
                    realised_pnl += pnl

                    # Update position
                    position["quantity"] -= sold_quantity

                    # If position fully closed, count as completed trade
                    if position["quantity"] == 0:
                        trades_count += 1
                        position["avg_price"] = 0
            else:
                position["unfilled_qty"] -= quantity

        # Calculate unrealised PnL from current open positions at this point in time
        unrealised_pnl_at_order = 0
        for sym, pos in positions.items():
            if pos["quantity"] > 0:
                # Use the most recent price for this symbol up to this order
                latest_price = price if sym == symbol else pos["avg_price"]
                # Search backwards through orders up to current index
                for o in reversed(orders[: orders.index(order) + 1]):
                    if o.symbol == sym and o.avg_fill_price:
                        latest_price = o.avg_fill_price
                        break
                unrealised_pnl_at_order += (latest_price - pos["avg_price"]) * pos[
                    "quantity"
                ]

        # Equity = Cash + Unrealised PnL (market value of positions)
        current_equity = cash + unrealised_pnl_at_order
        equity_curve.append((order.filled_at, current_equity))

    # Calculate final unrealised PnL from open positions using latest prices
    # unrealised_pnl = 0
    # for symbol, position in positions.items():
    #     if position["quantity"] > 0:
    #         # Find the most recent order for this symbol to get current market price
    #         last_order = next(
    #             (
    #                 o
    #                 for o in reversed(orders)
    #                 if o.symbol == symbol and o.avg_fill_price
    #             ),
    #             None,
    #         )
    #         if last_order and last_order.avg_fill_price:
    #             unrealised_pnl += (
    #                 last_order.avg_fill_price - position["avg_price"]
    #             ) * position["quantity"]
    unrealised_pnl = sum(
        position["unfilled_qty"] * position["cur_price"]
        for _, position in positions.items()
    )

    # Final equity = Cash + Unrealised PnL
    final_equity = cash + unrealised_pnl

    # Calculate metrics
    starting_balance = float(deployment.starting_balance or 0)
    _, total_return_pct = calculate_total_return(starting_balance, float(final_equity))

    # Calculate Sharpe ratio if we have enough data points
    sharpe_ratio = 0.0
    if len(equity_curve) >= 2:
        sharpe_ratio = calculate_sharpe_ratio(equity_curve)

    # Calculate max drawdown
    max_dd_dollars, max_dd_pct = calculate_max_drawdown(equity_curve, [])

    # Build weekly equity graph with 6 points
    weekly_equity_graph = await build_weekly_equity_graph(deployment, broker, db_sess)

    return PerformanceMetrics(
        realised_pnl=realised_pnl,
        unrealised_pnl=unrealised_pnl,
        total_return_pct=total_return_pct,
        sharpe_ratio=sharpe_ratio,
        max_drawdown=max_dd_pct,
        total_trades=len(orders),
        equity_curve=weekly_equity_graph,
    )


################################################################################


async def get_price_points(
    source: BrokerType,
    symbol: str,
    timeframe: Timeframe,
    start_date: datetime,
    end_date: datetime,
    db_sess: AsyncSession,
    n: int = 5,
) -> tuple[int, float]:
    # query = """
    # WITH time_points AS (
    #     SELECT
    #         generate_series(
    #             :start_date,
    #             :end_date,
    #             (:end_date - :start_date) / :n
    #         ) AS target_time
    # ),
    # nearest_candles AS (
    #     SELECT DISTINCT ON (tp.target_time)
    #         tp.target_time,
    #         candle.close,
    #         candle.timestamp,
    #         ABS(candle.timestamp - tp.target_time) AS time_diff
    #     FROM time_points tp
    #     CROSS JOIN ohlc_levels candle
    #     WHERE 
    #         candle.timestamp BETWEEN :start_date AND :end_date 
    #         AND source = :source
    #         AND symbol = :symbol
    #         AND timeframe = :timeframe
    #     ORDER BY tp.target_time, ABS(candle.timestamp - tp.target_time)
    # )
    # SELECT 
    #     target_time,
    #     close
    # FROM nearest_candles
    # ORDER BY target_time;
    # """
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

    res = await db_sess.execute(
        text(query),
        # {
        #     "source": source.value,
        #     "symbol": symbol,
        #     "timeframe": timeframe.value,
        #     "start_date": int(start_date.timestamp()),
        #     "end_date": int(end_date.timestamp()),
        #     "n": n,
        # },
    )

    return list(res.all())


async def calculate_deployment_metrics_new(
    deployment: StrategyDeployments,
    broker: BrokerType,
    db_sess: AsyncSession,
) -> PerformanceMetrics:
    total_trades = await db_sess.scalar(
        select(func.count()).where(Orders.deployment_id == deployment.deployment_id)
    )
    result = await db_sess.execute(
        select(Orders)
        .where(
            Orders.deployment_id == deployment.deployment_id,
            Orders.status.in_((OrderStatus.FILLED, OrderStatus.PARTIALLY_FILLED)),
        )
        .order_by(Orders.submitted_at.asc())
    )
    orders = result.scalars().all()

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

    if not orders:
        return default_metrics
    cur_price = await PriceService.get_price(broker, deployment.symbol)
    if not cur_price:
        logger.debug(f"Failed to find price for {deployment.symbol}")
        return default_metrics

    asset_points: list[Point] = []
    asset_avg_price = 0.0
    cur_balance = starting_balance
    start_date: datetime = None
    end_date: datetime = None
    equity_curve = []

    for order in orders:
        filled_quantity = order.filled_quantity
        filled_price = order.avg_fill_price
        order_value = filled_quantity * filled_price        

        # Asset Balances
        if not asset_points:
            if order.side == OrderSide.BUY:
                filled_qty = filled_quantity
                cur_balance -= order_value
                # Update average price for first purchase
                # asset_avg_prices[symbol] = filled_price
                asset_avg_price = filled_price
            else:
                continue
        else:
            prev_point = asset_points[-1]

            if order.side == OrderSide.BUY:
                # Update average price using weighted average
                total_cost = (
                    # prev_point.quantity * asset_avg_prices[symbol] + order_value
                    prev_point.quantity * asset_avg_price + order_value
                )
                filled_qty = prev_point.quantity + filled_quantity
                if filled_qty > 0:
                    # asset_avg_prices[symbol] = total_cost / filled_qty
                    asset_avg_price = total_cost / filled_qty
                cur_balance -= order_value
            else:
                filled_qty = prev_point.quantity - filled_quantity
                cur_balance += order_value

        # Dates
        if start_date is None or order.submitted_at < start_date:
            start_date = order.submitted_at
        if end_date is None or order.filled_at > end_date:
            end_date = order.filled_at

        asset_points.append(
            Point(
                timestamp=order.filled_at,
                quantity=filled_qty,
            )
        )

    # Equity Curve

    time_diff = (end_date - start_date).total_seconds()

    if time_diff < Timeframe.H1.get_seconds():
        tf = Timeframe.m1
    elif time_diff < Timeframe.D1.get_seconds():
        tf = Timeframe.m5
    elif time_diff < Timeframe.W1.get_seconds():
        tf = Timeframe.H1
    elif time_diff < Timeframe.m1.get_seconds():
        tf = Timeframe.H4
    else:
        tf = Timeframe.D1

    n_points = 5
    price_points = await get_price_points(
        broker, deployment.symbol, tf, start_date, end_date, db_sess, n=n_points
    )
    equity_curve = []
    
    if price_points:
        current_point_t, current_point_price = price_points[0]
        price_points_idx = 0
        equity_curve = [(current_point_t, starting_balance)]
        cur_quantity = None
        equity_curve_idx = 0
        
        for point in asset_points:
            pts = int(point.timestamp.timestamp())

            while pts >= current_point_t:
                if cur_quantity is None:
                    continue

                price_points_idx += 1
                equity_curve_idx += 1
                current_point_t, current_point_price = price_points[price_points_idx]
                equity_curve.append((current_point_t, cur_quantity * current_point_price))

            cur_quantity = point.quantity
            equity_curve[equity_curve_idx] = (current_point_t, cur_quantity * current_point_price)

        for i in range(len(equity_curve)):
            if equity_curve[i] is None:
                t, price = price_points[i]
                equity_curve[i] = (t, price * cur_quantity)        
    else:
        equity_curve = []

    unrealised_pnl = 0.0
    avg_price = asset_avg_price
    unrealised_pnl += (cur_price - avg_price) * asset_points[-1].quantity

    # Calculate max drawdown
    if equity_curve:
        sharpe_ratio = calculate_sharpe_ratio(equity_curve)
        _, max_dd_pct = calculate_max_drawdown(equity_curve, [])
    else:
        sharpe_ratio = 0.0
        max_dd_pct = 0.0


    return PerformanceMetrics(
        realised_pnl=cur_balance - starting_balance,
        unrealised_pnl=unrealised_pnl,
        sharpe_ratio=sharpe_ratio,
        total_return_pct=(cur_balance + unrealised_pnl) / starting_balance - 1,
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

    metrics = await calculate_deployment_metrics_new(
        deployment, BrokerType(broker_conn.broker), db_sess
    )

    return deployment, metrics
