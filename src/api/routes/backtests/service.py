from uuid import UUID

from mistralai.client.models import PaginationResponse
from sqlalchemy import select, exists, and_, delete
from sqlalchemy.ext.asyncio import AsyncSession

from api.routes.backtests.exception import SymbolNotFoundException, BacktestNotFoundException, \
    BacktestMetricsNotFoundException
from api.routes.backtests.model import CreateBacktestRequest, BacktestResponse, BacktestMetricsResponse, \
    BacktestOrderResponse
from api.routes.strategy.service import StrategyService
from enums import BacktestStatus
from infra.db.model import Backtest, OHLC, Strategy, BacktestMetrics, BacktestOrder
from service.backtest import BacktestService as IBacktestService


class BacktestService:

    def __init__(self, strategy_service: StrategyService, backtest_service: IBacktestService):
        self._strategy_service = strategy_service
        self._backtest_service = backtest_service

    async def create(self, request: CreateBacktestRequest, user_id: UUID, db_sess: AsyncSession) -> Backtest:
        await self._strategy_service.get_user_strategy(request.strategy_id, user_id, db_sess)
        res = await db_sess.execute(select(exists()).where(and_(OHLC.symbol == request.symbol, OHLC.source == request.broker, OHLC.market_type == request.market_type, OHLC.timeframe == request.timeframe)))
        if not res.first():
            raise SymbolNotFoundException(request.symbol)

        backtest = Backtest(
            strategy_id=request.strategy_id,
            symbol=request.symbol,
            broker=request.broker,
            starting_balance=request.starting_balance,
            start_date=request.start_date,
            end_date=request.end_date,
            timeframe=request.timeframe,
            market_type=request.market_type,
            status=BacktestStatus.IN_PROGRESS
        )
        db_sess.add(backtest)
        await db_sess.flush()
        await db_sess.refresh(backtest)

        await self._backtest_service.run_backtest(backtest.id)

        return backtest

    async def get_backtest(self, id: UUID, user_id: UUID, db_sess: AsyncSession) -> BacktestResponse:
        backtest = await self.get_user_backtest(id, user_id, db_sess)
        if backtest.status != BacktestStatus.COMPLETED:
            raise BacktestMetricsNotFoundException("Backtest is not complete. Please try again later")

        metrics: BacktestMetrics = await db_sess.scalar(select(BacktestMetrics).where(BacktestMetrics.backtest_id == id))
        if metrics is None:
            raise BacktestMetricsNotFoundException("Metrics not found. Please try again later")

        return BacktestResponse(
            id=id,
            strategy_id=backtest.strategy_id,
            symbol=backtest.symbol,
            broker=backtest.broker,
            market_type=backtest.market_type,
            starting_balance=backtest.starting_balance,
            start_date=backtest.start_date,
            end_date=backtest.end_date,
            status=backtest.status,
            created_at=backtest.created_at,
            metrics=BacktestMetricsResponse(
                realised_pnl=metrics.realised_pnl,
                unrealised_pnl=metrics.unrealised_pnl,
                total_return_pct=metrics.total_return_pct,
                profit_factor=metrics.profit_factor,
                total_orders=metrics.total_orders,
            )
        )

    async def get_user_backtest(self, id: UUID, user_id: UUID, db_sess: AsyncSession) -> Backtest:
        backtest = await db_sess.scalar(select(Backtest).where(Backtest.id == id).join(Strategy, Backtest.strategy_id == Strategy.strategy_id).where(Strategy.user_id == user_id))
        if backtest is None:
            raise BacktestNotFoundException()
        return backtest

    async def get_backtests(self, user_id: UUID, db_sess: AsyncSession, *, page: int, limit: int, status: list[BacktestStatus] | None = None, symbols: list[str] | None = None) -> PaginationResponse[BacktestResponse]:
        res = await db_sess.execute(
            select(Backtest, BacktestMetrics)
            .join(Strategy)
            .join(BacktestMetrics)
            .where(Strategy.user_id == user_id)
            .offset((page - 1) * limit)
            .limit(limit + 1)
            .order_by(Backtest.created_at.desc())
        )
        backtests = [
            BacktestResponse(
                id=backtest.id,
                strategy_id=backtest.strategy_id,
                symbol=backtest.symbol,
                broker=backtest.broker,
                market_type=backtest.market_type,
                starting_balance=backtest.starting_balance,
                start_date=backtest.start_date,
                end_date=backtest.end_date,
                status=backtest.status,
                created_at=backtest.created_at,
                metrics=BacktestMetricsResponse(
                    realised_pnl=metrics.realised_pnl,
                    unrealised_pnl=metrics.unrealised_pnl,
                    total_return_pct=metrics.total_return_pct,
                    profit_factor=metrics.profit_factor,
                    total_orders=metrics.total_orders,
                )
            )
            for backtest, metrics in res.scalars().all()
        ]

        return PaginationResponse[BacktestResponse](
            page=page,
            size=min(limit, len(backtests)),
            has_next=len(backtests) > limit,
            data=backtests[:limit]
        )

    async def delete(self, id: UUID, user_id: UUID, db_sess: AsyncSession):
        backtest = await self.get_user_backtest(id, user_id, db_sess)
        if backtest.status == BacktestStatus.IN_PROGRESS:
            raise ValueError("Backtest is currently in progress")

        await db_sess.delete(backtest)

    async def get_orders(self, id: UUID, user_id: UUID, db_sess: AsyncSession, *, page: int, limit: int):
        res = await db_sess.execute(
            select(BacktestOrder)
            .join(Backtest)
            .join(Strategy)
            .where(BacktestOrder.backtest_id == id)
            .where(Strategy.user_id == user_id)
            .offset((page - 1) * limit)
            .limit(limit)
            .order_by(BacktestOrder.submitted_at.asc())
        )

        backtests = [
            BacktestOrderResponse(
                id=bt_order.id,
                backtest_id=bt_order.backtest_id,
                symbol=bt_order.symbol,
                side=bt_order.side,
                order_type=bt_order.order_type,
                quantity=bt_order.quantity,
                notional=bt_order.notional,
                filled_quantity=bt_order.filled_quantity,
                limit_price=bt_order.limit_price,
                stop_price=bt_order.stop_price,
                avg_fill_price=bt_order.avg_fill_price,
                status=bt_order.status,
                submitted_at=bt_order.submitted_at,
                filled_at=bt_order.filled_at,
                details=bt_order.details,
            )
            for bt_order in res.scalars().all()
        ]

        return PaginationResponse[BacktestOrderResponse](
            page=page,
            size=min(limit, len(backtests)),
            has_next=len(backtests) > limit,
            data=backtests[:limit]
        )
