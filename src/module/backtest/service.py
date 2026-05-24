import logging
from datetime import date
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from module.api.schema import PaginatedResponse
from module.markets import MarketsService
from module.markets.model import Instrument
from module.strategy import StrategyService
from module.strategy.model import Strategy
from .enums import BacktestStatus
from .exception import BacktestNotFoundException, BacktestInProgressException
from .executor import BacktestExecutor
from .model import Backtest, BacktestMetrics, BacktestOrder
from .schema import (
    CreateBacktestRequest,
    BacktestResponse,
    BacktestMetricsSchema,
    BacktestOrderResponse,
)


class BacktestsService:

    def __init__(
        self,
        strategy_service: StrategyService,
        backtest_executor: BacktestExecutor,
        markets_service: MarketsService,
    ):
        self._strategy_service = strategy_service
        self._backtest_executor = backtest_executor
        self._markets_service = markets_service

        self._logger = logging.getLogger(self.__class__.__name__)

    async def create(
        self, request: CreateBacktestRequest, user_id: UUID, db_sess: AsyncSession
    ) -> Backtest:
        await self._strategy_service.get_user_strategy(
            request.strategy_id, user_id, db_sess
        )

        backtest = Backtest(
            strategy_id=request.strategy_id,
            starting_balance=request.starting_balance,
            start_date=request.start_date,
            end_date=request.end_date,
            status=BacktestStatus.PENDING,
        )
        db_sess.add(backtest)
        await db_sess.flush()
        await db_sess.refresh(backtest)

        await self._backtest_executor.run(backtest.id)

        return backtest

    async def get_backtest(
        self, id: UUID, user_id: UUID, db_sess: AsyncSession
    ) -> BacktestResponse:
        backtest = await self.get_user_backtest(id, user_id, db_sess)
        metrics = await db_sess.scalar(
            select(BacktestMetrics).where(BacktestMetrics.backtest_id == id)
        )
        return self.to_response(backtest, metrics)

    async def get_user_backtest(
        self, id: UUID, user_id: UUID, db_sess: AsyncSession
    ) -> Backtest:
        backtest = await db_sess.scalar(
            select(Backtest)
            .where(Backtest.id == id)
            .join(Strategy, Backtest.strategy_id == Strategy.strategy_id)
            .where(Strategy.user_id == user_id)
        )
        if backtest is None:
            raise BacktestNotFoundException()
        return backtest

    async def get_backtests(
        self,
        user_id: UUID,
        db_sess: AsyncSession,
        *,
        page: int,
        limit: int,
        status: list[BacktestStatus] | None = None,
        symbols: list[str] | None = None,
    ) -> PaginatedResponse[BacktestResponse]:
        stmt = (
            select(Backtest, BacktestMetrics, Instrument)
            # .join(Instrument, Instrument.id == Backtest.instrument_id)
            .outerjoin(BacktestMetrics)
            .join(Strategy, Strategy.strategy_id == Backtest.strategy_id)
            .where(Strategy.user_id == user_id)
            .offset((page - 1) * limit)
            .limit(limit + 1)
            .order_by(Backtest.created_at.desc())
        )

        if status is not None:
            stmt = stmt.where(Backtest.status.in_(status))
        if symbols is not None:
            stmt = stmt.where(Instrument.symbol.in_(symbols))

        res = await db_sess.execute(stmt)

        backtests = [
            # self.to_response(backtest, instrument, metrics)
            # for backtest, metrics, instrument in res.all()
            self.to_response(backtest, metrics)
            for backtest, metrics in res.all()
        ]

        return PaginatedResponse[BacktestResponse](
            page=page,
            size=min(limit, len(backtests)),
            has_next=len(backtests) > limit,
            data=backtests[:limit],
        )

    async def get_by_strategy_id(
        self, strategy_id: UUID, db_sess: AsyncSession, *, page: int, limit: int
    ):
        res = await db_sess.execute(
            # select(Backtest, BacktestMetrics, Instrument)
            select(Backtest, BacktestMetrics)
            .outerjoin(BacktestMetrics)
            # .join(Instrument, Instrument.id == Backtest.instrument_id)
            .where(Backtest.strategy_id == strategy_id)
            .offset((page - 1) * limit)
            .limit(limit + 1)
        )

        rows = res.all()
        return PaginatedResponse[BacktestResponse](
            page=page,
            size=min(limit, len(rows)),
            has_next=len(rows) > limit,
            data=[
                # self.to_response(backtest, instrument, metrics)
                self.to_response(backtest, metrics)
                # for backtest, metrics, instrument in rows[:limit]
                for backtest, metrics in rows[:limit]
            ],
        )

    def to_response(
        self,
        backtest: Backtest,
        # instrument: Instrument,
        metrics: BacktestMetrics | None,
    ) -> BacktestResponse:
        return BacktestResponse(
            id=backtest.id,
            strategy_id=backtest.strategy_id,
            starting_balance=backtest.starting_balance,
            start_date=date(
                year=backtest.start_date.year,
                month=backtest.start_date.month,
                day=backtest.start_date.day,
            ),
            end_date=date(
                year=backtest.end_date.year,
                month=backtest.end_date.month,
                day=backtest.end_date.day,
            ),
            status=BacktestStatus(backtest.status),
            created_at=backtest.created_at,
            metrics=(
                None
                if metrics is None
                else BacktestMetricsSchema(
                    realised_pnl=metrics.realised_pnl,
                    unrealised_pnl=metrics.unrealised_pnl,
                    total_return_pct=metrics.total_return_pct,
                    profit_factor=metrics.profit_factor,
                    total_orders=metrics.total_orders,
                    equity_curve=metrics.equity_curve,
                )
            ),
        )

    async def delete(self, id: UUID, user_id: UUID, db_sess: AsyncSession):
        backtest = await self.get_user_backtest(id, user_id, db_sess)
        if backtest.status == BacktestStatus.IN_PROGRESS:
            raise BacktestInProgressException()

        await db_sess.delete(backtest)

    async def get_orders(
        self, id: UUID, user_id: UUID, db_sess: AsyncSession, *, page: int, limit: int
    ):
        res = await db_sess.execute(
            select(BacktestOrder)
            .join(Backtest)
            # .join(Strategy)
            .join(Strategy, Strategy.strategy_id == Backtest.strategy_id)
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

        return PaginatedResponse[BacktestOrderResponse](
            page=page,
            size=min(limit, len(backtests)),
            has_next=len(backtests) > limit,
            data=backtests[:limit],
        )
