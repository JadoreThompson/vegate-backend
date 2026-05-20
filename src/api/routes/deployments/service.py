from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession

from api.models import PaginatedResponse
from api.routes.deployments.exception import (
    DeploymentAlreadyRunningException,
    DeploymentNotFoundException as APIDeploymentNotFoundException,
)
from api.routes.deployments.models import (
    CreateDeploymentRequest,
    StrategyDeploymentMetricsResponse,
    StrategyDeploymentResponse,
    StrategyDeploymentOrderResponse,
)
from api.routes.markets.service import MarketsService
from enums import StrategyDeploymentStatus
from events.deployment import DeploymentEventDeserialiser, DeploymentEventT
from infra.db.model import StrategyDeployments, Strategy, StrategyDeploymentOrders
from infra.db.model.deployment_event import DeploymentEvent
from infra.db.model.instrument import Instrument
from infra.db.model.strategy_deployment_metrics import StrategyDeploymentMetrics
from service.deployment import DeploymentService as IDeploymentService
from service.deployment.exception import DeploymentNotFoundException


class APIDeploymentsService:

    def __init__(
        self, markets_service: MarketsService, deployment_service: IDeploymentService
    ):
        self._markets_service = markets_service
        self._deployment_service = deployment_service

    async def create(
        self, request: CreateDeploymentRequest, db_sess: AsyncSession
    ) -> StrategyDeployments:
        info = await self._markets_service.get_symbol_info(
            request.symbol,
            request.market_type,
            request.broker_type,
            request.timeframe,
            db_sess,
        )

        deployment = StrategyDeployments(
            strategy_id=request.strategy_id,
            instrument_id=info.id,
            timeframe=request.timeframe,
            broker_connection_id=request.broker_connection_id,
        )

        db_sess.add(deployment)
        await db_sess.flush()
        await db_sess.refresh(deployment)

        await self._deployment_service.run(deployment.deployment_id)

        return deployment

    async def start(self, deployment_id: UUID, user_id: UUID, db_sess: AsyncSession):
        deployment = await self._get_user_deployment(deployment_id, user_id, db_sess)
        if deployment.status == StrategyDeploymentStatus.RUNNING:
            raise DeploymentAlreadyRunningException(deployment_id)

        await self._deployment_service.run(deployment_id)

    async def stop(self, deployment_id: UUID, user_id: UUID, db_sess: AsyncSession):
        deployment = await self._get_user_deployment(deployment_id, user_id, db_sess)

        if deployment.status in {
            StrategyDeploymentStatus.STOP_REQUESTED,
            StrategyDeploymentStatus.STOPPED,
        }:
            return
        
        deployment.status = StrategyDeploymentStatus.STOP_REQUESTED

        try:
            await self._deployment_service.stop(deployment_id)
        except DeploymentNotFoundException as e:
            # raise APIDeploymentNotFoundException(e.deployment_id)
            pass

    async def get(self, deployment_id: UUID, user_id: UUID, db_sess: AsyncSession):
        return await self._get_user_deployment(deployment_id, user_id, db_sess)

    async def get_metrics(
        self, deployment_id: UUID, user_id: UUID, db_sess: AsyncSession
    ):
        deployment = await self._get_user_deployment(deployment_id, user_id, db_sess)
        metrics = deployment.metrics

        return self.to_response(deployment, metrics)

    async def get_all(
        self,
        user_id: UUID,
        db_sess: AsyncSession,
        *,
        page: int,
        limit: int,
        status: list[StrategyDeploymentStatus] | None = None,
    ):
        stmt = (
            select(StrategyDeployments, StrategyDeploymentMetrics, Instrument)
            .join(Instrument, Instrument.id == StrategyDeployments.instrument_id)
            .join(Strategy)
            .where(Strategy.user_id == user_id)
        )

        # Apply status filter if provided
        if status is not None:
            stmt = stmt.where(StrategyDeployments.status == status)

        stmt = (
            stmt.offset((page - 1) * limit)
            .limit(limit + 1)
            .order_by(StrategyDeployments.created_at.desc())
        )

        res = await db_sess.execute(stmt)
        rows = res.all()
        return PaginatedResponse[StrategyDeploymentResponse](
            page=page,
            size=min(limit, len(rows)),
            has_next=len(rows) >= limit,
            data=[
                self.to_response(deployment, instrument, metrics)
                for deployment, metrics, instrument in rows[:limit]
            ],
        )

    async def get_by_strategy_id(
        self, strategy_id: UUID, db_sess: AsyncSession, *, page: int, limit: int
    ):
        res = await db_sess.execute(
            select(StrategyDeployments, StrategyDeploymentMetrics, Instrument)
            .join(Instrument, Instrument.id == StrategyDeployments.instrument_id)
            .join(Strategy, StrategyDeployments.strategy_id == Strategy.strategy_id)
            .outerjoin(
                StrategyDeploymentMetrics,
                StrategyDeploymentMetrics.deployment_id
                == StrategyDeployments.deployment_id,
            )
            .where(Strategy.strategy_id == strategy_id)
            .order_by(StrategyDeployments.created_at.desc())
            .offset((page - 1) * limit)
            .limit(limit + 1)
        )

        rows = res.all()

        return PaginatedResponse[StrategyDeploymentResponse](
            page=page,
            size=min(limit, len(rows)),
            has_next=len(rows) >= limit,
            data=[
                self.to_response(deployment, instrument, metrics)
                for deployment, metrics, instrument in rows[:limit]
            ],
        )

    async def get_orders(
        self,
        deployment_id: UUID,
        user_id: UUID,
        db_sess: AsyncSession,
        *,
        page: int,
        limit: int,
    ):
        deployment = await self._get_user_deployment(deployment_id, user_id, db_sess)

        res = await db_sess.scalars(
            select(StrategyDeploymentOrders)
            .where(StrategyDeploymentOrders.deployment_id == deployment_id)
            .offset((page - 1) * limit)
            .limit(limit)
            .order_by(StrategyDeploymentOrders.created_at.asc())
        )

        rows = res.all()

        return PaginatedResponse[StrategyDeploymentOrderResponse](
            page=page,
            size=min(limit, len(rows)),
            has_next=len(rows) >= limit,
            data=[
                StrategyDeploymentOrderResponse(
                    id=order.id,
                    broker_order_id=order.broker_order_id,
                    deployment_id=order.deployment_id,
                    symbol=order.symbol,
                    side=order.side,
                    order_type=order.order_type,
                    quantity=order.quantity,
                    notional=order.notional,
                    filled_quantity=order.filled_quantity,
                    limit_price=order.limit_price,
                    stop_price=order.stop_price,
                    avg_fill_price=order.avg_fill_price,
                    status=order.status,
                    created_at=order.created_at,
                    candle_ts=order.candle_ts,
                )
                for order in rows[:limit]
            ],
        )

    async def get_events(
        self, deployment_id: UUID, db_sess: AsyncSession, *, page: int, limit: int
    ):
        res = await db_sess.execute(
            select(DeploymentEvent)
            .where(DeploymentEvent.deployment_id == deployment_id)
            .offset((page - 1) * limit)
            .limit(limit + 1)
            .order_by(DeploymentEvent.timestamp.desc())
        )
        rows = res.scalars().all()

        deserialiser = DeploymentEventDeserialiser()
        return PaginatedResponse[DeploymentEventT](
            page=page,
            size=min(limit, len(rows)),
            has_next=len(rows) > limit,
            data=[deserialiser.deserialise(item.payload) for item in rows],
        )

    async def _get_user_deployment(
        self, deployment_id: UUID, user_id: UUID, db_sess: AsyncSession
    ) -> StrategyDeployments:
        deployment = await db_sess.scalar(
            select(StrategyDeployments)
            .where(StrategyDeployments.deployment_id == deployment_id)
            .join(Strategy, Strategy.strategy_id == StrategyDeployments.strategy_id)
            .where(Strategy.user_id == user_id)
            .options(selectinload(StrategyDeployments.metrics))
        )

        if deployment is None:
            raise APIDeploymentNotFoundException(deployment_id)

        return deployment

    def to_response(
        self,
        deployment: StrategyDeployments,
        instrument: Instrument,
        metrics: StrategyDeploymentMetrics,
    ):
        return StrategyDeploymentResponse(
            id=deployment.deployment_id,
            strategy_id=deployment.strategy_id,
            broker_connection_id=deployment.broker_connection_id,
            symbol=instrument.symbol,
            timeframe=deployment.timeframe,
            status=StrategyDeploymentStatus(deployment.status),
            error_message=deployment.error_message,
            created_at=deployment.created_at,
            updated_at=deployment.updated_at,
            stopped_at=deployment.stopped_at,
            metrics=(
                None
                if metrics is None
                else StrategyDeploymentMetricsResponse(
                    realised_pnl=metrics.realised_pnl,
                    unrealised_pnl=metrics.unrealised_pnl,
                    profit_factor=metrics.profit_factor,
                    total_return_pct=metrics.total_return_pct,
                    total_orders=metrics.total_orders,
                )
            ),
        )
