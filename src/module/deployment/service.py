from typing import TYPE_CHECKING
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.orm import selectinload
from sqlalchemy.ext.asyncio import AsyncSession

from module.api.schema import PaginatedResponse
from module.broker_connections import BrokerConnectionsService
from module.event_bus import EventPublisher
from module.markets import MarketsService

from .enums import StrategyDeploymentStatus
from .event import (
    DeploymentEventUnion,
    DeploymentRequestedEvent,
    DeploymentStopRequestedEvent,
)
from .event.deserialiser import DeploymentEventDeserialiser
from .exception import DeploymentAlreadyRunningException, DeploymentNotFoundException
from .model import (
    StrategyDeployments,
    StrategyDeploymentMetrics,
    StrategyDeploymentOrders,
    DeploymentEvent,
)
from .schema import (
    CreateDeploymentRequest,
    StrategyDeploymentMetricsResponse,
    StrategyDeploymentResponse,
    StrategyDeploymentOrderResponse,
)

if TYPE_CHECKING:
    from module.strategy import StrategyService


class DeploymentsService:

    def __init__(
        self,
        strategy_service: "StrategyService",
        markets_service: MarketsService,
        broker_connections_service: BrokerConnectionsService,
        event_publisher: EventPublisher,
    ):
        self._strategy_service = strategy_service
        self._markets_service = markets_service
        self._broker_connections_service = broker_connections_service
        self._event_publisher = event_publisher

    async def create(
        self, request: CreateDeploymentRequest, user_id: UUID, db_sess: AsyncSession
    ) -> StrategyDeployments:
        version = await self._strategy_service.get_version_by_id(
            request.version_id, db_sess
        )
        deployment = StrategyDeployments(
            user_id=user_id,
            strategy_id=version.strategy_id,
            version_id=request.version_id,
            broker_connection_id=request.broker_connection_id,
        )

        db_sess.add(deployment)
        await db_sess.flush()
        await db_sess.refresh(deployment)

        await self._event_publisher.publish(
            DeploymentRequestedEvent(deployment_id=deployment.id), db_sess
        )

        return deployment

    async def start(self, deployment_id: UUID, user_id: UUID, db_sess: AsyncSession):
        deployment = await self._get_user_deployment(deployment_id, user_id, db_sess)
        if deployment.status not in {
            StrategyDeploymentStatus.PENDING,
            StrategyDeploymentStatus.STOPPED,
        }:
            raise DeploymentAlreadyRunningException(deployment_id)

        await self._event_publisher.publish(
            DeploymentRequestedEvent(deployment_id=deployment.id), db_sess
        )

    async def stop(self, deployment_id: UUID, user_id: UUID, db_sess: AsyncSession):
        deployment = await self._get_user_deployment(deployment_id, user_id, db_sess)

        if deployment.status in {
            StrategyDeploymentStatus.STOP_REQUESTED,
            StrategyDeploymentStatus.STOPPED,
        }:
            return

        await self._event_publisher.publish(
            DeploymentStopRequestedEvent(deployment_id=deployment_id), db_sess
        )

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
            select(StrategyDeployments, StrategyDeploymentMetrics)
            .outerjoin(StrategyDeploymentMetrics)
            .where(StrategyDeployments.user_id == user_id)
        )

        # Apply status filter if provided
        if status is not None:
            stmt = stmt.where(StrategyDeployments.status.in_(status))

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
                self.to_response(deployment, metrics)
                for deployment, metrics in rows[:limit]
            ],
        )

    async def get_by_strategy_id(
        self,
        strategy_id: UUID,
        db_sess: AsyncSession,
        *,
        page: int,
        limit: int,
        status: list[StrategyDeploymentStatus] | None = None,
    ):
        stmt = (
            select(StrategyDeployments, StrategyDeploymentMetrics)
            .outerjoin(
                StrategyDeploymentMetrics,
                StrategyDeploymentMetrics.deployment_id == StrategyDeployments.id,
            )
            .where(StrategyDeployments.strategy_id == strategy_id)
            .order_by(StrategyDeployments.created_at.desc())
            .offset((page - 1) * limit)
            .limit(limit + 1)
        )

        if status is not None:
            stmt = stmt.where(StrategyDeployments.status.in_(status))

        res = await db_sess.execute(stmt)

        rows = res.all()

        return PaginatedResponse[StrategyDeploymentResponse](
            page=page,
            size=min(limit, len(rows)),
            has_next=len(rows) >= limit,
            data=[
                self.to_response(deployment, metrics)
                for deployment, metrics in rows[:limit]
            ],
        )

    async def get_by_version_id(
        self, version_id: UUID, db_sess: AsyncSession, *, page: int, limit: int
    ):
        res = await db_sess.execute(
            select(StrategyDeployments, StrategyDeploymentMetrics)
            .outerjoin(
                StrategyDeploymentMetrics,
                StrategyDeploymentMetrics.deployment_id == StrategyDeployments.id,
            )
            .where(StrategyDeployments.version_id == version_id)
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
                self.to_response(deployment, metrics)
                for deployment, metrics in rows[:limit]
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
            .order_by(StrategyDeploymentOrders.created_at.desc())
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
                )
                for order in rows[:limit]
            ],
        )

    async def get_events(
        self,
        deployment_id: UUID,
        user_id,
        db_sess: AsyncSession,
        *,
        page: int,
        limit: int,
    ):
        deployment = await self._get_user_deployment(deployment_id, user_id, db_sess)

        res = await db_sess.execute(
            select(DeploymentEvent)
            .where(DeploymentEvent.deployment_id == deployment_id)
            .offset((page - 1) * limit)
            .limit(limit + 1)
            .order_by(DeploymentEvent.timestamp.desc())
        )
        rows = res.scalars().all()

        deserialiser = DeploymentEventDeserialiser()
        return PaginatedResponse[DeploymentEventUnion](
            page=page,
            size=min(limit, len(rows)),
            has_next=len(rows) > limit,
            data=[deserialiser.deserialise(item.payload) for item in rows[:limit]],
        )

    async def _get_user_deployment(
        self, deployment_id: UUID, user_id: UUID, db_sess: AsyncSession
    ) -> StrategyDeployments:
        deployment = await db_sess.scalar(
            select(StrategyDeployments)
            .where(StrategyDeployments.id == deployment_id)
            .where(StrategyDeployments.user_id == user_id)
            .options(selectinload(StrategyDeployments.metrics))
        )

        if deployment is None:
            raise DeploymentNotFoundException(deployment_id)

        return deployment

    def to_response(
        self,
        deployment: StrategyDeployments,
        metrics: StrategyDeploymentMetrics | None = None,
    ):
        return StrategyDeploymentResponse(
            id=deployment.id,
            version_id=deployment.version_id,
            broker_connection_id=deployment.broker_connection_id,
            status=StrategyDeploymentStatus(deployment.status),
            error_message=deployment.error_message,
            created_at=deployment.created_at,
            updated_at=deployment.updated_at,
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
