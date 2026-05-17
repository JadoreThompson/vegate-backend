from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.models import PaginatedResponse
from api.routes.deployments.exception import DeploymentNotFoundException
from api.routes.deployments.models import CreateDeploymentRequest, StrategyDeploymentMetricsResponse, \
    StrategyDeploymentResponse, StrategyDeploymentOrderResponse
from api.routes.markets.service import MarketsService
from enums import StrategyDeploymentStatus
from infra.db.model import StrategyDeployments, Strategy, StrategyDeploymentOrders
from infra.db.model.strategy_deployment_metrics import StrategyDeploymentMetrics
from service.deployment import DeploymentService as IDeploymentService


class DeploymentService:

    def __init__(self, markets_service: MarketsService, deployment_service: IDeploymentService):
        self._markets_service = markets_service
        self._deployment_service = deployment_service

    async def create(self, request: CreateDeploymentRequest, db_sess: AsyncSession) -> StrategyDeployments:
        info = await self._markets_service.get_symbol_info(request.symbol, request.market_type, request.broker_type,
                                                           request.timeframe, db_sess)

        deployment = StrategyDeployments(
            strategy_id=request.strategy_id,
            symbol=request.symbol,
            broker=request.broker_type,
            timeframe=request.timeframe,
            market_type=request.market_type,
            broker_connection_id=request.broker_connection_id
        )

        db_sess.add(deployment)
        await db_sess.flush()
        await db_sess.refresh(deployment)

        return deployment

    async def stop(self, deployment_id: UUID, user_id: UUID, db_sess: AsyncSession):
        deployment = await self._get_user_deployment(deployment_id, user_id, db_sess)

        if deployment.status in {StrategyDeploymentStatus.STOP_REQUESTED, StrategyDeploymentStatus.STOPPED}:
            return

        await self._deployment_service.stop(deployment_id)

    async def get(self, deployment_id: UUID, user_id: UUID, db_sess: AsyncSession):
        return await self._get_user_deployment(deployment_id, user_id, db_sess)

    async def get_metrics(self, deployment_id: UUID, user_id: UUID, db_sess: AsyncSession):
        deployment = await self._get_user_deployment(deployment_id, user_id, db_sess)
        metrics = deployment.metrics

        return self.to_response(deployment, metrics)

    async def get_all(self, user_id: UUID, db_sess: AsyncSession, *, page: int, limit: int,
                      status: list[StrategyDeploymentStatus] | None = None):
        stmt = (
            select(StrategyDeployments, StrategyDeploymentMetrics).join(Strategy).where(Strategy.user_id == user_id)
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
            data=[self.to_response(deployment, metrics) for deployment, metrics in rows],
        )

    async def get_orders(self, deployment_id: UUID, user_id: UUID, db_sess: AsyncSession, *, page: int, limit: int):
        deployment = await self._get_user_deployment(deployment_id, user_id, db_sess)

        res = await db_sess.scalars(
            select(StrategyDeploymentOrders)
            .where(StrategyDeploymentOrders.deployment_id == deployment_id)
            .offset((page - 1) * limit)
            .limit(limit)
            .order_by(StrategyDeploymentOrders.created_at.asc())
        )

        rows = res.all()

        return PaginatedResponse[StrategyDeploymentResponse](
            page=page,
            size=min(limit, len(rows)),
            has_next=len(rows) >= limit,
            data=[
                StrategyDeploymentOrderResponse(
                    id=order.id,
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
                    candle_ts=order.candle_ts
                )
                for order in rows
            ]
        )

    async def _get_user_deployment(self, deployment_id: UUID, user_id: UUID,
                                   db_sess: AsyncSession) -> StrategyDeployments:
        deployment = await db_sess.scalar(
            select(StrategyDeployments).where(StrategyDeployments.deployment_id == deployment_id).join(Strategy).where(
                Strategy.user_id == user_id))

        if deployment is None:
            raise DeploymentNotFoundException(deployment_id)

        return deployment

    def to_response(self, deployment: StrategyDeployments, metrics: StrategyDeploymentMetrics):
        return StrategyDeploymentResponse(
            id=deployment.deployment_id,
            strategy_id=deployment.strategy_id,
            broker_connection_id=deployment.broker_connection_id,
            symbol=deployment.symbol,
            timeframe=deployment.timeframe,
            status=StrategyDeploymentStatus(deployment.status),
            error_message=deployment.error_message,
            created_at=deployment.created_at,
            updated_at=deployment.updated_at,
            stopped_at=deployment.stopped_at,
            metrics=None if metrics is None else StrategyDeploymentMetricsResponse(
                realised_pnl=metrics.realised_pnl,
                unrealised_pnl=metrics.unrealised_pnl,
                profit_factor=metrics.profit_factor,
                total_return_pct=metrics.total_return_pct,
                total_orders=metrics.total_orders,
            )
        )
