from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import depends_db_sess, depends_deployment_service, depends_jwt, CSVQuery
from api.types import JWTPayload
from enums import StrategyDeploymentStatus
from infra.db.model import Strategy
from service.deployment import ProcessDeploymentService
from .controller import (
    create_deployment,
    get_deployment_orders,
    get_deployment_with_metrics,
    list_all_deployments,
    list_strategy_deployments,
    stop_deployment,
)
from .models import CreateDeploymentRequest, StrategyDeploymentResponse, StrategyDeploymentDetailResponse
from api.shared.models import OrderResponse, PerformanceMetrics
from .service import DeploymentService
from ..markets.service import MarketsService

router = APIRouter(prefix="/deployments", tags=["Deployments"])
service = DeploymentService(markets_service=MarketsService(), deployment_service=ProcessDeploymentService())


@router.post(
    "/",
    # response_model=DeploymentResponse,
    status_code=201,
)
async def create_strategy(
        body: CreateDeploymentRequest,
        jwt: JWTPayload = Depends(depends_jwt()),
        db_sess: AsyncSession = Depends(depends_db_sess),
        # deployment_service: DeploymentService = Depends(depends_deployment_service),
):
    # deployment = await create_deployment(jwt.sub, body, db_sess)
    # return {"deployment_id": deployment.deployment_id}
    # await deployment_service.run_strategy(deployment.deployment_id)

    # rsp_body = DeploymentResponse(
    #     deployment_id=deployment.deployment_id,
    #     strategy_id=deployment.strategy_id,
    #     broker_connection_id=deployment.broker_connection_id,
    #     symbol=deployment.symbol,
    #     timeframe=deployment.timeframe,
    #     status=deployment.status,
    #     error_message=deployment.error_message,
    #     created_at=deployment.created_at,
    #     updated_at=deployment.updated_at,
    #     stopped_at=deployment.stopped_at,
    # )

    # await db_sess.commit()

    # return rsp_body
    deployment = await service.create(body, db_sess)
    await db_sess.commit()
    return {"id": deployment.deployment_id}


@router.get(
    "/strategy/{strategy_id}/deployments",
    response_model=list[StrategyDeploymentResponse],
)
async def list_strategy_deployments_endpoint(
        strategy_id: UUID,
        skip: int = Query(0, ge=0),
        limit: int = Query(100, ge=1, le=100),
        jwt: JWTPayload = Depends(depends_jwt()),
        db_sess: AsyncSession = Depends(depends_db_sess),
):
    """
    List all deployments for a specific strategy with pagination.

    Returns list of deployments for the given strategy,
    ordered by creation date (newest first).
    """
    deployments = await list_strategy_deployments(
        jwt.sub, strategy_id, db_sess, skip, limit
    )

    return [
        StrategyDeploymentResponse(
            deployment_id=d.deployment_id,
            strategy_id=d.strategy_id,
            broker_connection_id=d.broker_connection_id,
            symbol=d.symbol,
            timeframe=d.timeframe,
            starting_balance=d.starting_balance or 0,
            status=d.status,
            error_message=d.error_message,
            created_at=d.created_at,
            updated_at=d.updated_at,
            stopped_at=d.stopped_at,
        )
        for d in deployments
    ]


@router.get("/{deployment_id}", response_model=StrategyDeploymentResponse)
async def get_deployment_endpoint(
        deployment_id: UUID,
        jwt: JWTPayload = Depends(depends_jwt()),
        db_sess: AsyncSession = Depends(depends_db_sess),
):
    """
    Get deployment details by ID.

    Returns full details of a specific deployment including status,
    configuration, and error messages if any.
    """
    # deployment = await get_deployment(deployment_id, db_sess)
    # deployment, metrics = await get_deployment_with_metrics(
    #     jwt.sub, deployment_id, db_sess
    # )
    #
    # if not deployment:
    #     raise HTTPException(status_code=404, detail="Deployment not found")
    #
    # strategy = await db_sess.scalar(
    #     select(Strategy).where(Strategy.strategy_id == deployment.strategy_id)
    # )
    # if not strategy or strategy.user_id != jwt.sub:
    #     raise HTTPException(status_code=404, detail="Deployment not found")
    #
    # return StrategyDeploymentResponse(
    #     deployment_id=deployment.deployment_id,
    #     strategy_id=deployment.strategy_id,
    #     broker_connection_id=deployment.broker_connection_id,
    #     symbol=deployment.symbol,
    #     timeframe=deployment.timeframe,
    #     starting_balance=deployment.starting_balance or 0,
    #     status=deployment.status,
    #     error_message=deployment.error_message,
    #     created_at=deployment.created_at,
    #     updated_at=deployment.updated_at,
    #     stopped_at=deployment.stopped_at,
    # )
    deployment = await service.get(deployment_id, jwt.sub, db_sess)
    return service.to_response(deployment, deployment.metrics)


# @router.get("/{deployment_id}/details", response_model=StrategyDeploymentDetailResponse)
# async def get_deployment_details_endpoint(
#         deployment_id: UUID,
#         jwt: JWTPayload = Depends(depends_jwt()),
#         db_sess: AsyncSession = Depends(depends_db_sess),
# ):
#     """
#     Get detailed deployment information including performance metrics.
#
#     This endpoint calculates real-time performance metrics from the deployment's
#     order history, including:
#     - Realised and unrealised PnL
#     - Total return percentage
#     - Sharpe ratio
#     - Maximum drawdown
#     - Trade count
#     - Equity curve
#
#     Note: Metrics are calculated on-demand from orders for accuracy.
#     """
#     deployment, metrics = await get_deployment_with_metrics(
#         jwt.sub, deployment_id, db_sess
#     )
#
#     return StrategyDeploymentDetailResponse(
#         deployment_id=deployment.deployment_id,
#         strategy_id=deployment.strategy_id,
#         broker_connection_id=deployment.broker_connection_id,
#         symbol=deployment.symbol,
#         timeframe=deployment.timeframe,
#         starting_balance=deployment.starting_balance or 0,
#         status=deployment.status,
#         error_message=deployment.error_message,
#         created_at=deployment.created_at,
#         updated_at=deployment.updated_at,
#         stopped_at=deployment.stopped_at,
#         metrics=metrics,
#     )


@router.post("/{deployment_id}/stop")
async def stop_deployment_endpoint(
        deployment_id: UUID,
        jwt: JWTPayload = Depends(depends_jwt()),
        db_sess: AsyncSession = Depends(depends_db_sess),
):
    """
    Stop a running deployment.

    Updates deployment status and sets stopped_at timestamp.
    Can only stop deployments that are currently RUNNING or PENDING.
    """
    # deployment = await stop_deployment(jwt.sub, deployment_id, db_sess)
    # rsp_body = StrategyDeploymentResponse(
    #     deployment_id=deployment.deployment_id,
    #     strategy_id=deployment.strategy_id,
    #     broker_connection_id=deployment.broker_connection_id,
    #     symbol=deployment.symbol,
    #     timeframe=deployment.timeframe,
    #     starting_balance=deployment.starting_balance or 0,
    #     status=deployment.status,
    #     error_message=deployment.error_message,
    #     created_at=deployment.created_at,
    #     updated_at=deployment.updated_at,
    #     stopped_at=deployment.stopped_at,
    # )
    # await db_sess.commit()
    #
    # return rsp_body
    await service.stop(deployment_id, jwt.sub, db_sess)
    await db_sess.commit()


@router.get("/", response_model=list[StrategyDeploymentResponse])
async def get_deployments(
        page: int = Query(1, ge=1),
        limit: int = Query(100, ge=1, le=100),
        # status: StrategyDeploymentStatus | None = Query(None),
        status: list[StrategyDeploymentStatus] | None = CSVQuery("status", StrategyDeploymentStatus, None),
        jwt: JWTPayload = Depends(depends_jwt()),
        db_sess: AsyncSession = Depends(depends_db_sess),
):
    # deployments = await list_all_deployments(jwt.sub, db_sess, skip, limit, status)
    #
    # return [
    #     StrategyDeploymentResponse(
    #         deployment_id=d.deployment_id,
    #         strategy_id=d.strategy_id,
    #         broker_connection_id=d.broker_connection_id,
    #         symbol=d.symbol,
    #         timeframe=d.timeframe,
    #         starting_balance=d.starting_balance or 0,
    #         status=d.status,
    #         error_message=d.error_message,
    #         created_at=d.created_at,
    #         updated_at=d.updated_at,
    #         stopped_at=d.stopped_at,
    #     )
    #     for d in deployments
    # ]
    return await service.get_all(jwt.sub, db_sess, page=page, limit=limit, status=status)


@router.get("/{deployment_id}/orders", response_model=list[OrderResponse])
async def get_deployment_orders_endpoint(
        deployment_id: UUID,
        page: int = Query(1, ge=1),
        limit: int = Query(50, ge=1, le=100),
        jwt: JWTPayload = Depends(depends_jwt()),
        db_sess: AsyncSession = Depends(depends_db_sess),
):
    # orders = await get_deployment_orders(jwt.sub, deployment_id, db_sess, skip, limit)
    #
    # return [
    #     OrderResponse(
    #         order_id=o.order_id,
    #         symbol=o.symbol,
    #         side=o.side,
    #         order_type=o.order_type,
    #         quantity=o.quantity,
    #         notional=o.notional,
    #         filled_quantity=o.filled_quantity,
    #         limit_price=o.limit_price,
    #         stop_price=o.stop_price,
    #         average_fill_price=o.avg_fill_price,
    #         status=o.status,
    #         submitted_at=o.submitted_at,
    #         filled_at=o.filled_at,
    #         broker_order_id=o.broker_order_id,
    #     )
    #     for o in orders
    # ]
    return await service.get_orders(deployment_id, jwt.sub, db_sess, page=page, limit=limit)
