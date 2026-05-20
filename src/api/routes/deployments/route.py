from uuid import UUID

from fastapi import APIRouter, Depends, Query, Response
from fastapi.sse import EventSourceResponse
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import (
    depends_class,
    depends_db_sess,
    depends_jwt,
    CSVQuery,
)
from api.models import PaginatedResponse
from api.types import JWTPayload
from enums import StrategyDeploymentStatus
from events.deployment import DeploymentEventT
from infra.db.model.instrument import Instrument
from service.deployment.consumer import StrategyDeploymentEventsConsumer
from .models import (
    CreateDeploymentRequest,
    CreateStrategyDeploymentResponse,
    StrategyDeploymentOrderResponse,
    StrategyDeploymentResponse,
)
from .service import APIDeploymentsService

router = APIRouter(prefix="/deployments", tags=["Deployments"])


@router.post(
    "/",
    response_model=CreateStrategyDeploymentResponse,
    status_code=201,
)
async def create_deployment(
    body: CreateDeploymentRequest,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    deployments_service: APIDeploymentsService = Depends(
        depends_class(APIDeploymentsService)
    ),
):
    deployment = await deployments_service.create(body, db_sess)
    await db_sess.commit()
    return {"id": deployment.deployment_id}


@router.get("/{deployment_id}", response_model=StrategyDeploymentResponse)
async def get_deployment_endpoint(
    deployment_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    deployments_service: APIDeploymentsService = Depends(
        depends_class(APIDeploymentsService)
    ),
):
    """
    Get deployment details by ID.

    Returns full details of a specific deployment including status,
    configuration, and error messages if any.
    """
    deployment = await deployments_service.get(deployment_id, jwt.sub, db_sess)
    instrument = await db_sess.get(Instrument, deployment.instrument_id)
    return deployments_service.to_response(deployment, instrument, deployment.metrics)


@router.post("/{deployment_id}/start")
async def start_deployment_endpoint(
    deployment_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    deployments_service: APIDeploymentsService = Depends(
        depends_class(APIDeploymentsService)
    ),
):
    await deployments_service.start(deployment_id, jwt.sub, db_sess)
    await db_sess.commit()


@router.post("/{deployment_id}/stop")
async def stop_deployment_endpoint(
    deployment_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    deployments_service: APIDeploymentsService = Depends(
        depends_class(APIDeploymentsService)
    ),
):
    await deployments_service.stop(deployment_id, jwt.sub, db_sess)
    await db_sess.commit()


@router.get("/", response_model=PaginatedResponse[StrategyDeploymentResponse])
async def get_deployments(
    page: int = Query(1, ge=1),
    limit: int = Query(100, ge=1, le=100),
    status: list[StrategyDeploymentStatus] | None = CSVQuery(
        "status", StrategyDeploymentStatus, None
    ),
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    deployments_service: APIDeploymentsService = Depends(
        depends_class(APIDeploymentsService)
    ),
):
    return await deployments_service.get_all(
        jwt.sub, db_sess, page=page, limit=limit, status=status
    )


@router.get(
    "/{deployment_id}/orders",
    response_model=PaginatedResponse[StrategyDeploymentOrderResponse],
)
async def get_deployment_orders_endpoint(
    deployment_id: UUID,
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    deployments_service: APIDeploymentsService = Depends(
        depends_class(APIDeploymentsService)
    ),
):
    return await deployments_service.get_orders(
        deployment_id, jwt.sub, db_sess, page=page, limit=limit
    )


@router.get(
    "/{deployment_id}/events", response_model=PaginatedResponse[DeploymentEventT]
)
async def get_events(
    deployment_id: UUID,
    page: int = Query(1, ge=1),
    limit: int = Query(50, ge=1, le=100),
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    deployments_service: APIDeploymentsService = Depends(
        depends_class(APIDeploymentsService)
    ),
):
    return await deployments_service.get_events(
        deployment_id, jwt.sub, db_sess, page=page, limit=limit
    )


@router.get("/{deployment_id}/events/stream", response_class=EventSourceResponse)
async def sse_stream(
    deployment_id: UUID,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
    consumer: StrategyDeploymentEventsConsumer = Depends(
        depends_class(StrategyDeploymentEventsConsumer)
    ),
    deployments_service: APIDeploymentsService = Depends(
        depends_class(APIDeploymentsService)
    ),
):
    await deployments_service.get(deployment_id, jwt.sub, db_sess)
    
    try:
        queue = await consumer.register(deployment_id)
        while True:
            event = await queue.get()
            yield event
    finally:
        await consumer.remove(deployment_id)
