from uuid import UUID

from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import depends_db_sess, depends_jwt
from api.typing import JWTPayload
from config import RAILWAY_API_KEY, RAILWAY_PROJECT_ID
from core.enums import StrategyDeploymentStatus
from services import DeploymentService
from .models import (
    DeployStrategyRequest,
    DeploymentResponse,
)

router = APIRouter(prefix="/deployments", tags=["Deployments"])
deployment_service = DeploymentService(
    api_key=RAILWAY_API_KEY,
    project_id=RAILWAY_PROJECT_ID,
    docker_image="wifimemes/vegate-deployment:latest",
)

@router.post(
    "/strategies/{strategy_id}/deploy",
    response_model=DeploymentResponse,
    status_code=201,
)
async def deploy_strategy_endpoint(
    strategy_id: UUID,
    body: DeployStrategyRequest,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """
    Deploy a strategy to a broker connection.

    Creates a new deployment that will run the specified strategy
    with the given configuration on the connected broker account.
    """
    # TODO: Implement controller logic
    # - Verify strategy exists and belongs to user
    # - Verify broker_connection exists and belongs to user
    # - Create deployment record with PENDING status
    # - Validate ticker and timeframe formats
    # - Return DeploymentResponse
    pass


@router.get(
    "/strategies/{strategy_id}/deployments",
    response_model=list[DeploymentResponse],
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
    # TODO: Implement controller logic
    # - Verify strategy exists and belongs to user
    # - Query deployments filtered by strategy_id and user_id
    # - Apply pagination using skip and limit
    # - Return list[DeploymentResponse]
    return []



@router.get("/{deployment_id}", response_model=DeploymentResponse)
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
    # TODO: Implement controller logic
    # - Query deployment by deployment_id
    # - Verify deployment exists (raise 404 if not)
    # - Verify deployment belongs to user via strategy relationship
    # - Return DeploymentResponse
    pass


@router.post("/{deployment_id}/stop", response_model=DeploymentResponse)
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
    # TODO: Implement controller logic
    # - Query deployment by deployment_id
    # - Verify deployment exists (raise 404 if not)
    # - Verify deployment belongs to user via strategy relationship
    # - Check deployment status (raise error if already stopped)
    # - Update status to appropriate stopped state
    # - Set stopped_at timestamp
    # - Return updated DeploymentResponse
    pass


@router.get("/", response_model=list[DeploymentResponse])
async def list_all_deployments_endpoint(
    skip: int = Query(0, ge=0),
    limit: int = Query(100, ge=1, le=100),
    status: StrategyDeploymentStatus | None = Query(None),
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    """
    List all user's deployments with pagination and optional status filter.

    Returns list of all deployments belonging to the user,
    optionally filtered by status, ordered by creation date (newest first).
    """
    # TODO: Implement controller logic
    # - Query deployments filtered by user_id (via strategy relationship)
    # - Apply optional status filter if provided
    # - Apply pagination using skip and limit
    # - Return list[DeploymentResponse]
    return []
