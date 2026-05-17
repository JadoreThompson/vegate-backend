import asyncio
import logging
from uuid import UUID

from aiohttp import ClientSession
from sqlalchemy import select, update

from config import (
    RAILWAY_API_KEY,
    RAILWAY_ENVIRONMENT_ID,
    RAILWAY_PROJECT_ID,
    RAILWAY_SERVICE_IMAGE,
)
from infra.db.model.backtest import Backtest
from infra.db.model.strategy_deployments import StrategyDeployments
from infra.db.utils import get_db_session

from .exception import RailwayDeploymentException
from service.deployment.base import DeploymentService


class RailwayDeploymentService(DeploymentService):
    def __init__(self):
        super().__init__()
        self._base_url = "https://backboard.railway.app/graphql/v2"
        self._headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {RAILWAY_API_KEY}",
        }
        self._http_sess: ClientSession | None = None
        self._logger = logging.getLogger(type(self).__name__)

    async def init(self) -> None:
        """Initialize the HTTP session. Must be called before using the service."""
        if self._http_sess is None:
            self._http_sess = ClientSession()

    async def deploy_backtest(self, backtest_id: UUID) -> dict:
        name = f"bt_{backtest_id}"
        start_command = f"uv run src/main.py backtest run --backtest-id {backtest_id}"
        service_id = await self.create_service(name)
        await self.set_backtest_service_id(backtest_id, service_id)
        await self.update_service(service_id, start_command)
        await self.deploy_service(service_id)

        return {
            "service_id": service_id,
            "service_name": name,
            "environment": "production",
        }

    async def stop_backtest(self, backtest_id: UUID) -> dict:
        async with get_db_session() as sess:
            result = await sess.execute(
                select(Backtest.service_id).where(Backtest.id == backtest_id)
            )
            service_id = result.scalar()

        if service_id is None:
            service_id = await self.get_service_id_by_name(f"bt_{backtest_id}")
            if service_id is None:
                raise RailwayDeploymentException(
                    f"No service ID found for backtest {backtest_id}"
                )

        await self.stop_service(service_id)
        return {"service_id": service_id}

    async def run(self, deployment_id: UUID) -> dict:
        name = f"dp_{deployment_id}"
        start_command = (
            f"uv run src/main.py deployment run --deployment-id {deployment_id}"
        )
        service_id = await self.create_service(name)
        await self.set_deployment_service_id(deployment_id, service_id)
        await self.update_service(service_id, start_command)
        await self.deploy_service(service_id)

        return {
            "service_id": service_id,
            "service_name": name,
            "environment": "production",
        }

    async def stop(self, deployment_id: UUID) -> dict:
        async with get_db_session() as sess:
            result = await sess.execute(
                select(StrategyDeployments.service_id).where(
                    StrategyDeployments.deployment_id == deployment_id
                )
            )
            service_id = result.scalar()

        if service_id is None:
            service_id = await self.get_service_id_by_name(f"dp_{deployment_id}")
            if service_id is None:
                raise RailwayDeploymentException(
                    f"No service ID found for deployment {deployment_id}"
                )

        await self.stop_service(service_id)
        return {"service_id": service_id}

    async def execute_query(self, query: str, variables: dict | None = None) -> dict:
        """Execute a GraphQL query asynchronously"""
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        rsp = await self._http_sess.post(
            self._base_url, headers=self._headers, json=payload, timeout=30.0
        )
        if rsp.status != 200:
            raise RailwayDeploymentException(
                f"Query failed with status {rsp.status}: {await rsp.text()}"
            )

        result = await rsp.json()

        if "errors" in result:
            error_messages = [
                error.get("message", str(error)) for error in result["errors"]
            ]
            raise RailwayDeploymentException(
                f"GraphQL errors: {', '.join(error_messages)}"
            )

        return result["data"]

    async def create_service(self, service_name: str) -> str:
        """Create a new Railway service"""
        query = """
        mutation ServiceCreate($input: ServiceCreateInput!) {
            serviceCreate(input: $input) {
                id
            }
        }
        """
        variables = {
            "input": {
                "projectId": RAILWAY_PROJECT_ID,
                "name": service_name,
                "source": {"image": RAILWAY_SERVICE_IMAGE},
            }
        }

        self._logger.info("Creating service")
        result = await self.execute_query(query, variables)
        self._logger.info(f"Service created with ID: {result['serviceCreate']['id']}")
        return result["serviceCreate"]["id"]

    async def update_service(self, service_id: str, start_command: str):
        query = """
        mutation serviceInstanceUpdate($input: ServiceInstanceUpdateInput!, $serviceId: String!) {
            serviceInstanceUpdate(
                input: $input,
                serviceId: $serviceId
            ) 
        }
        """

        variables = {"input": {"startCommand": start_command}, "serviceId": service_id}
        self._logger.info(f"Updating service '{service_id}'")
        await self.execute_query(query, variables)

    async def deploy_service(self, service_id: str):
        """Deploy the service instance"""
        query = """
        mutation ServiceInstanceDeploy($serviceId: String!, $environmentId: String!) {
            serviceInstanceDeploy(
                serviceId: $serviceId,
                environmentId: $environmentId
            )
        }
        """

        variables = {"serviceId": service_id, "environmentId": RAILWAY_ENVIRONMENT_ID}
        self._logger.info(f"Deploying service '{service_id}'")
        result = await self.execute_query(query, variables)
        return result

    async def get_service_id_by_name(self, service_name: str) -> str:
        """Look up a service ID by name within the configured project."""
        query = """
        query GetProject($id: String!) {
            project(id: $id) {
                services {
                    edges {
                        node {
                            id
                            name
                        }
                    }
                }
            }
        }
        """
        variables = {"id": RAILWAY_PROJECT_ID}
        self._logger.info(f"Looking up service ID for '{service_name}'")
        result = await self.execute_query(query, variables)

        for edge in result["project"]["services"]["edges"]:
            node = edge["node"]
            if node["name"] == service_name:
                return node["id"]

        raise RailwayDeploymentException(f"No service found with name '{service_name}'")

    async def get_service_by_id(self, service_id: str) -> dict:
        query = """
        query GetService($id: String!) {
            service(id: $id) {
                __typename
                id
                name                
                icon
                createdAt
                projectId
            }
        }
        """

        variables = {"id": service_id}

        self._logger.info(f"Fetching service by id '{service_id}'")
        result = await self.execute_query(query, variables)

        if not result or not result.get("service"):
            raise RailwayDeploymentException(f"No service found with id '{service_id}'")

        return result["service"]

    async def stop_service(self, service_id: str) -> None:
        """Stop a running service by deleting it."""
        query = """
        mutation serviceDelete($id: String!) {
            serviceDelete(id: $id)
        }
        """
        variables = {"id": service_id}
        self._logger.info(f"Deleting service '{service_id}'")
        await self.execute_query(query, variables)

    async def set_backtest_service_id(self, backtest_id: UUID, service_id: str):
        """Store the Railway service ID in the database for later reference"""
        async with get_db_session() as sess:
            await sess.execute(
                update(Backtest)
                .where(Backtest.id == backtest_id)
                .values(service_id=service_id)
            )
            await sess.commit()

    async def set_deployment_service_id(self, deployment_id: UUID, service_id: str):
        """Store the Railway service ID in the database for later reference"""
        async with get_db_session() as sess:
            await sess.execute(
                update(StrategyDeployments)
                .where(StrategyDeployments.deployment_id == deployment_id)
                .values(service_id=service_id)
            )
            await sess.commit()

    async def stop_all(self) -> dict:
        """Stop all running services related to backtests and deployments"""
        async with get_db_session() as sess:
            backtest_result = await sess.execute(
                select(Backtest.service_id).where(Backtest.service_id.is_not(None))
            )
            deployment_result = await sess.execute(
                select(StrategyDeployments.service_id).where(
                    StrategyDeployments.service_id.is_not(None)
                )
            )

            backtest_service_ids = {row[0] for row in backtest_result.all()}
            deployment_service_ids = {row[0] for row in deployment_result.all()}

        service_ids = backtest_service_ids | deployment_service_ids

        if not service_ids:
            return {
                "stopped": 0,
                "service_ids": [],
                "message": "No running services found",
            }

        self._logger.info(f"Stopping {len(service_ids)} services")

        results = await asyncio.gather(
            *[self.stop_service(service_id) for service_id in service_ids],
            return_exceptions=True,
        )

        failed = []
        for service_id, result in zip(service_ids, results):
            if isinstance(result, Exception):
                self._logger.error(f"Failed to stop {service_id}: {result}")
                failed.append(service_id)

        return {
            "stopped": len(service_ids) - len(failed),
            "failed": failed,
            "service_ids": list(service_ids),
        }
