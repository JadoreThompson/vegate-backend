import logging
from uuid import UUID

from aiohttp import ClientSession

from config import (
    RAILWAY_API_KEY,
    RAILWAY_ENVIRONMENT_ID,
    RAILWAY_PROJECT_ID,
    RAILWAY_SERVICE_IMAGE,
)

from .exc import DeploymentError


logger = logging.getLogger(__name__)


class DeploymentService:
    def __init__(
        self,
    ):
        self._base_url = "https://backboard.railway.app/graphql/v2"
        self._headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {RAILWAY_API_KEY}",
        }
        self._http_sess = ClientSession()

    async def deploy(
        self, backtest_id: UUID | None = None, deployment_id: UUID | None = None
    ) -> dict:
        """
        Create a new Railway service and deploy it with the given deployment_id.
        """
        start_command = f"uv run src/main.py "

        if backtest_id is not None:
            name = f"bt_{backtest_id}"
            start_command += f"backtest run --backtest-id {backtest_id}"
        elif deployment_id is not None:
            name = f"dp_{deployment_id}"
            start_command += f"deplyoment run --deployment-id {deployment_id}"
        else:
            raise ValueError(f"Neither deployment_id nor backtest_id were provided")

        service_id = await self._create_service(name)
        await self._update_service(service_id, start_command)
        await self._deploy_service(service_id)

        return {
            "service_id": service_id,
            "service_name": name,
        }

    async def _execute_query(self, query: str, variables: dict | None = None) -> dict:
        """Execute a GraphQL query asynchronously"""
        payload = {"query": query}
        if variables:
            payload["variables"] = variables

        rsp = await self._http_sess.post(
            self._base_url, headers=self._headers, json=payload, timeout=30.0
        )
        if rsp.status != 200:
            raise DeploymentError(
                f"Query failed with status {rsp.status}: {await rsp.text()}"
            )

        result = await rsp.json()

        if "errors" in result:
            error_messages = [
                error.get("message", str(error)) for error in result["errors"]
            ]
            raise DeploymentError(f"GraphQL errors: {', '.join(error_messages)}")

        return result["data"]

    async def _create_service(self, service_name: str) -> str:
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

        logger.info("Creating service")
        result = await self._execute_query(query, variables)
        return result["serviceCreate"]["id"]

    async def _update_service(self, service_id: str, start_command: str):
        query = """
        mutation serviceInstanceUpdate($input: ServiceInstanceUpdateInput!, $serviceId: String!) {
            serviceInstanceUpdate(
                input: $input,
                serviceId: $serviceId
            ) 
        }
        """

        variables = {"input": {"startCommand": start_command}, "serviceId": service_id}
        logger.info(f"Updating service '{service_id}'")
        await self._execute_query(query, variables)

    async def _deploy_service(self, service_id: str):
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
        logger.info(f"Deploying service '{service_id}'")
        result = await self._execute_query(query, variables)

        return result
