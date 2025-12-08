import logging
from uuid import UUID

from aiohttp import ClientSession

from .exc import DeploymentError


logger = logging.getLogger(__name__)


class DeploymentService:
    def __init__(
        self,
        api_key: str,
        project_id: str,
        docker_image: str,
        service_name_prefix: str,
        environment_id: str,
    ):
        self.api_key = api_key
        self.project_id = project_id
        self.docker_image = docker_image
        self.service_name_prefix = service_name_prefix
        self.environment_id = environment_id
        self._base_url = "https://backboard.railway.app/graphql/v2"
        self._headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {api_key}",
        }
        self._http_sess = ClientSession()

    async def deploy(self, id_: UUID) -> dict:
        """
        Create a new Railway service and deploy it with the given deployment_id.
        """
        service_name = f"{self.service_name_prefix}{id_}"
        service_id = await self._create_service(service_name, id_)

        return {
            "service_id": service_id,
            "service_name": service_name,
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

    async def _create_service(self, service_name: str, id_: UUID) -> str:
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
                "projectId": self.project_id,
                "name": service_name,
                "source": {"image": self.docker_image},
                "variables": {self.environment_id: str(id_)},
            }
        }

        logger.info("Creating service")
        result = await self._execute_query(query, variables)
        return result["serviceCreate"]["id"]
