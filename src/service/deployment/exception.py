from uuid import UUID


class DeploymentNotFoundException(Exception):

    def __init__(self, deployment_id: UUID):
        super().__init__(f"Deployment '{deployment_id}' not found")
        self._deployment_id = deployment_id

    @property
    def deployment_id(self) -> UUID:
        return self._deployment_id
