from uuid import UUID


class DeploymentNotFoundException(Exception):

    def __init__(self, deployment_id: UUID):
        super().__init__(f"Deployment with id '{deployment_id}' not found")
        self._deployment_id = deployment_id

    @property
    def deployment_id(self):
        return self._deployment_id


class DeploymentAlreadyRunningException(Exception):

    def __init__(self, deployment_id: UUID):
        self._deployment_id = deployment_id
        super().__init__(f"Deployment with id '{deployment_id}' is already running")

    @property
    def deployment_id(self):
        return self._deployment_id
