class DeploymentExistsException(Exception):
    def __init__(self, deployment_id: str):
        self.deployment_id = deployment_id
        super().__init__(f"Deployment with id '{deployment_id}' already exists.")


class DeploymentLimitReached(Exception):
    def __init__(self):
        super().__init__("Max concurrent deployments reached")
