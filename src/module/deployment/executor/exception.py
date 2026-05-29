class DeploymentLimitReached(Exception):
    def __init__(self):
        super().__init__("Max concurrent deployments reached")
