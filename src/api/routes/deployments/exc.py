"""Custom exceptions for deployment operations."""


class DeploymentNotFoundError(Exception):
    """Raised when a deployment is not found."""

    pass


class DeploymentAlreadyStoppedError(Exception):
    """Raised when trying to stop an already stopped deployment."""

    pass


class InvalidDeploymentStatusError(Exception):
    """Raised when deployment is in wrong state for operation."""

    pass
