import sys
from typing import ClassVar

import docker

from config import IMAGE_NAME
from .base import DeploymentExecutor
from .docker import DockerDeploymentExecutor
from .process import ProcessDeploymentExecutor


class DeploymentExecutorFactory:

    _executors: ClassVar[dict] = {}

    @classmethod
    def create(cls, name: str) -> DeploymentExecutor:
        if name in cls._executors:
            return cls._executors[name]
        
        if name == "process":
            cls._executors[name] = cls._create_process_executor()
            return cls._executors[name]

        if name == "docker":
            cls._executors[name] = cls._create_docker_executor()
            return cls._executors[name]

        raise ValueError(f"Executor name '{name}' not supported")

    @classmethod
    def _create_process_executor(cls):
        return ProcessDeploymentExecutor()

    @classmethod
    def _create_docker_executor(cls):
        platform = sys.platform

        if platform == "linux":
            docker_client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        elif platform == "win32":
            docker_client = docker.from_env()
        else:
            raise ValueError(f"Unknown platform '{platform}'")

        return DockerDeploymentExecutor(
            image_name=IMAGE_NAME, docker_client=docker_client
        )
