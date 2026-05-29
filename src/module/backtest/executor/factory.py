import sys
from typing import ClassVar

import docker

from config import IMAGE_NAME
from .base import BacktestExecutor
from .docker import DockerBacktestExecutor
from .process import ProcessBacktestExecutor


class BacktestExecutorFactory:

    _executors: ClassVar[dict] = {}

    @classmethod
    def create(cls, name: str) -> BacktestExecutor:
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
        return ProcessBacktestExecutor()

    @classmethod
    def _create_docker_executor(cls):
        platform = sys.platform

        if platform == "linux":
            docker_client = docker.DockerClient(base_url="unix://var/run/docker.sock")
        elif platform == "win32":
            docker_client = docker.from_env()
        else:
            raise ValueError(f"Unknown platform '{platform}'")

        return DockerBacktestExecutor(
            image_name=IMAGE_NAME, docker_client=docker_client
        )
