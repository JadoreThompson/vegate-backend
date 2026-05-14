import logging
from multiprocessing import Process
import sys
import time
import click
import uvicorn

from config import IS_PRODUCTION
from runners.api_runner import APIRunner
from runners.runner_config import RunnerConfig
from runners.utils import run_runner

logger = logging.getLogger(__name__)


@click.group("http")
def http():
    return


@http.command(name="run")
def run():
    uvicorn.run(
        "api.app:app", host="0.0.0.0" if IS_PRODUCTION else "localhost", port=8000
    )
