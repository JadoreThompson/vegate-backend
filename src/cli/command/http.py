import logging
import click
import uvicorn

from config import IS_PRODUCTION


logger = logging.getLogger(__name__)


@click.group("http")
def http():
    return


@http.command(name="run")
def run():
    uvicorn.run(
        "module.api.app:app", 
        host="0.0.0.0",# if IS_PRODUCTION else "localhost", 
        port=8000
    )
