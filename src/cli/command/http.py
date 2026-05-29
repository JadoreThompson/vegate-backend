import logging
import click
import uvicorn


logger = logging.getLogger(__name__)


@click.group("http")
def http():
    return


@http.command(name="run")
def run():
    uvicorn.run("module.api.app:app", host="0.0.0.0", port=8000)
