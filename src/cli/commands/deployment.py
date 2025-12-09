"""Deployment commands."""

import sys
import logging

import click

from runners import DeploymentRunner

logger = logging.getLogger(__name__)


@click.group()
def deployment():
    """Manage live strategy deployments."""
    pass


@deployment.command(name="run")
@click.option("--deployment-id", required=True, help="UUID of the deployment to run")
@click.option("--verbose", is_flag=True, help="Enable verbose output")
def deployment_run(deployment_id, verbose):
    """
    Run a live strategy deployment by its ID.

    The deployment configuration must already exist in the database.

    Examples:
      vegate deployment run --deployment-id 123e4567-e89b-12d3-a456-426614174000
      vegate deployment run --deployment-id 123e4567-e89b-12d3-a456-426614174000 --verbose
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    click.echo(f"Starting deployment: {deployment_id}")
    click.echo("Press Ctrl+C to stop the deployment")

    try:
        runner = DeploymentRunner(deployment_id=deployment_id)
        runner.run()
        click.echo("Deployment stopped")
    except KeyboardInterrupt:
        click.echo("\nDeployment stopped by user")
    except Exception as e:
        click.echo(f"Error running deployment: {e}", err=True)
        logger.exception("Deployment failed")
        sys.exit(1)
