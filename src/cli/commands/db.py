import sys
import subprocess

import click

from infra.db import write_db_url_alembic_ini


@click.group()
def db():
    """Database management commands."""
    pass


@db.command(name="upgrade")
def db_upgrade():
    """
    Apply all migrations to the database
    """
    click.echo(f"Upgrading database")
    try:
        write_db_url_to_alembic_ini()
        subprocess.run(["uv", "run", "alembic", "upgrade", "head"], check=True)
        click.echo("Database upgraded successfully")
    except subprocess.CalledProcessError as e:
        click.echo(f"Error upgrading database: {e}", err=True)
        sys.exit(1)
    except Exception as e:
        click.echo(f"Unexpected error: {e}", err=True)
        sys.exit(1)
