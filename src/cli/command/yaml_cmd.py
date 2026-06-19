import sys
from pathlib import Path

import click
import yaml as pyyaml

from core.yaml import YamlLoader


def _dot_get(data, key_path: str):
    """Traverse *data* following dot-separated *key_path* and return the
    value at the end (or the whole *data* if *key_path* is empty)."""
    if not key_path:
        return data
    current = data
    for part in key_path.split("."):
        if isinstance(current, dict):
            current = current[part]
        elif isinstance(current, list):
            current = current[int(part)]
        else:
            raise KeyError(f"Cannot traverse {type(current).__name__} with key {part!r}")
    return current


@click.command(name="yaml")
@click.option(
    "--file",
    "-f",
    type=click.Path(exists=True, dir_okay=False, file_okay=True),
    required=True,
    help="Path to the YAML file to interpolate",
)
@click.option(
    "--env-file",
    "-e",
    "env_file",
    type=click.Path(exists=True, dir_okay=False, file_okay=True),
    default=None,
    help="Path to a .env file to load before interpolation",
)
@click.option(
    "-o",
    "output_key",
    type=str,
    default=None,
    help="Dot-notation key path to extract (e.g. 'foo.bar.0'). "
         "When omitted the whole document is printed.",
)
def yaml_cmd(file, env_file, output_key):
    """Load a YAML file, interpolate ${VAR} / ${VAR:-default} /
    ${VAR:?error} placeholders using environment variables, and print the
    result as YAML to stdout.

    Pipe the output to a file with ``> output.yaml``.
    """
    if env_file:
        from dotenv import load_dotenv

        load_dotenv(env_file, override=True)

    loader = YamlLoader(file)
    try:
        data = loader.load()
    except Exception as exc:
        click.echo(f"Error loading YAML: {exc}", err=True)
        sys.exit(1)

    if output_key:
        try:
            data = _dot_get(data, output_key)
        except (KeyError, IndexError, ValueError) as exc:
            click.echo(f"Error traversing key path {output_key!r}: {exc}", err=True)
            sys.exit(1)

    pyyaml.dump(data, sys.stdout, default_flow_style=False, sort_keys=False)