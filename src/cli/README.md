# CLI Module Structure

This directory contains the modular CLI implementation for the Vegate Backend.

## Architecture

The CLI follows a modular design pattern for scalability and maintainability:

```
src/cli/
├── __init__.py           # Package exports
├── main.py               # Main CLI entry point
├── commands/             # Command modules
│   ├── __init__.py       # Command exports
│   ├── backend.py        # Backend server commands
│   ├── backtest.py       # Backtest commands
│   ├── deployment.py     # Deployment commands
│   └── db.py             # Database commands
└── README.md             # This file
```

## Design Principles

### 1. Separation of Concerns

Each command group is in its own module, making it easy to:

- Locate and modify specific functionality
- Add new commands without touching other code
- Test individual command groups in isolation

### 2. Scalability

Adding new commands is straightforward:

1. Create a new file in `commands/` (e.g., `monitoring.py`)
2. Define a Click command group
3. Export it from `commands/__init__.py`
4. Register it in `main.py`

### 3. Maintainability

- Clear file structure
- Consistent naming conventions
- Self-contained modules
- Minimal coupling between commands

## Command Modules

### backend.py

Manages the main API server with multiprocessing support.

**Commands:**

- `vegate backend run` - Start the FastAPI server

**Key Features:**

- Process management
- Auto-reload for development
- Multiple worker support
- Graceful shutdown handling

### backtest.py

Handles backtest execution.

**Commands:**

- `vegate backtest run` - Execute a backtest by ID

**Key Features:**

- Database integration
- Verbose logging option
- Error handling and reporting

### deployment.py

Manages live trading deployments.

**Commands:**

- `vegate deployment run` - Run a live deployment by ID

**Key Features:**

- Live trading support
- Broker integration
- Graceful shutdown
- Error recovery

### db.py

Database migration and management utilities.

**Commands:**

- `vegate db upgrade` - Apply migrations
- `vegate db downgrade` - Rollback migrations
- `vegate db current` - Show current revision
- `vegate db history` - View migration history

**Key Features:**

- Alembic integration
- Auto-configuration from environment
- Safety confirmations for destructive operations

## Adding New Commands

### Example: Adding a Monitoring Command

1. **Create the module** (`commands/monitoring.py`):

```python
"""Monitoring commands."""
import click

@click.group()
def monitoring():
    """Monitor system health and metrics."""
    pass

@monitoring.command(name="status")
def monitoring_status():
    """Check system status."""
    click.echo("System is running")
```

2. **Export from `commands/__init__.py`**:

```python
from .monitoring import monitoring

__all__ = [..., "monitoring"]
```

3. **Register in `main.py`**:

```python
from .commands import monitoring

cli.add_command(monitoring)
```

That's it! Now you can run `vegate monitoring status`.

## Best Practices

### Error Handling

- Always catch exceptions in command functions
- Use `click.echo(err=True)` for errors
- Exit with appropriate status codes (`sys.exit(1)`)
- Log exceptions for debugging

### User Feedback

- Use `click.echo()` for user-facing messages
- Show progress for long operations
- Provide clear error messages
- Include examples in docstrings

### Options and Arguments

- Use `--option-name` for flags and options
- Use descriptive help text
- Set sensible defaults with `show_default=True`
- Group related options logically

### Documentation

- Write clear docstrings for all commands
- Include usage examples
- Document options and their effects
- Update the main CLI.md when adding features

## Testing

### Manual Testing

```bash
# Test help messages
python -m cli.main --help
python -m cli.main backend --help

# Test commands
python -m cli.main backend run --help
python -m cli.main db current
```

### Unit Testing

Create test files in `tests/cli/`:

```python
from click.testing import CliRunner
from cli.main import cli

def test_backend_run():
    runner = CliRunner()
    result = runner.invoke(cli, ['backend', 'run', '--help'])
    assert result.exit_code == 0
```

## Common Patterns

### Progress Indicators

```python
import click

with click.progressbar(items) as bar:
    for item in bar:
        process(item)
```

### Confirmations

```python
if click.confirm('Are you sure?'):
    perform_action()
```

### Verbose Mode

```python
@click.option('--verbose', is_flag=True)
def command(verbose):
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)
```

### Multiple Options

```python
@click.option('--host', default='0.0.0.0')
@click.option('--port', default=8000, type=int)
@click.option('--reload', is_flag=True)
def command(host, port, reload):
    # Implementation
```

## Troubleshooting

### Import Issues

- Ensure `commands/__init__.py` exports all command groups
- Check that `main.py` imports and registers commands
- Verify module paths are correct

### Command Not Found

- Check the command is registered in `main.py`
- Verify the entry point in `pyproject.toml`
- Reinstall with `uv sync` or `pip install -e .`

### Click Errors

- Ensure all Click decorators are properly ordered
- Verify option names don't conflict
- Check that command functions have unique names

## Future Enhancements

Potential improvements to consider:

1. **Configuration Files**: Support for `.vegaterc` or `vegate.yaml`
2. **Plugin System**: Allow external command plugins
3. **Shell Completion**: Tab completion for bash/zsh
4. **Logging Configuration**: Per-command log levels
5. **Interactive Mode**: REPL-style interface
6. **Remote Execution**: Run commands on remote servers
7. **Batch Operations**: Process multiple items at once

## References

- [Click Documentation](https://click.palletsprojects.com/)
- [Click Best Practices](https://click.palletsprojects.com/en/8.1.x/complex/)
- Main CLI Documentation: `docs/CLI.md`
