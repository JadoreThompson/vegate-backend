import sys
import logging
from datetime import datetime

import click

from enums import BrokerType, Timeframe
from runners import LoaderRunner, RunnerConfig
from runners.loader_runner import LoaderConfig
from services.loaders import AlpacaLoader

logger = logging.getLogger("commands.loader")


@click.group()
def loader():
    """Manage historical data loading."""
    pass


@loader.command(name="run")
@click.option(
    "--broker",
    type=click.Choice(
        list(BrokerType._value2member_map_.keys()), case_sensitive=False
    ),
    required=True,
    help="Broker to load data from",
)
@click.option(
    "--symbol",
    required=True,
    help="Trading symbol (e.g., AAPL)",
)
@click.option(
    "--timeframe",
    type=click.Choice(list(Timeframe._value2member_map_.keys()), case_sensitive=False),
    required=True,
    help="Candle timeframe",
)
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=True,
    help="Start date (YYYY-MM-DD)",
)
@click.option(
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=True,
    help="End date (YYYY-MM-DD)",
)
@click.option(
    "--api-key",
    envvar="ALPACA_API_KEY",
    help="Alpaca API key (or set ALPACA_API_KEY env var)",
)
@click.option(
    "--secret-key",
    envvar="ALPACA_SECRET_KEY",
    help="Alpaca secret key (or set ALPACA_SECRET_KEY env var)",
)
@click.option("--verbose", is_flag=True, help="Enable verbose output")
def loader_run(
    broker, symbol, timeframe, start_date, end_date, api_key, secret_key, verbose
):
    """
    Load historical candles from a broker and persist to database.

    Examples:
      vegate loader run --broker alpaca --symbol AAPL --timeframe 1d \\
        --start-date 2024-01-01 --end-date 2024-12-31

      vegate loader run --broker alpaca --symbol AAPL --timeframe 1h \\
        --start-date 2024-01-01 --end-date 2024-01-31 \\
        --api-key YOUR_KEY --secret-key YOUR_SECRET
    """
    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    broker_enum = BrokerType(broker)
    if broker_enum == BrokerType.ALPACA:
        if not api_key or not secret_key:
            click.echo(
                "Error: --api-key and --secret-key are required for Alpaca broker",
                err=True,
            )
            sys.exit(1)

        loader_cls = AlpacaLoader
        loader_kwargs = {"api_key": api_key, "secret_key": secret_key}
    else:
        click.echo(f"Error: Unsupported broker: {broker}", err=True)
        sys.exit(1)

    timeframe_enum = Timeframe(timeframe)

    def create_loader():
        return loader_cls(**loader_kwargs)

    # Create LoaderConfig
    loader_config = LoaderConfig(
        cls=create_loader,
        symbol=symbol,
        timeframe=timeframe_enum,
        start_date=start_date.date(),
        end_date=end_date.date(),
    )

    click.echo(
        f"Loading {symbol} candles ({timeframe}) from {start_date.date()} to {end_date.date()}"
    )

    try:
        runner_config = RunnerConfig(
            cls=LoaderRunner,
            args=([loader_config],),
        )

        runner = runner_config.cls(*runner_config.args, **runner_config.kwargs)
        runner.run()

        click.echo("Data loaded successfully")
    except KeyboardInterrupt:
        click.echo("\nLoader stopped by user")
        sys.exit(0)
    except Exception as e:
        click.echo(f"Error loading data: {e}", err=True)
        logger.exception("Loader failed")
        sys.exit(1)
