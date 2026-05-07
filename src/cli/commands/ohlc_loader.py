import sys
import logging

import click

from cli.param.enum import EnumParam
from enums import BrokerType, MarketType, Timeframe
from runners import LoaderRunner, RunnerConfig
from service.ohlc.loader import LoaderConfig, AlpacaOHLCLoader
from utils import get_datetime

logger = logging.getLogger("commands.loader")


@click.group()
def loader():
    """Manage historical data loading."""
    pass


@loader.command(name="run")
@click.option(
    "--broker",
    type=EnumParam(BrokerType),
    required=True,
    help=f"Broker to load data from ({', '.join(BrokerType._value2member_map_.keys())})",
)
@click.option(
    "--symbol",
    required=True,
    help="Trading symbol (e.g., AAPL)",
)
@click.option(
    "--market-type",
    type=EnumParam(MarketType),
    required=True,
    help=f"Market type ({', '.join(MarketType._value2member_map_.keys())})",
)
@click.option(
    "--timeframe",
    type=EnumParam(Timeframe),
    required=True,
    help=f"Candle timeframe ({', '.join(Timeframe._value2member_map_.keys())})",
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
    default=get_datetime().date().strftime("%Y-%m-%d"),
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
@click.option(
    "--poll-interval",
    type=int,
    default=5,
    help="Polling interval in seconds",
)
@click.option("--verbose", is_flag=True, help="Enable verbose output")
def loader_run(
    broker,
    symbol,
    market_type,
    timeframe,
    start_date,
    end_date,
    api_key,
    secret_key,
    verbose,
    poll_interval,
):
    """
    Load historical candles from a broker and persist to database.
    """

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    broker_enum = broker

    if broker_enum == BrokerType.ALPACA:
        if not api_key or not secret_key:
            click.echo(
                "Error: --api-key and --secret-key are required for Alpaca broker",
                err=True,
            )
            sys.exit(1)

        loader_cls = AlpacaOHLCLoader
        loader_kwargs = {"api_key": api_key, "secret_key": secret_key}
    else:
        click.echo(f"Error: Unsupported broker: {broker_enum}", err=True)
        sys.exit(1)

    create_loader = lambda: loader_cls(**loader_kwargs)
    loader_config = LoaderConfig(
        cls=create_loader,
        symbol=symbol,
        market_type=market_type,
        timeframe=timeframe,
        start_date=start_date.date(),
        end_date=end_date.date(),
        poll_interval=poll_interval,
    )

    click.echo(
        f"Loading {symbol} candles ({timeframe}) "
        f"from {start_date.date()} to {end_date.date()}"
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