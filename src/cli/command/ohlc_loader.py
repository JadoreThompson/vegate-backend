import asyncio
import logging
import sys
from datetime import timedelta

import click

from cli.param.enum import EnumParam
from module.broker.enums import BrokerType
from module.markets.enums import MarketType, Timeframe
from module.markets.loader.alpaca import AlpacaOHLCLoader
from util import get_datetime

logger = logging.getLogger("commands.ohlc_loader")


@click.group(name="ohlc_loader")
def ohlc_loader():
    """Manage historical data loading."""
    pass


@ohlc_loader.command(name="run")
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
    default=(get_datetime().date() + timedelta(days=1)).strftime("%Y-%m-%d"),
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
    broker,
    symbol,
    market_type,
    timeframe,
    start_date,
    end_date,
    api_key,
    secret_key,
    verbose,
):
    """
    Load historical candles from a broker and persist to database.
    """

    if verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if not api_key or not secret_key:
        click.echo(
            "Error: --api-key and --secret-key are required for Alpaca broker",
            err=True,
        )
        sys.exit(1)

    if broker == BrokerType.ALPACA:
        loader = AlpacaOHLCLoader(api_key=api_key, secret_key=secret_key)
    else:
        click.echo(f"Error: Unsupported broker: {broker}", err=True)
        sys.exit(1)

    click.echo(
        f"Loading {symbol} candles ({timeframe}) "
        f"from {start_date.date()} to {end_date.date()}"
    )

    async def run():
        try:
            await loader.load_candles(
                symbol=symbol,
                market_type=market_type,
                timeframe=timeframe,
                start_date=start_date,
                end_date=end_date,
            )

        finally:
            await loader.close()

    try:
        asyncio.run(run())
        click.echo("Data loaded successfully")
    except KeyboardInterrupt:
        sys.exit(0)
    except Exception as e:
        click.echo(f"Error loading data: {e}", err=True)
        logger.exception("Loader failed")
        sys.exit(1)
