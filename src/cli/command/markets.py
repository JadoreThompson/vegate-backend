import asyncio
import logging
import sys
from datetime import datetime, timedelta

import click

from cli.param.enum import EnumParam
from config import ALPACA_API_KEY, ALPACA_SECRET_KEY, ALPACA_API_KEY, CONFIG_YAML
from module.broker.enums import BrokerType
from module.markets.enums import MarketType, Timeframe
from module.markets.feed.alpaca.service import AlpacaOHLCFeed
from module.markets.feed.base import OHLCFeed
from module.markets.feed.manager import FeedManager
from module.markets.feed.server import OHLCFeedServer
from module.markets.loader.alpaca import AlpacaOHLCLoader
from util import get_datetime

logger = logging.getLogger("commands.markets")


@click.group(name="markets")
def markets():
    """Manage market data."""
    pass


@markets.group(name="loader")
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
def run(
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


@markets.group(name="feed")
def feed():
    """Manage live OHLC feeds."""
    pass


async def _wrapper(coro):
    try:
        await coro
    except asyncio.CancelledError:
        pass


@feed.command(name="run")
@click.option("--host", type=str, required=True, help="Server host")
@click.option("--port", type=int, required=True, help="Server port")
def run(host, port):
    async def _run():
        feeds: list[OHLCFeed] = []
        for item in CONFIG_YAML["ohlc_feed"]:
            broker = BrokerType(item["broker"])
            market_type = MarketType(item["market_type"])
            symbol = item["symbol"]

            for tf in item["timeframes"]:
                timeframe = Timeframe(tf)
                if broker == BrokerType.ALPACA:
                    feed = AlpacaOHLCFeed(
                        symbol=symbol,
                        market_type=market_type,
                        timeframe=timeframe,
                        api_key=ALPACA_API_KEY,
                        secret_key=ALPACA_SECRET_KEY,
                    )

                    loader = AlpacaOHLCLoader(
                        api_key=ALPACA_API_KEY, secret_key=ALPACA_SECRET_KEY
                    )
                else:
                    raise ValueError(f"Unsupported broker type '{broker}'")

                await loader.load_candles(
                    symbol,
                    market_type,
                    timeframe,
                    item["start_date"],
                    datetime.now() + timedelta(days=1),
                )
                asyncio.create_task(_wrapper(feed.run()))
                feeds.append(feed)

        feed_manager = FeedManager()
        server = OHLCFeedServer(feed_manager, host, port)
        try:
            await server.init(feeds)
            await server.run()
        except KeyboardInterrupt:
            pass
        finally:
            await server.stop()
            for feed in feeds:
                await feed.stop()

    asyncio.run(_run())
