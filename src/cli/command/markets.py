import asyncio
import logging
import sys
from datetime import date, timedelta

import click
import yaml

from cli.param.enum import EnumParam
from core.yaml import YamlLoader
from module.health.server import HealthCheckServer
from module.markets.feed.alpaca import AlpacaOHLCFeed
from module.markets.feed.base import OHLCFeed
from module.markets.feed.manager import FeedManager
from module.markets.feed.server import OHLCFeedServer
from module.markets.loader import OHLCLoader, AlpacaOHLCLoader, OHLCLoadResult
from module.markets.loader.poll import OHLCPoller, PollSubscription
from util import get_datetime
from vegate.markets.enums import MarketType, Timeframe
from vegate.oms.enums import BrokerType

logger = logging.getLogger("commands.markets")


@click.group(name="markets")
def markets():
    """Manage market data."""
    pass


@markets.group(name="loader")
def loader():
    """Manage historical data loading."""
    pass


def handle_poll_loaders(fpath: str, poll_interval: int, health_port: int):
    if fpath is None:
        click.echo(
            "Error: --file must be provided if --poll-interval is specified",
            err=True,
        )
        sys.exit(1)

    with open(fpath, "r") as f:
        load_config = yaml.load(f, Loader=yaml.SafeLoader)

    yamloader = YamlLoader(fpath)
    load_config = yamloader.load()

    if not isinstance(load_config, list):
        click.echo("File must contain a list of load configurations", err=True)
        sys.exit(1)

    loaders: dict[BrokerType, OHLCLoader] = {}
    subscriptions: list[PollSubscription] = []

    for item in load_config:
        broker = BrokerType(item["broker"])
        api_key = item["api_key"]
        secret_key = item["secret_key"]

        if broker not in loaders:
            if broker == BrokerType.ALPACA:
                loaders[broker] = AlpacaOHLCLoader(
                    api_key=api_key, secret_key=secret_key
                )
            else:
                click.echo(
                    f"Error: Unsupported broker: {broker}",
                    err=True,
                )
                sys.exit(1)

        market_type = MarketType(item["market_type"])

        start_date = item["start_date"]

        end_date = (
            item["end_date"]
            if item.get("end_date") is not None
            else None
        )

        symbol = item["symbol"]

        subscriptions.append(
            PollSubscription(
                broker=broker,
                symbol=symbol,
                market_type=market_type,
                timeframes=[Timeframe(tf) for tf in item["timeframes"]],
                start_date=start_date,
                end_date=end_date,
            )
        )

    poller = OHLCPoller(
        loaders=loaders,
        subscriptions=subscriptions,
        poll_interval=poll_interval,
    )

    health_server = HealthCheckServer(port=health_port)

    async def run():
        try:
            await asyncio.gather(
                poller.run(),
                health_server.run_forever(),
            )
        finally:
            await asyncio.gather(
                health_server.stop(),
                *[loader.close() for loader in loaders.values()],
                return_exceptions=True,
            )

    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        click.echo("Poller stopped")
        sys.exit(0)


def handle_single_load(
    broker: BrokerType,
    symbol: str,
    market_type: MarketType,
    timeframe: Timeframe,
    api_key: str,
    secret_key: str,
    start_date: date,
    end_date: date | None = None,
):
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
                timeframes=[timeframe],
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


@loader.command(name="run")
@click.option(
    "--broker",
    type=EnumParam(BrokerType),
    required=False,
    help=f"Broker to load data from ({', '.join(BrokerType._value2member_map_.keys())})",
)
@click.option(
    "--symbol",
    required=False,
    help="Trading symbol (e.g., AAPL)",
)
@click.option(
    "--market-type",
    type=EnumParam(MarketType),
    required=False,
    help=f"Market type ({', '.join(MarketType._value2member_map_.keys())})",
)
@click.option(
    "--timeframe",
    type=EnumParam(Timeframe),
    required=False,
    help=f"Candle timeframe ({', '.join(Timeframe._value2member_map_.keys())})",
)
@click.option(
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    required=False,
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
@click.option(
    "--poll-interval",
    type=int,
    required=False,
    default=0,
)
@click.option(
    "--file",
    type=click.Path(exists=True, dir_okay=False, file_okay=True, path_type=str),
    required=False,
    default=0,
)
@click.option("--health-port", type=int, default=5555, help="Health check server port")
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
    poll_interval,
    file,
    health_port,
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

    if poll_interval > 0:
        if file is None:
            click.echo(
                "Error: --file must be provided if --poll-interval is specified",
                err=True,
            )
            sys.exit(1)

        return handle_poll_loaders(file, poll_interval, health_port)
    
    for var, flag in (
        (broker, "--broker"),
        (market_type, "--market-type"),
        (timeframe, "--timeframe"),
        (start_date, "--start-date"),
    ):
        if var is None:
            click.echo(f"Error: {flag} must be provided", err=True)
            sys.exit(1)

    if not api_key or not secret_key:
        click.echo(
            "Error: --api-key and --secret-key are required for Alpaca broker",
            err=True,
        )
        sys.exit(1)

    return handle_single_load(
        broker=broker,
        symbol=symbol,
        market_type=market_type,
        timeframe=timeframe,
        api_key=api_key,
        secret_key=secret_key,
        start_date=start_date,
        end_date=end_date,
    )


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
@click.option("--host", type=str, default="localhost", help="Server host")
@click.option("--port", type=int, default=8001, help="Server port")
@click.option("--health-port", type=int, default=5555, help="Health check server port")
@click.option(
    "--file",
    type=click.Path(exists=True, dir_okay=False, file_okay=True, path_type=str),
    required=True,
    default=0,
)
def feed_run(host, port, health_port, file):
    yamloader = YamlLoader(file)
    instruments_config = yamloader.load()

    # Group instruments by (broker, market_type) so we create one feed per group.
    groups: dict[
        tuple[BrokerType, MarketType],
        tuple[str, str, list[tuple[str, list[Timeframe]]]],
    ] = {}
    for item in instruments_config:
        broker = BrokerType(item["broker"])
        market_type = MarketType(item["market_type"])
        symbol = item["symbol"]
        
        tfs = [Timeframe(tf) for tf in item["timeframes"]]

        key = (broker, market_type)

        if key not in groups:
            groups[key] = (item["api_key"], item["secret_key"], [])

        groups[key][2].append((symbol, tfs))

    feeds: list[OHLCFeed] = []
    for (broker, market_type), (api_key, secret_key, instruments) in groups.items():
        if broker == BrokerType.ALPACA:
            feed = AlpacaOHLCFeed(
                market_type=market_type,
                instruments=instruments,
                api_key=api_key,
                secret_key=secret_key,
            )
        else:
            raise ValueError(f"Unsupported broker type '{broker}'")
    
        feeds.append(feed)

    feed_manager = FeedManager()
    server = OHLCFeedServer(feed_manager, host, port)
    health_server = HealthCheckServer(host=host, port=health_port)

    async def _run():
        try:
            await server.init(feeds)
            await asyncio.gather(
                *[feed.run() for feed in feeds],
                server.run(),
                health_server.run_forever(),
            )
        except KeyboardInterrupt:
            pass
        finally:
            await server.stop()
            for feed in feeds:
                await feed.stop()

    asyncio.run(_run())
