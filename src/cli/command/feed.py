import asyncio
from datetime import datetime, timedelta
import sys
import logging

import click

from config import ALPACA_API_KEY, ALPACA_SECRET_KEY, CONFIG_YAML
from module.broker.enums import BrokerType
from module.markets.enums import MarketType, Timeframe
from module.markets.feed import OHLCFeed, OHLCFeedServer
from module.markets.feed.alpaca import AlpacaOHLCFeed
from module.markets.feed.manager import FeedManager
from module.markets.loader.alpaca import AlpacaOHLCLoader


logger = logging.getLogger("commands.backtest")


@click.group()
def feed():
    """OHLC Feed Server."""
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