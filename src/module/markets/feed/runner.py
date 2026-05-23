import asyncio
from datetime import datetime, timedelta

from config import ALPACA_API_KEY, ALPACA_SECRET_KEY, CONFIG_YAML

from module.broker.enums import BrokerType
from .base import OHLCFeed
from .alpaca import AlpacaOHLCFeed
from .server import OHLCFeedServer
from ..enums import MarketType, Timeframe
from ..loader.alpaca import AlpacaOHLCLoader


class OHLCFeedRunner:
    """
    Launches the market feed server
    """

    def __init__(self, host: str, port: int):
        super().__init__()
        self._host = host
        self._port = port

    def run(self):
        asyncio.run(self._run())

    async def _run(self):
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
                asyncio.create_task(self._wrapper(feed.run()))
                feeds.append(feed)

        server = OHLCFeedServer(self._host, self._port)
        try:
            await server.init(feeds)
            await server.run()
        except KeyboardInterrupt:
            pass
        finally:
            await server.stop()
            for feed in feeds:
                await feed.stop()

    async def _wrapper(self, coro):
        try:
            await coro
        except asyncio.CancelledError:
            pass
