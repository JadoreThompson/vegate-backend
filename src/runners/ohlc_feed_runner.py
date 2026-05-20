import asyncio

from config import ALPACA_API_KEY, ALPACA_SECRET_KEY, CONFIG_YAML
from enums import BrokerType, MarketType, Timeframe
from runners.base import BaseRunner
from service.ohlc.feed.alpaca.service import AlpacaOHLCFeed
from service.ohlc.feed.base import OHLCFeed
from service.ohlc.feed.server import OHLCFeedServer


class OHLCFeedRunner(BaseRunner):
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
                        market_type=market_type,
                        symbol=symbol,
                        timeframe=timeframe,
                        api_key=ALPACA_API_KEY,
                        secret_key=ALPACA_SECRET_KEY,
                        start_date=item["start_date"],
                    )
                else:
                    raise ValueError(f"Unsupported broker type '{broker}'")

                # await feed.start()
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
