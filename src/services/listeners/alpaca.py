import asyncio
from functools import partial
import logging

from alpaca.data.live import CryptoDataStream, StockDataStream
from alpaca.data.models import Trade

from config import ALPACA_API_KEY, ALPACA_SECRET_KEY, REDIS_BROKER_TRADE_EVENTS_KEY
from core.events import BrokerTradeEvent
from engine.enums import BrokerType, MarketType
from services.price_service import PriceService
from utils.redis import REDIS_CLIENT


class AlpacaListener:
    def __init__(self):
        self._crypto_stream_client: CryptoDataStream | None = None
        self._stock_stream_client: StockDataStream | None = None
        self._logger= logging.getLogger(type(self).__name__)

    def initialise(self):
        print(1)
        self._crypto_stream_client = self._crypto_stream_client or CryptoDataStream(
            api_key=ALPACA_API_KEY, secret_key=ALPACA_SECRET_KEY
        )
        self._stock_stream_client = self._stock_stream_client or StockDataStream(
            api_key=ALPACA_API_KEY, secret_key=ALPACA_SECRET_KEY
        )
        print(2)

    async def run(self):
        try:
            await asyncio.gather(
                self._crypto_stream_client._run_forever(),
                self._stock_stream_client._run_forever(),
            )
        except Exception as e:
            self._logger.error("An error occured running the clients", exc_info=e)

    def listen(self, market_type: MarketType, symbols: list[str]):
        # if market_type == MarketType.CRYPTO:
        #     client = self._crypto_stream_client
        #     handler = partial(self._handle_trade, MarketType.CRYPTO)

        # elif market_type == MarketType.STOCKS:
        #     client = self._stock_stream_client
        #     handler = partial(self._handle_trade, MarketType.STOCKS)

        # client.subscribe_trades(handler, *list(set(symbols)))

        client_map = {
            MarketType.CRYPTO: self._crypto_stream_client,
            MarketType.STOCKS: self._stock_stream_client,
        }

        client = client_map[market_type]
        handler = partial(self._handle_trade, market_type)
        client.subscribe_trades(handler, *set(symbols))

    async def _handle_trade(self, market_type: MarketType, trade: Trade):
        event = BrokerTradeEvent(
            broker=BrokerType.ALPACA,
            market_type=market_type,
            symbol=trade.symbol,
            quantity=trade.size,
            price=trade.price,
            timestamp=int(trade.timestamp.timestamp()),
        )
        dumped_event = event.model_dump_json()

        # Publish trade event for OHLC building
        await REDIS_CLIENT.publish(REDIS_BROKER_TRADE_EVENTS_KEY, dumped_event)

        # Set current price using PriceService
        await PriceService.set_price(event.broker, event.symbol, event.price)
