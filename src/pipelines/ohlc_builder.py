import json
import logging
from collections import defaultdict
from datetime import datetime
from decimal import Decimal

from pydantic import ValidationError

from config import REDIS_BROKER_TRADE_EVENTS_KEY, REDIS_CANDLE_CLOSE_EVENTS_KEY
from core.events import BrokerTradeEvent, CandleCloseEvent
from core.models import OHLCV
from db_models import OHLCs
from engine.enums import BrokerType, Timeframe
from utils.db import get_db_sess
from utils.redis import REDIS_CLIENT


class OHLCBuilder:
    """
    Listens to trade events from Redis pub/sub and builds OHLC objects for each timeframe.

    This class provides a reliability layer by:
    - Building OHLC data from raw trade events
    - Maintaining state for all timeframes independently
    - Handling missed or delayed trade events gracefully
    - Persisting OHLC state to Redis for recovery
    """

    def __init__(self):
        # Broker => Symbol => Timeframe => OHLCV
        self._current_ohlc: dict[
            BrokerType, dict[str, dict[Timeframe, OHLCV | None]]
        ] = defaultdict(
            lambda: defaultdict(
                lambda: {tf: None for tf in Timeframe._member_map_.values()}
            )
        )
        self._pubsub = None
        self._running = False
        self._logger = logging.getLogger(type(self).__name__)

    async def start(self):
        """Start listening to trade events and building OHLC data."""
        if self._running:
            self._logger.warning("OHLCBuilder is already running")
            return

        self._running = True
        self._logger.info("Starting OHLCBuilder")

        # Restore previous state from Redis
        await self._restore_state()

        # Start listening to trade events
        self._pubsub = REDIS_CLIENT.pubsub()
        await self._pubsub.subscribe(REDIS_BROKER_TRADE_EVENTS_KEY)
        self._logger.info(f"Subscribed to {REDIS_BROKER_TRADE_EVENTS_KEY}")

        try:
            async for message in self._pubsub.listen():
                if not self._running:
                    break

                if message["type"] != "message":
                    continue

                try:
                    event_data = json.loads(message["data"])
                    trade_event = BrokerTradeEvent(**event_data)
                    await self._process_trade(trade_event)
                except (json.JSONDecodeError, ValidationError) as e:
                    self._logger.error(f"Failed to parse trade event: {e}")
                    continue
                except Exception as e:
                    self._logger.exception(f"Error processing trade event: {e}")
                    continue

        finally:
            await self.stop()

    async def stop(self):
        """Stop the OHLC builder and clean up resources."""
        if not self._running:
            return

        self._running = False
        self._logger.info("Stopping OHLCBuilder")

        if self._pubsub:
            await self._pubsub.unsubscribe()
            await self._pubsub.aclose()
            self._pubsub = None

        self._logger.info("OHLCBuilder stopped")

    async def _process_trade(self, trade: BrokerTradeEvent):
        """
        Process a single trade event and update OHLC data for all timeframes.

        Args:
            trade: The trade event to process
        """
        broker = trade.broker
        symbol = trade.symbol
        trade_time = datetime.fromtimestamp(trade.timestamp)
        trade_price = trade.price
        trade_volume = trade.quantity

        self._logger.debug(
            f"Processing trade: {symbol} @ {trade_price} "
            f"at {trade_time} ({broker.value})"
        )

        ohlc_dict = self._current_ohlc[broker][symbol]

        # Process each timeframe
        for timeframe in Timeframe._member_map_.values():
            current_ohlc = ohlc_dict[timeframe]
            tf_seconds = timeframe.get_seconds()

            # Calculate the candle start time for this trade
            candle_start_ts = (trade.timestamp // tf_seconds) * tf_seconds
            candle_start_time = datetime.fromtimestamp(candle_start_ts)

            # If no current OHLC exists, create one
            if current_ohlc is None:
                ohlc_dict[timeframe] = OHLCV(
                    symbol=symbol,
                    timestamp=candle_start_time,
                    open=trade_price,
                    high=trade_price,
                    low=trade_price,
                    close=trade_price,
                    volume=trade_volume,
                    timeframe=timeframe,
                )
                self._logger.debug(
                    f"Created new OHLC for {symbol} {timeframe.value} "
                    f"at {candle_start_time}"
                )

                await self._persist_ohlc(
                    broker, symbol, timeframe, ohlc_dict[timeframe]
                )
                continue

            # Check if trade belongs to current candle or next candle
            time_diff = candle_start_time - current_ohlc.timestamp

            if time_diff.total_seconds() == 0:
                current_ohlc.close = trade_price
                current_ohlc.high = max(current_ohlc.high, trade_price)
                current_ohlc.low = min(current_ohlc.low, trade_price)
                current_ohlc.volume += trade_volume

                self._logger.debug(
                    f"Updated OHLC for {symbol} {timeframe.value}: "
                    f"H={current_ohlc.high}, L={current_ohlc.low}, "
                    f"C={current_ohlc.close}, V={current_ohlc.volume}"
                )

                await self._persist_ohlc(broker, symbol, timeframe, current_ohlc)

            elif time_diff.total_seconds() > 0:
                self._logger.info(
                    f"Closing OHLC for {symbol} {timeframe.value} "
                    f"at {current_ohlc.timestamp}, starting new candle at {candle_start_time}"
                )

                # Emit candle close event for the completed candle
                await self._emit_candle_close(broker, current_ohlc)

                ohlc_dict[timeframe] = OHLCV(
                    symbol=symbol,
                    timestamp=candle_start_time,
                    open=trade_price,
                    high=trade_price,
                    low=trade_price,
                    close=trade_price,
                    volume=trade_volume,
                    timeframe=timeframe,
                )

                await self._persist_ohlc(
                    broker, symbol, timeframe, ohlc_dict[timeframe]
                )

            else:
                self._logger.warning(
                    f"Received out-of-order trade for {symbol} {timeframe.value}: "
                    f"trade time {trade_time} < current candle {current_ohlc.timestamp}"
                )

    async def _emit_candle_close(self, broker: BrokerType, ohlc: OHLCV):
        """
        Emit a candle close event to Redis pub/sub and persist to database.

        Args:
            broker: The broker type
            ohlc: The completed OHLC candle
        """
        # Persist to database
        await self._persist_to_database(broker, ohlc)

        # Emit event to Redis
        event = CandleCloseEvent(
            broker=broker,
            symbol=ohlc.symbol,
            timeframe=ohlc.timeframe,
            timestamp=ohlc.timestamp.isoformat(),
            open=ohlc.open,
            high=ohlc.high,
            low=ohlc.low,
            close=ohlc.close,
            volume=ohlc.volume,
        )

        await REDIS_CLIENT.publish(
            REDIS_CANDLE_CLOSE_EVENTS_KEY, event.model_dump_json()
        )

        self._logger.info(
            f"Emitted candle close event: {ohlc.symbol} {ohlc.timeframe.value} "
            f"at {ohlc.timestamp} (O={ohlc.open}, H={ohlc.high}, L={ohlc.low}, "
            f"C={ohlc.close}, V={ohlc.volume})"
        )

    async def _persist_to_database(self, broker: BrokerType, ohlc: OHLCV):
        """
        Persist completed OHLC data to the database.

        Args:
            broker: The broker type
            ohlc: The completed OHLC candle to persist
        """
        try:
            async with get_db_sess() as session:
                db_ohlc = OHLCs(
                    source=broker.value,
                    symbol=ohlc.symbol,
                    open=ohlc.open,
                    high=ohlc.high,
                    low=ohlc.low,
                    close=ohlc.close,
                    timeframe=ohlc.timeframe,
                    timestamp=int(ohlc.timestamp.timestamp()),
                )
                session.add(db_ohlc)
                await session.commit()

                self._logger.debug(
                    f"Persisted OHLC to database: {ohlc.symbol} {ohlc.timeframe.value} "
                    f"at {ohlc.timestamp} ({broker.value})"
                )
        except Exception as e:
            self._logger.error(
                f"Failed to persist OHLC to database for {ohlc.symbol} "
                f"{ohlc.timeframe.value}: {e}"
            )

    async def _persist_ohlc(
        self, broker: BrokerType, symbol: str, timeframe: Timeframe, ohlc: OHLCV
    ):
        """
        Persist OHLC data to Redis for recovery.

        Args:
            broker: The broker type
            symbol: The trading symbol
            timeframe: The timeframe
            ohlc: The OHLC object to persist
        """
        redis_key = self._get_redis_key(broker, symbol, timeframe)
        await REDIS_CLIENT.set(redis_key, ohlc.model_dump_json())

    async def _restore_state(self):
        """Restore OHLC state from Redis after restart."""
        self._logger.info("Restoring OHLC state from Redis")
        count = 0

        async for key in REDIS_CLIENT.scan_iter("ohlc:*"):
            try:
                data = await REDIS_CLIENT.get(key)
                if not data:
                    continue

                ohlc_data = json.loads(data)

                # Parse the Redis key: ohlc:broker:symbol:timeframe
                _, broker_str, symbol, timeframe_str = key.decode().split(":", 3)

                broker = BrokerType(broker_str)
                timeframe = Timeframe(timeframe_str)

                # Reconstruct OHLCV object
                ohlc = OHLCV(
                    symbol=ohlc_data["symbol"],
                    timestamp=datetime.fromisoformat(ohlc_data["timestamp"]),
                    open=ohlc_data["open"],
                    high=ohlc_data["high"],
                    low=ohlc_data["low"],
                    close=ohlc_data["close"],
                    volume=ohlc_data["volume"],
                    timeframe=timeframe,
                )

                self._current_ohlc[broker][symbol][timeframe] = ohlc
                count += 1

                self._logger.debug(
                    f"Restored OHLC: {symbol} {timeframe.value} "
                    f"at {ohlc.timestamp} ({broker.value})"
                )

            except Exception as e:
                self._logger.error(f"Failed to restore OHLC from key {key}: {e}")
                continue

        self._logger.info(f"Restored {count} OHLC candles from Redis")

    def get_current_ohlc(
        self, broker: BrokerType, symbol: str, timeframe: Timeframe
    ) -> OHLCV | None:
        """
        Get the current OHLC for a specific broker, symbol, and timeframe.

        Args:
            broker: The broker type
            symbol: The trading symbol
            timeframe: The timeframe

        Returns:
            The current OHLC object, or None if not available
        """
        return self._current_ohlc[broker][symbol].get(timeframe)

    def get_all_ohlc_for_symbol(
        self, broker: BrokerType, symbol: str
    ) -> dict[Timeframe, OHLCV | None]:
        """
        Get all OHLC data for a symbol across all timeframes.

        Args:
            broker: The broker type
            symbol: The trading symbol

        Returns:
            Dictionary mapping timeframes to OHLC objects
        """
        return self._current_ohlc[broker][symbol].copy()

    @staticmethod
    def _get_redis_key(broker: BrokerType, symbol: str, timeframe: Timeframe) -> str:
        """Generate Redis key for storing OHLC data."""
        return f"ohlc:{broker.value}:{symbol}:{timeframe.value}"
