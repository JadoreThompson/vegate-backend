import logging
from decimal import Decimal
from typing import Optional

from engine.enums import BrokerType
from infra.redis import REDIS_CLIENT


class PriceService:
    """
    Service for managing current market prices in Redis.
    """

    _logger = logging.getLogger("PriceService")

    @staticmethod
    def _get_price_key(broker: BrokerType, symbol: str) -> str:
        """
        Generate Redis key for storing current price.

        Args:
            broker: The broker type
            symbol: The trading symbol

        Returns:
            Redis key string in format "price:broker:symbol"
        """
        return f"price:{broker.value}:{symbol}"

    @classmethod
    async def set_price(cls, broker: BrokerType, symbol: str, price: float) -> None:
        """
        Set the current price for a broker/symbol pair.

        This is typically called by listeners when they receive trade events.

        Args:
            broker: The broker type
            symbol: The trading symbol
            price: The current price (float)

        Example:
            await PriceService.set_price(BrokerType.ALPACA, "AAPL", 150.25)
        """
        key = cls._get_price_key(broker, symbol)
        await REDIS_CLIENT.set(key, str(price))

        cls._logger.debug(f"Set price for {broker.value}:{symbol} = {price}")

    @classmethod
    async def get_price(cls, broker: BrokerType, symbol: str) -> Optional[Decimal]:
        """
        Get the current price for a broker/symbol pair.

        Args:
            broker: The broker type
            symbol: The trading symbol

        Returns:
            Current price as Decimal, or None if not available

        Example:
            price = await PriceService.get_price(BrokerType.ALPACA, "AAPL")
            if price:
                print(f"Current AAPL price: ${price}")
        """
        key = cls._get_price_key(broker, symbol)

        price_str = await REDIS_CLIENT.get(key)

        if price_str is None:
            cls._logger.debug(f"No price found for {broker.value}:{symbol}")
            return None

        try:
            price = float(price_str)
            return price
        except (ValueError, TypeError) as e:
            cls._logger.error(
                f"Failed to parse price for {broker.value}:{symbol}: "
                f"{price_str} - {e}"
            )
            return None

    @classmethod
    async def delete_price(cls, broker: BrokerType, symbol: str) -> bool:
        """
        Delete the price for a broker/symbol pair.

        Args:
            broker: The broker type
            symbol: The trading symbol

        Returns:
            True if price was deleted, False if it didn't exist
        """
        key = cls._get_price_key(broker, symbol)
        result = await REDIS_CLIENT.delete(key)

        deleted = result > 0
        if deleted:
            cls._logger.debug(f"Deleted price for {broker.value}:{symbol}")

        return deleted
