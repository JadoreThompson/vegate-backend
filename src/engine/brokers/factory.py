import logging
from typing import Literal, Type, Any

from .base import BaseBroker
from .exc import BrokerError


logger = logging.getLogger(__name__)


BrokerNameT = Literal["alpaca"]


class BrokerFactory:
    """
    Factory for creating broker instances using the registry pattern.

    This factory maintains a registry of available broker implementations
    and creates instances based on a broker name and credentials.

    New broker implementations can be registered at runtime, making the
    system easily extensible without modifying existing code.

    Attributes:
        _brokers: Registry mapping broker names to broker classes

    Example:
        # Register a broker
        BrokerFactory.register('alpaca', AlpacaBroker)

        # Create an instance
        broker = BrokerFactory.create('alpaca', credentials)

        # Use the broker
        with broker:
            broker.submit_order(order)
    """

    _brokers: dict[BrokerNameT, Type[BaseBroker]] = {
        'alpaca': 
    }

