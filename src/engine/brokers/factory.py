import logging
from typing import Literal, Type

from .base import BaseBroker
from .alpaca import AlpacaBroker


logger = logging.getLogger(__name__)


BrokerNameT = Literal["alpaca"]


class BrokerFactory:
    _brokers: dict[BrokerNameT, Type[BaseBroker]] = {
        "alpaca": AlpacaBroker(),
    }
