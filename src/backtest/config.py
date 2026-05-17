from datetime import datetime

from enums import MarketType, Timeframe, BrokerType
from strategy.model import StrategyConfig


class BacktestConfig(StrategyConfig):

    def __init__(
            self,
            symbol: str,
            market_type: MarketType,
            timeframe: Timeframe,
            broker: BrokerType,
            starting_balance: float,
            start_date: datetime,
            end_date: datetime,
    ):
        super().__init__(symbol, market_type, timeframe)
        self.starting_balance = starting_balance
        self.start_date = start_date
        self.end_date = end_date
        self.broker = broker
