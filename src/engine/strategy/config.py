from engine.enums import Timeframe
from core.models import CustomBaseModel


class StrategyConfig(CustomBaseModel):
    timeframe: Timeframe
    symbol: str
