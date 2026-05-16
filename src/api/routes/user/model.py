from enums import PricingTierType
from models import CustomBaseModel


class UserResponse(CustomBaseModel):
    username: str
    pricing_tier: PricingTierType
