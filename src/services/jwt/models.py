from uuid import UUID

from enums import PricingTierType
from models import CustomBaseModel


class JWTPayload(CustomBaseModel):
    sub: UUID
    em: str
    exp: int
    pricing_tier: PricingTierType
    authenticated: bool
