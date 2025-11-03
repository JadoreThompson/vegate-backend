from dataclasses import dataclass
from datetime import datetime
from uuid import UUID

from core.enums import PricingTierType


@dataclass
class JWTPayload:
    sub: UUID
    em: str
    exp: datetime
    pricing_tier: PricingTierType
    authenticated: bool
