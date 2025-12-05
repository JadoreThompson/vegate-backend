from enum import Enum


class PricingTierType(str, Enum):
    FREE = "free"
    PRO = "pro"
    ENTERPRISE = "enterprise"
