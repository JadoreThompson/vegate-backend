from uuid import UUID

from core.schema import CustomBaseModel


class JWTPayload(CustomBaseModel):
    sub: UUID
    em: str
    exp: int
