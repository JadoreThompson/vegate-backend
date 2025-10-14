from core.models import CustomBaseModel


class UserCreate(CustomBaseModel):
    username: str
    password: str


class UserLogin(CustomBaseModel):
    username: str | None = None
    password: str
