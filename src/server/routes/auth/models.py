from typing import Literal

from pydantic import BaseModel, field_validator

from core.models import CustomBaseModel
from server.exc import CustomValidationError


class PasswordField(BaseModel):
    password: str

    @field_validator("password", mode="before")
    def password_validator(cls, value: str) -> str:
        min_length = 8
        min_special_chars = 2
        min_uppercase = 2
        status = 400

        if len(value) < min_length:
            raise CustomValidationError(
                status, f"Password must be at least {min_length} characters long."
            )

        if sum(1 for c in value if c.isupper()) < min_uppercase:
            raise CustomValidationError(
                status,
                f"Password must contain at least {min_uppercase} uppercase letters.",
            )

        if sum(1 for c in value if not c.isalnum()) < min_special_chars:
            raise CustomValidationError(
                status,
                f"Password must contain at least {min_special_chars} special characters.",
            )

        return value


class UserCreate(PasswordField):
    username: str
    email: str


class UserLogin(CustomBaseModel):
    username: str | None = None
    email: str | None = None
    password: str


class UserConnection(BaseModel):
    username: str
    avatar: str


class UserMe(CustomBaseModel):
    username: str


class UpdateUsername(BaseModel):
    username: str


class UpdatePassword(PasswordField):
    pass


class VerifyCode(BaseModel):
    code: str


class VerifyAction(VerifyCode):
    action: Literal["change_username", "change_password"]
