from pydantic import BaseModel, field_validator, EmailStr, model_validator


class PasswordField(BaseModel):
    password: str

    @field_validator("password", mode="before")
    def password_validator(cls, value: str) -> str:
        min_length = 8
        min_special_chars = 2
        min_uppercase = 2

        if len(value) < min_length:
            raise ValueError(f"Password must be at least {min_length} characters long.")

        if sum(1 for c in value if c.isupper()) < min_uppercase:
            raise ValueError(
                f"Password must contain at least {min_uppercase} uppercase letters.",
            )

        if sum(1 for c in value if not c.isalnum()) < min_special_chars:
            raise ValueError(
                f"Password must contain at least {min_special_chars} special characters."
            )

        return value


class EmailField(BaseModel):
    email: EmailStr


class RegisterUserRequest(PasswordField, EmailField):
    username: str


class LoginUserRequest(BaseModel):
    username: str | None = None
    email: EmailStr | None = None
    password: str

    @model_validator(mode="after")
    def verify_username_email(self):
        if (self.username is None or not self.username.strip()) and (
            self.email is None or not self.email.strip()
        ):
            raise ValueError("Either username or email must be provided.")
        return self


class VerificationCode(BaseModel):
    code: str


class ResetPasswordRequest(BaseModel):
    email: str


class ResetPasswordVerificationRequest(PasswordField):
    code: str
    password: str


class ResetPasswordResponse(BaseModel):
    message: str = "A verification email has been sent to your email"


class ChangeEmailRequest(EmailField):
    pass


class ChangeUsernameRequest(BaseModel):
    username: str


class ChangePasswordRequest(PasswordField):
    pass


class EmailVerificationRequest(EmailField):
    pass
