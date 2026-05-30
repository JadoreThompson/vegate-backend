import json
import random
import string
from typing import Type
from uuid import UUID

from argon2 import PasswordHasher
from argon2.exceptions import Argon2Error
from redis.asyncio import Redis as AsyncRedis
from sqlalchemy import insert, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from config import (
    FRONTEND_DOMAIN,
    FRONTEND_SUB_DOMAIN,
    PW_HASH_SALT,
    REDIS_CHANGE_EMAIL_KEY_PREFIX,
    REDIS_CHANGE_PASSWORD_KEY_PREFIX,
    REDIS_CHANGE_USERNAME_KEY_PREFIX,
    REDIS_EMAIL_VERIFCATION_EXPIRY_SECS,
    REDIS_EMAIL_VERIFICATION_KEY_PREFIX,
    REDIS_PASSWORD_RESET_EXPIRY_SECS,
    REDIS_PASSWORD_RESET_TOKEN_KEY_PREFIX,
    SCHEME,
    VERIFICATION_CODE_EXPIRY_SECS,
)
from core.redis import REDIS_CLIENT
from module.email import EmailService
from module.user.model import User
from util import get_datetime
from .exception import (
    InvalidCredentialsException,
    UserAlreadyExistsException,
    UserDoesNotExistException,
    UserNotAuthenticatedException,
)
from .schema import (
    ChangeEmailRequest,
    ChangePasswordRequest,
    ChangeUsernameRequest,
    RegisterUserRequest,
    LoginUserRequest,
    ResetPasswordRequest,
    ResetPasswordResponse,
    ResetPasswordVerificationRequest,
    VerificationCode,
)


class AuthService:

    def __init__(
        self,
        email_service_cls: Type[EmailService],
        redis_client: AsyncRedis = REDIS_CLIENT,
        email_verification_key_prefix: str = REDIS_EMAIL_VERIFICATION_KEY_PREFIX,
        email_verification_expiry: int = REDIS_EMAIL_VERIFCATION_EXPIRY_SECS,
    ):
        """
        Initializes the authentication service.

        Args:
            email_service_cls: Email service class used to send emails.
            redis_client: Redis client used for verification code storage.
            email_verification_key_prefix: Prefix for Redis keys storing verification codes.
            email_verification_expiry: Expiry time (seconds) for verification codes.
        """
        self._email_service = email_service_cls("Vegate", "no-reply@vegate.jadore.dev")
        self._redis_client = redis_client
        self._email_verification_key_prefix = email_verification_key_prefix
        self._email_verification_expiry = email_verification_expiry
        self._pw_hasher = PasswordHasher()

    async def register_user(
        self, request: RegisterUserRequest, db_sess: AsyncSession
    ) -> User:
        """
        Registers a new user and sends an email verification code.

        Args:
            request: User registration payload containing username, email, and password.
            db_sess: Active SQLAlchemy async database session.

        Returns:
            The created User object.

        Raises:
            UserAlreadyExistsException: If a user with the same username or email already exists.
        """
        res = await db_sess.execute(
            select(User.user_id).where(
                or_(User.username == request.username, User.email == request.email)
            )
        )

        if res.first():
            raise UserAlreadyExistsException()

        hashed_pw = self.hash_password(request.password)

        user = await db_sess.scalar(
            insert(User)
            .values(
                username=request.username,
                email=request.email,
                password=hashed_pw,
            )
            .returning(User)
        )

        await self._send_verification_code(request.email, user.user_id)
        return user

    async def _send_verification_code(self, email: str, user_id: UUID):
        """
        Generates and sends an email verification code.

        Stores the code in Redis with an expiration time and emails it to the user.

        Args:
            email: Recipient email address.
            user_id: Id of the user to associate with the verification code.
        """
        code = self.gen_verification_code()
        key = f"{self._email_verification_key_prefix}{user_id}"

        await self._redis_client.set(key, code, ex=self._email_verification_expiry)

        await self._email_service.send_email(
            email, "Verify your email", f"Your verification code is: {code}"
        )

    async def request_email_verification(self, user_id: UUID, db_sess: AsyncSession):
        user = await db_sess.scalar(select(User).where(User.user_id == user_id))

        if user is None:
            raise UserDoesNotExistException()

        if user.authenticated_at is not None:
            raise ValueError("User already authenticated.")

        await self._send_verification_code(email=user.email, user_id=user_id)

    async def verify_email(
        self, request: VerificationCode, user_id: UUID, db_sess: AsyncSession
    ) -> User:
        """
        Verifies a user's email using a code stored in Redis.

        Args:
            request: Verification code submitted by the user.
            user_id: Id of the user being verified.
            db_sess: Active SQLAlchemy async database session.

        Returns:
            The updated User object with verification timestamp set.

        Raises:
            ValueError: If the verification code is invalid or expired.
        """
        key = f"{self._email_verification_key_prefix}{user_id}"
        code: bytes | None = await self._redis_client.get(key)

        if code is None or code.decode() != request.code:
            raise ValueError("Invalid or expired verification code.")

        await self._redis_client.delete(key)

        user = await db_sess.scalar(select(User).where(User.user_id == user_id))
        user.authenticated_at = get_datetime()
        return user

    async def authenticate_user(
        self, request: LoginUserRequest, db_sess: AsyncSession
    ) -> User:
        """
        Authenticates a user using username/email and password.

        Args:
            request: Login request containing credentials.
            db_sess: Active SQLAlchemy async database session.

        Returns:
            The authenticated User object.
        """
        query = select(User)

        if request.username is not None:
            query = query.where(User.username == request.username)

        if request.email is not None:
            query = query.where(User.email == request.email)

        user = await db_sess.scalar(query)

        if user is None:
            raise InvalidCredentialsException()

        if not self.verify_password(request.password, user.password):
            raise InvalidCredentialsException()

        if not user.authenticated_at:
            await self._send_verification_code(user.email, user.user_id)
            raise UserNotAuthenticatedException()

        return user

    async def request_email_change(
        self, request: ChangeEmailRequest, user_id: UUID, db_sess: AsyncSession
    ):
        """
        Initiates an email change request for a user.
        """
        user = await db_sess.scalar(select(User).where(User.user_id == user_id))
        if user is None:
            raise UserDoesNotExistException()

        existing_user = await db_sess.scalar(
            select(User).where(User.username == request.email)
        )
        if existing_user:
            raise UserAlreadyExistsException("Email already exists")

        key = f"{REDIS_CHANGE_EMAIL_KEY_PREFIX}{user_id}"
        code = self.gen_verification_code()
        payload = json.dumps({"code": code, "email": request.email})

        await self._redis_client.set(key, payload, ex=VERIFICATION_CODE_EXPIRY_SECS)

        await self._email_service.send_email(
            user.email,
            "Confirm Your Email Change",
            f"Your verification code is: {code}",
        )

    async def verify_email_change(
        self, request: VerificationCode, user_id: UUID, db_sess: AsyncSession
    ):
        key = f"{REDIS_CHANGE_EMAIL_KEY_PREFIX}{user_id}"
        data = await self._redis_client.get(key)

        if data is None:
            raise ValueError("Invalid or expired token")

        payload = json.loads(data)
        if payload["code"] != request.code:
            raise ValueError("Invalid or expired token")

        user = await db_sess.get(User, user_id)
        user.email = payload["email"]
        return user

    async def request_username_change(
        self, request: ChangeUsernameRequest, user_id: UUID, db_sess: AsyncSession
    ):
        """
        Initiates an username change request for a user.
        """
        user = await db_sess.scalar(select(User).where(User.user_id == user_id))
        if user is None:
            raise UserDoesNotExistException()

        existing_user = await db_sess.scalar(
            select(User).where(User.username == request.username)
        )
        if existing_user:
            raise UserAlreadyExistsException(
                f"User with username '{request.username}' already exists"
            )

        key = f"{REDIS_CHANGE_USERNAME_KEY_PREFIX}{user_id}"
        code = self.gen_verification_code()
        payload = json.dumps({"code": code, "username": request.username})

        await self._redis_client.set(key, payload, ex=VERIFICATION_CODE_EXPIRY_SECS)

        await self._email_service.send_email(
            user.email,
            "Confirm Your Username Change",
            f"Your verification code is: {code}",
        )

    async def verify_username_change(
        self, request: VerificationCode, user_id: UUID, db_sess: AsyncSession
    ) -> User:
        key = f"{REDIS_CHANGE_USERNAME_KEY_PREFIX}{user_id}"
        data = await self._redis_client.get(key)

        if data is None:
            raise ValueError("Invalid or expired token")

        payload = json.loads(data)
        if payload["code"] != request.code:
            raise ValueError("Invalid or expired token")

        user = await db_sess.get(User, user_id)
        user.username = payload["username"]
        return user

    async def request_password_change(
        self, request: ChangePasswordRequest, user_id: UUID, db_sess: AsyncSession
    ):
        """
        Initiates an username change request for a user.
        """
        user = await db_sess.get(User, user_id)
        if user is None:
            raise UserDoesNotExistException()

        key = f"{REDIS_CHANGE_PASSWORD_KEY_PREFIX}{user_id}"
        code = self.gen_verification_code()
        payload = json.dumps({"code": code, "password": request.password})

        await self._redis_client.set(key, payload, ex=VERIFICATION_CODE_EXPIRY_SECS)

        await self._email_service.send_email(
            user.email,
            "Confirm Your Password Change",
            f"Your verification code is: {code}",
        )

    async def verify_password_change(
        self, request: VerificationCode, user_id: UUID, db_sess: AsyncSession
    ) -> User:
        key = f"{REDIS_CHANGE_PASSWORD_KEY_PREFIX}{user_id}"
        data = await self._redis_client.get(key)

        if data is None:
            raise ValueError("Invalid or expired token")

        payload = json.loads(data)
        if payload["code"] != request.code:
            raise ValueError("Invalid or expired token")

        user = await db_sess.get(User, user_id)
        user.password = payload["password"]
        return user

    async def request_reset_password(
        self, request: ResetPasswordRequest, db_sess: AsyncSession
    ) -> ResetPasswordResponse:
        user = await db_sess.scalar(select(User).where(User.email == request.email))
        if user is None:
            return ResetPasswordResponse()

        code = self.gen_verification_code()
        key = f"{REDIS_PASSWORD_RESET_TOKEN_KEY_PREFIX}{code}"
        await self._redis_client.set(
            key,
            json.dumps({"user_id": str(user.user_id)}),
            ex=REDIS_PASSWORD_RESET_EXPIRY_SECS,
        )

        await self._email_service.send_email(
            user.email,
            "Reset Your Password",
            f"Follow this link: {SCHEME}://{FRONTEND_SUB_DOMAIN}{FRONTEND_DOMAIN}/reset-password/?code={code}",
        )

        return ResetPasswordResponse()

    async def verify_reset_password(
        self, request: ResetPasswordVerificationRequest, db_sess: AsyncSession
    ) -> User:
        token_key = f"{REDIS_PASSWORD_RESET_TOKEN_KEY_PREFIX}{request.code}"
        data = await self._redis_client.get(token_key)
        await self._redis_client.delete(token_key)
        if data is None:
            raise ValueError("Invalid or expired reset code")

        payload = json.loads(data)
        user = await db_sess.get(User, UUID(payload["user_id"]))

        if user is None:
            raise ValueError("Invalid or expired reset code")

        user.password = self.hash_password(request.password)
        return user

    def gen_verification_code(self, k: int = 6) -> str:
        """
        Generates a random alphanumeric verification code.

        Args:
            k: Length of the verification code.

        Returns:
            A randomly generated verification code string.
        """
        return "".join(random.choices(string.ascii_uppercase + string.digits, k=k))

    def hash_password(self, password: str) -> str:
        return self._pw_hasher.hash(password, salt=PW_HASH_SALT.encode())

    def verify_password(self, password: str, hashed_password: str) -> bool:
        """
        Verifies a password against a hashed value.

        Returns:
            True if password matches, otherwise False.
        """
        try:
            self._pw_hasher.verify(hashed_password, password)
            return True
        except Argon2Error:
            return False
