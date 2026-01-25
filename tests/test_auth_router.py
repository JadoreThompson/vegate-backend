import json
import pytest
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from api.routes.auth.route import router
from api.dependencies import depends_db_sess, depends_jwt
from api.types import JWTPayload
from config import (
    COOKIE_ALIAS,
    REDIS_EMAIL_VERIFICATION_KEY_PREFIX,
)
from core.enums import PricingTierType
from infra.db.models import Users
from services.jwt import JWTService
from infra.redis import REDIS_CLIENT


# Fixtures
@pytest.fixture(scope="module")
def test_app():
    """Create a test client for the auth router."""
    app = FastAPI()
    app.include_router(router)
    client = TestClient(app)
    yield client


@pytest.fixture
def mock_db_session():
    """Create a mock database session."""
    session = MagicMock()
    session.scalar = AsyncMock(return_value=None)
    session.execute = AsyncMock()
    session.commit = AsyncMock()
    session.flush = AsyncMock()
    session.rollback = AsyncMock()
    return session


@pytest.fixture
def test_user():
    """Create a test user object."""
    user = Users(
        user_id=uuid4(),
        username="testuser",
        email="test@example.com",
        password="hashed_password",
        pricing_tier=PricingTierType.FREE.value,
        jwt="test_jwt_token",
        authenticated_at=None,
    )
    return user


@pytest.fixture
def test_jwt_payload(test_user):
    """Create a test JWT payload."""
    return JWTPayload(
        sub=str(test_user.user_id),
        em=test_user.email,
        iat=1234567890,
        exp=9999999999,
    )


# Registration Tests
def test_register_success(test_app, monkeypatch, mock_db_session):
    """Test successful user registration."""
    test_request = {
        "username": "newuser",
        "email": "newuser@example.com",
        "password": "password123",
    }

    # Mock database to return None (user doesn't exist)
    async def mock_scalar(query):
        return None

    mock_db_session.scalar = mock_scalar

    # Mock insert to return new user
    new_user = Users(
        user_id=uuid4(),
        username=test_request["username"],
        email=test_request["email"],
        password="hashed_password",
        pricing_tier=PricingTierType.FREE.value,
    )

    async def mock_scalar_insert(query):
        return new_user

    # Override dependencies
    app.dependency_overrides[depends_db_sess] = lambda: mock_db_session

    # Mock Redis operations
    async def mock_redis_delete(key):
        return True

    async def mock_redis_set(key, value, ex):
        return True

    monkeypatch.setattr(REDIS_CLIENT, "delete", mock_redis_delete)
    monkeypatch.setattr(REDIS_CLIENT, "set", mock_redis_set)

    # Mock JWT service
    async def mock_set_user_cookie(user, db_sess):
        from fastapi.responses import JSONResponse

        response = JSONResponse(content={"message": "User registered"})
        response.set_cookie(key=COOKIE_ALIAS, value="test_token")
        return response

    monkeypatch.setattr(JWTService, "set_user_cookie", mock_set_user_cookie)

    response = test_app.post("/auth/register", json=test_request)

    assert response.status_code == 202
    assert COOKIE_ALIAS in response.cookies

    # Clean up
    app.dependency_overrides = {}


def test_register_duplicate_username(test_app, monkeypatch, mock_db_session, test_user):
    """Test registration with existing username."""
    test_request = {
        "username": "testuser",
        "email": "newemail@example.com",
        "password": "password123",
    }

    # Mock database to return existing user
    async def mock_scalar(query):
        return test_user

    mock_db_session.scalar = mock_scalar

    app.dependency_overrides[depends_db_sess] = lambda: mock_db_session

    response = test_app.post("/auth/register", json=test_request)

    assert response.status_code == 400
    assert response.json()["detail"] == "Username or email already exists."

    app.dependency_overrides = {}


def test_register_duplicate_email(test_app, monkeypatch, mock_db_session, test_user):
    """Test registration with existing email."""
    test_request = {
        "username": "newuser",
        "email": "test@example.com",
        "password": "password123",
    }

    # Mock database to return existing user
    async def mock_scalar(query):
        return test_user

    mock_db_session.scalar = mock_scalar

    app.dependency_overrides[depends_db_sess] = lambda: mock_db_session

    response = test_app.post("/auth/register", json=test_request)

    assert response.status_code == 400
    assert response.json()["detail"] == "Username or email already exists."

    app.dependency_overrides = {}


# Login Tests
def test_login_with_username_success(test_app, monkeypatch, mock_db_session, test_user):
    """Test successful login with username."""
    test_request = {"username": "testuser", "password": "password123"}

    # Mock database to return user
    async def mock_scalar(query):
        return test_user

    mock_db_session.scalar = mock_scalar

    app.dependency_overrides[depends_db_sess] = lambda: mock_db_session

    # Mock password verification
    def mock_verify(hashed, plain):
        return True

    from api.routes.auth import route

    monkeypatch.setattr(route.pw_hasher, "verify", mock_verify)

    # Mock JWT service
    async def mock_set_user_cookie(user, db_sess):
        from fastapi.responses import JSONResponse

        response = JSONResponse(content={"message": "Login successful"})
        response.set_cookie(key=COOKIE_ALIAS, value="test_token")
        return response

    monkeypatch.setattr(JWTService, "set_user_cookie", mock_set_user_cookie)

    response = test_app.post("/auth/login", json=test_request)

    assert response.status_code == 200
    assert COOKIE_ALIAS in response.cookies

    app.dependency_overrides = {}


def test_login_with_email_success(test_app, monkeypatch, mock_db_session, test_user):
    """Test successful login with email."""
    test_request = {"email": "test@example.com", "password": "password123"}

    # Mock database to return user
    async def mock_scalar(query):
        return test_user

    mock_db_session.scalar = mock_scalar

    app.dependency_overrides[depends_db_sess] = lambda: mock_db_session

    # Mock password verification
    def mock_verify(hashed, plain):
        return True

    from api.routes.auth import route

    monkeypatch.setattr(route.pw_hasher, "verify", mock_verify)

    # Mock JWT service
    async def mock_set_user_cookie(user, db_sess):
        from fastapi.responses import JSONResponse

        response = JSONResponse(content={"message": "Login successful"})
        response.set_cookie(key=COOKIE_ALIAS, value="test_token")
        return response

    monkeypatch.setattr(JWTService, "set_user_cookie", mock_set_user_cookie)

    response = test_app.post("/auth/login", json=test_request)

    assert response.status_code == 200
    assert COOKIE_ALIAS in response.cookies

    app.dependency_overrides = {}


def test_login_missing_credentials(test_app):
    """Test login without username or email."""
    test_request = {"password": "password123"}

    response = test_app.post("/auth/login", json=test_request)

    assert response.status_code == 400
    assert response.json()["detail"] == "Either username or email must be provided."


def test_login_user_not_found(test_app, mock_db_session):
    """Test login with non-existent user."""
    test_request = {"username": "nonexistent", "password": "password123"}

    # Mock database to return None
    async def mock_scalar(query):
        return None

    mock_db_session.scalar = mock_scalar

    app.dependency_overrides[depends_db_sess] = lambda: mock_db_session

    response = test_app.post("/auth/login", json=test_request)

    assert response.status_code == 400
    assert response.json()["detail"] == "User doesn't exist."

    app.dependency_overrides = {}


def test_login_invalid_password(test_app, monkeypatch, mock_db_session, test_user):
    """Test login with invalid password."""
    test_request = {"username": "testuser", "password": "wrongpassword"}

    # Mock database to return user
    async def mock_scalar(query):
        return test_user

    mock_db_session.scalar = mock_scalar

    app.dependency_overrides[depends_db_sess] = lambda: mock_db_session

    # Mock password verification to raise error
    from argon2.exceptions import VerifyMismatchError

    def mock_verify(hashed, plain):
        raise VerifyMismatchError()

    from api.routes.auth import route

    monkeypatch.setattr(route.pw_hasher, "verify", mock_verify)

    response = test_app.post("/auth/login", json=test_request)

    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid password."

    app.dependency_overrides = {}


# Email Verification Tests
def test_request_email_verification(test_app, monkeypatch, test_jwt_payload):
    """Test requesting email verification code."""

    # Mock JWT dependency
    async def mock_depends_jwt():
        return test_jwt_payload

    app.dependency_overrides[depends_jwt(False)] = mock_depends_jwt

    # Mock Redis operations
    async def mock_redis_delete(key):
        return True

    async def mock_redis_set(key, value, ex):
        return True

    monkeypatch.setattr(REDIS_CLIENT, "delete", mock_redis_delete)
    monkeypatch.setattr(REDIS_CLIENT, "set", mock_redis_set)

    response = test_app.post("/auth/request-email-verification")

    assert response.status_code == 200

    app.dependency_overrides = {}


def test_verify_email_success(
    test_app, monkeypatch, mock_db_session, test_user, test_jwt_payload
):
    """Test successful email verification."""
    test_request = {"code": "123456"}

    # Mock JWT dependency
    async def mock_depends_jwt():
        return test_jwt_payload

    app.dependency_overrides[depends_jwt(False)] = mock_depends_jwt
    app.dependency_overrides[depends_db_sess] = lambda: mock_db_session

    # Mock Redis get to return the code
    async def mock_redis_get(key):
        return b"123456"

    async def mock_redis_delete(key):
        return True

    monkeypatch.setattr(REDIS_CLIENT, "get", mock_redis_get)
    monkeypatch.setattr(REDIS_CLIENT, "delete", mock_redis_delete)

    # Mock database to return user
    async def mock_scalar(query):
        return test_user

    mock_db_session.scalar = mock_scalar

    # Mock JWT service
    async def mock_set_user_cookie(user, db_sess):
        from fastapi.responses import JSONResponse

        response = JSONResponse(content={"message": "Email verified"})
        response.set_cookie(key=COOKIE_ALIAS, value="test_token")
        return response

    monkeypatch.setattr(JWTService, "set_user_cookie", mock_set_user_cookie)

    response = test_app.post("/auth/verify-email", json=test_request)

    assert response.status_code == 200
    assert COOKIE_ALIAS in response.cookies

    app.dependency_overrides = {}


def test_verify_email_invalid_code(
    test_app, monkeypatch, test_jwt_payload
):
    """Test email verification with invalid code."""
    test_request = {"code": "wrong_code"}

    # Mock JWT dependency
    async def mock_depends_jwt():
        return test_jwt_payload

    app.dependency_overrides[depends_jwt(False)] = mock_depends_jwt

    # Mock Redis get to return different code
    async def mock_redis_get(key):
        return b"123456"

    monkeypatch.setattr(REDIS_CLIENT, "get", mock_redis_get)

    response = test_app.post("/auth/verify-email", json=test_request)

    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid or expired verification code."

    app.dependency_overrides = {}


def test_verify_email_expired_code(test_app, monkeypatch, test_jwt_payload):
    """Test email verification with expired code."""
    test_request = {"code": "123456"}

    # Mock JWT dependency
    async def mock_depends_jwt():
        return test_jwt_payload

    app.dependency_overrides[depends_jwt(False)] = mock_depends_jwt

    # Mock Redis get to return None (expired)
    async def mock_redis_get(key):
        return None

    monkeypatch.setattr(REDIS_CLIENT, "get", mock_redis_get)

    response = test_app.post("/auth/verify-email", json=test_request)

    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid or expired verification code."

    app.dependency_overrides = {}


# Logout Tests
def test_logout_success(test_app, monkeypatch, mock_db_session, test_jwt_payload):
    """Test successful logout."""

    # Mock JWT dependency
    async def mock_depends_jwt():
        return test_jwt_payload

    app.dependency_overrides[depends_jwt(False)] = mock_depends_jwt
    app.dependency_overrides[depends_db_sess] = lambda: mock_db_session

    # Mock JWT service
    def mock_remove_cookie():
        from fastapi.responses import JSONResponse

        response = JSONResponse(content={"message": "Logged out"})
        response.delete_cookie(key=COOKIE_ALIAS)
        return response

    monkeypatch.setattr(JWTService, "remove_cookie", mock_remove_cookie)

    response = test_app.post("/auth/logout")

    assert response.status_code == 200

    app.dependency_overrides = {}


# Get Me Tests
def test_get_me_success(test_app, mock_db_session, test_user, test_jwt_payload):
    """Test getting current user info."""

    # Mock JWT dependency
    async def mock_depends_jwt():
        return test_jwt_payload

    app.dependency_overrides[depends_jwt()] = mock_depends_jwt
    app.dependency_overrides[depends_db_sess] = lambda: mock_db_session

    # Mock database to return user
    async def mock_scalar(query):
        return test_user

    mock_db_session.scalar = mock_scalar

    response = test_app.get("/auth/me")

    assert response.status_code == 200
    assert response.json()["username"] == test_user.username
    assert response.json()["pricing_tier"] == test_user.pricing_tier

    app.dependency_overrides = {}


def test_get_me_user_not_found(test_app, mock_db_session, test_jwt_payload):
    """Test getting current user when user doesn't exist."""

    # Mock JWT dependency
    async def mock_depends_jwt():
        return test_jwt_payload

    app.dependency_overrides[depends_jwt()] = mock_depends_jwt
    app.dependency_overrides[depends_db_sess] = lambda: mock_db_session

    # Mock database to return None
    async def mock_scalar(query):
        return None

    mock_db_session.scalar = mock_scalar

    response = test_app.get("/auth/me")

    assert response.status_code == 404
    assert response.json()["detail"] == "User not found."

    app.dependency_overrides = {}


# Change Username Tests
def test_change_username_success(
    test_app, monkeypatch, mock_db_session, test_user, test_jwt_payload
):
    """Test successful username change request."""
    test_request = {"username": "newusername"}

    # Mock JWT dependency
    async def mock_depends_jwt():
        return test_jwt_payload

    app.dependency_overrides[depends_jwt()] = mock_depends_jwt
    app.dependency_overrides[depends_db_sess] = lambda: mock_db_session

    # Mock database to return user, then None for username check
    call_count = [0]

    async def mock_scalar(query):
        call_count[0] += 1
        if call_count[0] == 1:
            return test_user
        return None  # Username doesn't exist

    mock_db_session.scalar = mock_scalar

    # Mock Redis operations
    async def mock_redis_scan_iter(pattern):
        return iter([])

    async def mock_redis_delete(key):
        return True

    async def mock_redis_set(key, value, ex):
        return True

    monkeypatch.setattr(REDIS_CLIENT, "scan_iter", mock_redis_scan_iter)
    monkeypatch.setattr(REDIS_CLIENT, "delete", mock_redis_delete)
    monkeypatch.setattr(REDIS_CLIENT, "set", mock_redis_set)

    response = test_app.post("/auth/change-username", json=test_request)

    assert response.status_code == 202
    assert "verification code" in response.json()["message"].lower()

    app.dependency_overrides = {}


def test_change_username_already_exists(
    test_app, mock_db_session, test_user, test_jwt_payload
):
    """Test username change with existing username."""
    test_request = {"username": "existinguser"}

    # Mock JWT dependency
    async def mock_depends_jwt():
        return test_jwt_payload

    app.dependency_overrides[depends_jwt()] = mock_depends_jwt
    app.dependency_overrides[depends_db_sess] = lambda: mock_db_session

    # Mock database to return user for both calls
    async def mock_scalar(query):
        return test_user

    mock_db_session.scalar = mock_scalar

    response = test_app.post("/auth/change-username", json=test_request)

    assert response.status_code == 400
    assert response.json()["detail"] == "Username already exists."

    app.dependency_overrides = {}


# Change Password Tests
def test_change_password_success(
    test_app, monkeypatch, mock_db_session, test_user, test_jwt_payload
):
    """Test successful password change request."""
    test_request = {"password": "newpassword123"}

    # Mock JWT dependency
    async def mock_depends_jwt():
        return test_jwt_payload

    app.dependency_overrides[depends_jwt()] = mock_depends_jwt
    app.dependency_overrides[depends_db_sess] = lambda: mock_db_session

    # Mock database to return user
    async def mock_scalar(query):
        return test_user

    mock_db_session.scalar = mock_scalar

    # Mock Redis operations
    async def mock_redis_scan_iter(pattern):
        return iter([])

    async def mock_redis_set(key, value, ex):
        return True

    monkeypatch.setattr(REDIS_CLIENT, "scan_iter", mock_redis_scan_iter)
    monkeypatch.setattr(REDIS_CLIENT, "set", mock_redis_set)

    response = test_app.post("/auth/change-password", json=test_request)

    assert response.status_code == 202
    assert "verification code" in response.json()["message"].lower()

    app.dependency_overrides = {}


# Change Email Tests
def test_change_email_success(
    test_app, monkeypatch, mock_db_session, test_user, test_jwt_payload
):
    """Test successful email change request."""
    test_request = {"email": "newemail@example.com"}

    # Mock JWT dependency
    async def mock_depends_jwt():
        return test_jwt_payload

    app.dependency_overrides[depends_jwt()] = mock_depends_jwt
    app.dependency_overrides[depends_db_sess] = lambda: mock_db_session

    # Mock database to return user, then None for email check
    call_count = [0]

    async def mock_scalar(query):
        call_count[0] += 1
        if call_count[0] == 1:
            return test_user
        return None  # Email doesn't exist

    mock_db_session.scalar = mock_scalar

    # Mock Redis operations
    async def mock_redis_scan_iter(pattern):
        return iter([])

    async def mock_redis_delete(key):
        return True

    async def mock_redis_set(key, value, ex):
        return True

    monkeypatch.setattr(REDIS_CLIENT, "scan_iter", mock_redis_scan_iter)
    monkeypatch.setattr(REDIS_CLIENT, "delete", mock_redis_delete)
    monkeypatch.setattr(REDIS_CLIENT, "set", mock_redis_set)

    response = test_app.post("/auth/change-email", json=test_request)

    assert response.status_code == 202
    assert "verification code" in response.json()["message"].lower()

    app.dependency_overrides = {}


def test_change_email_already_exists(
    test_app, mock_db_session, test_user, test_jwt_payload
):
    """Test email change with existing email."""
    test_request = {"email": "existing@example.com"}

    # Mock JWT dependency
    async def mock_depends_jwt():
        return test_jwt_payload

    app.dependency_overrides[depends_jwt()] = mock_depends_jwt
    app.dependency_overrides[depends_db_sess] = lambda: mock_db_session

    # Mock database to return user for both calls
    async def mock_scalar(query):
        return test_user

    mock_db_session.scalar = mock_scalar

    response = test_app.post("/auth/change-email", json=test_request)

    assert response.status_code == 400
    assert response.json()["detail"] == "Email already exists."

    app.dependency_overrides = {}


# Verify Action Tests
def test_verify_action_change_username(
    test_app, monkeypatch, mock_db_session, test_user, test_jwt_payload
):
    """Test verifying username change action."""
    test_request = {"action": "change_username", "code": "123456"}

    # Mock JWT dependency
    async def mock_depends_jwt():
        return test_jwt_payload

    app.dependency_overrides[depends_jwt()] = mock_depends_jwt
    app.dependency_overrides[depends_db_sess] = lambda: mock_db_session

    # Mock Redis get to return action data
    action_data = json.dumps({
        "user_id": str(test_jwt_payload.sub),
        "action": "change_username",
        "new_value": "newusername",
    })

    async def mock_redis_get(key):
        return action_data.encode()

    async def mock_redis_delete(key):
        return True

    monkeypatch.setattr(REDIS_CLIENT, "get", mock_redis_get)
    monkeypatch.setattr(REDIS_CLIENT, "delete", mock_redis_delete)

    # Mock database to return None (username doesn't exist)
    async def mock_scalar(query):
        return None

    mock_db_session.scalar = mock_scalar

    response = test_app.post("/auth/verify-action", json=test_request)

    assert response.status_code == 200
    assert "Username changed successfully" in response.json()["message"]

    app.dependency_overrides = {}


def test_verify_action_change_email(
    test_app, monkeypatch, mock_db_session, test_user, test_jwt_payload
):
    """Test verifying email change action."""
    test_request = {"action": "change_email", "code": "123456"}

    # Mock JWT dependency
    async def mock_depends_jwt():
        return test_jwt_payload

    app.dependency_overrides[depends_jwt()] = mock_depends_jwt
    app.dependency_overrides[depends_db_sess] = lambda: mock_db_session

    # Mock Redis get to return action data
    action_data = json.dumps({
        "user_id": str(test_jwt_payload.sub),
        "action": "change_email",
        "new_value": "newemail@example.com",
    })

    async def mock_redis_get(key):
        return action_data.encode()

    async def mock_redis_delete(key):
        return True

    monkeypatch.setattr(REDIS_CLIENT, "get", mock_redis_get)
    monkeypatch.setattr(REDIS_CLIENT, "delete", mock_redis_delete)

    # Mock database to return None for email check, then user
    call_count = [0]

    async def mock_scalar(query):
        call_count[0] += 1
        if call_count[0] == 1:
            return None  # Email doesn't exist
        return test_user

    mock_db_session.scalar = mock_scalar

    # Mock JWT service
    async def mock_set_user_cookie(user, db_sess):
        from fastapi.responses import JSONResponse

        response = JSONResponse(content={"message": "Email changed successfully."})
        response.set_cookie(key=COOKIE_ALIAS, value="test_token")
        return response

    monkeypatch.setattr(JWTService, "set_user_cookie", mock_set_user_cookie)

    response = test_app.post("/auth/verify-action", json=test_request)

    assert response.status_code == 200

    app.dependency_overrides = {}


def test_verify_action_change_password(
    test_app, monkeypatch, mock_db_session, test_jwt_payload
):
    """Test verifying password change action."""
    test_request = {"action": "change_password", "code": "123456"}

    # Mock JWT dependency
    async def mock_depends_jwt():
        return test_jwt_payload

    app.dependency_overrides[depends_jwt()] = mock_depends_jwt
    app.dependency_overrides[depends_db_sess] = lambda: mock_db_session

    # Mock Redis get to return action data
    action_data = json.dumps({
        "user_id": str(test_jwt_payload.sub),
        "action": "change_password",
        "new_value": "newpassword123",
    })

    async def mock_redis_get(key):
        return action_data.encode()

    async def mock_redis_delete(key):
        return True

    monkeypatch.setattr(REDIS_CLIENT, "get", mock_redis_get)
    monkeypatch.setattr(REDIS_CLIENT, "delete", mock_redis_delete)

    # Mock JWT service
    def mock_remove_cookie(response=None):
        from fastapi.responses import JSONResponse

        if response is None:
            response = JSONResponse(content={"message": "Password changed successfully."})
        response.delete_cookie(key=COOKIE_ALIAS)
        return response

    monkeypatch.setattr(JWTService, "remove_cookie", mock_remove_cookie)

    response = test_app.post("/auth/verify-action", json=test_request)

    assert response.status_code == 200
    assert "Password changed successfully" in response.json()["message"]

    app.dependency_overrides = {}


def test_verify_action_invalid_code(test_app, monkeypatch, test_jwt_payload):
    """Test verifying action with invalid code."""
    test_request = {"action": "change_username", "code": "wrong_code"}

    # Mock JWT dependency
    async def mock_depends_jwt():
        return test_jwt_payload

    app.dependency_overrides[depends_jwt()] = mock_depends_jwt

    # Mock Redis get to return None
    async def mock_redis_get(key):
        return None

    monkeypatch.setattr(REDIS_CLIENT, "get", mock_redis_get)

    response = test_app.post("/auth/verify-action", json=test_request)

    assert response.status_code == 400
    assert response.json()["detail"] == "Invalid or expired verification code."

    app.dependency_overrides = {}


def test_verify_action_unauthorized(
    test_app, monkeypatch, test_jwt_payload
):
    """Test verifying action with mismatched user ID."""
    test_request = {"action": "change_username", "code": "123456"}

    # Mock JWT dependency
    async def mock_depends_jwt():
        return test_jwt_payload

    app.dependency_overrides[depends_jwt()] = mock_depends_jwt

    # Mock Redis get to return action data with different user_id
    action_data = json.dumps({
        "user_id": str(uuid4()),  # Different user ID
        "action": "change_username",
        "new_value": "newusername",
    })

    async def mock_redis_get(key):
        return action_data.encode()

    async def mock_redis_delete(key):
        return True

    monkeypatch.setattr(REDIS_CLIENT, "get", mock_redis_get)
    monkeypatch.setattr(REDIS_CLIENT, "delete", mock_redis_delete)

    response = test_app.post("/auth/verify-action", json=test_request)

    assert response.status_code == 401
    assert response.json()["detail"] == "Unauthorised request."

    app.dependency_overrides = {}
