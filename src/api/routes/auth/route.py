import json

from argon2 import PasswordHasher
from argon2.exceptions import Argon2Error
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from fastapi.responses import JSONResponse
from sqlalchemy import insert, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import depends_db_sess, depends_jwt
from api.services import JWTService
from api.types import JWTPayload
from config import (
    PW_HASH_SALT,
    REDIS_EMAIL_VERIFICATION_KEY_PREFIX,
    REDIS_EMAIL_VERIFCATION_EXPIRY_SECS,
)
from utils.redis import REDIS_CLIENT
from services import EmailService
from db_models import Users
from utils.utils import get_datetime
from .controller import gen_verification_code
from .models import (
    UpdateEmail,
    UpdatePassword,
    UpdateUsername,
    UserCreate,
    UserLogin,
    UserMe,
    VerifyAction,
    VerifyCode,
)


router = APIRouter(prefix="/auth", tags=["Auth"])
em_service = EmailService("No-Reply", "no-reply@domain.com")
pw_hasher = PasswordHasher()


@router.post("/register")
async def register(
    body: UserCreate,
    bg_tasks: BackgroundTasks,
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    global em_service, pw_hasher

    res = await db_sess.scalar(
        select(Users).where(
            (Users.username == body.username) | (Users.email == body.email)
        )
    )
    if res is not None:
        raise HTTPException(status_code=400, detail="Username or email already exists.")

    body.password = pw_hasher.hash(body.password, salt=PW_HASH_SALT.encode())

    user = await db_sess.scalar(
        insert(Users).values(**body.model_dump()).returning(Users)
    )

    code = gen_verification_code()
    key = f"{REDIS_EMAIL_VERIFICATION_KEY_PREFIX}{str(user.user_id)}"
    await REDIS_CLIENT.delete(key)
    await REDIS_CLIENT.set(key, code, ex=REDIS_EMAIL_VERIFCATION_EXPIRY_SECS)

    bg_tasks.add_task(
        em_service.send_email,
        body.email,
        "Verify your email",
        f"Your verification code is: {code}",
    )

    rsp = await JWTService.set_user_cookie(user, db_sess)
    rsp.status_code = 202
    await db_sess.commit()

    return rsp


@router.post("/login")
async def login(body: UserLogin, db_sess: AsyncSession = Depends(depends_db_sess)):
    if (body.username is None or not body.username.strip()) and (
        body.email is None or not body.email.strip()
    ):
        raise HTTPException(
            status_code=400, detail="Either username or email must be provided."
        )

    query = select(Users)
    if body.username is not None:
        query = query.where(Users.username == body.username)
    if body.email is not None:
        query = query.where(Users.email == body.email)

    user = await db_sess.scalar(query)
    if user is None:
        raise HTTPException(status_code=400, detail="User doesn't exist.")

    try:
        pw_hasher.verify(user.password, body.password)
    except Argon2Error:
        raise HTTPException(status_code=400, detail="Invalid password.")

    rsp = await JWTService.set_user_cookie(user, db_sess)
    await db_sess.commit()
    return rsp


@router.post("/request-email-verification")
async def request_email_verification(
    bg_tasks: BackgroundTasks, jwt: JWTPayload = Depends(depends_jwt(False))
):
    global em_service

    code = gen_verification_code()
    key = f"{REDIS_EMAIL_VERIFICATION_KEY_PREFIX}{str(jwt.sub)}"

    await REDIS_CLIENT.delete(key)
    await REDIS_CLIENT.set(key, code, ex=REDIS_EMAIL_VERIFCATION_EXPIRY_SECS)

    bg_tasks.add_task(
        em_service.send_email,
        jwt.em,
        "Verify your email",
        f"Your verification code is: {code}",
    )


@router.post("/verify-email")
async def verify_email(
    body: VerifyCode,
    jwt: JWTPayload = Depends(depends_jwt(False)),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    key = f"{REDIS_EMAIL_VERIFICATION_KEY_PREFIX}{str(jwt.sub)}"
    code = await REDIS_CLIENT.get(key)
    await REDIS_CLIENT.delete(key)

    if code is None or code != body.code:
        raise HTTPException(
            status_code=400, detail="Invalid or expired verification code."
        )

    user = await db_sess.scalar(select(Users).where(Users.user_id == jwt.sub))
    user.authenticated_at = get_datetime()
    rsp = await JWTService.set_user_cookie(user, db_sess)
    await db_sess.commit()
    return rsp


@router.post("/logout")
async def logout(
    jwt: JWTPayload = Depends(depends_jwt(False)),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    rsp = JWTService.remove_cookie()
    await db_sess.execute(
        update(Users).values(jwt=None).where(Users.user_id == jwt.sub)
    )
    await db_sess.commit()
    return rsp


@router.get("/me", response_model=UserMe)
async def get_me(
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    user = await db_sess.scalar(select(Users).where(Users.user_id == jwt.sub))
    if not user:
        raise HTTPException(status_code=404, detail="User not found.")

    username, pricing_tier = user.username, user.pricing_tier
    await db_sess.commit()

    return UserMe(username=username, pricing_tier=pricing_tier)


@router.post("/change-username", status_code=202)
async def change_username(
    body: UpdateUsername,
    bg_tasks: BackgroundTasks,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    global em_service

    user = await db_sess.scalar(select(Users).where(Users.user_id == jwt.sub))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    existing_user = await db_sess.scalar(
        select(Users).where(Users.username == body.username)
    )
    if existing_user:
        raise HTTPException(status_code=400, detail="Username already exists.")

    prefix = f"change_username:{jwt.sub}:"
    async for key in REDIS_CLIENT.scan_iter(f"{prefix}*"):
        await REDIS_CLIENT.delete(key)

    verification_code = gen_verification_code()
    payload = json.dumps(
        {
            "user_id": str(user.user_id),
            "action": "change_username",
            "new_value": body.username,
        }
    )
    redis_key = f"{prefix}{verification_code}"
    await REDIS_CLIENT.set(redis_key, payload, ex=REDIS_EMAIL_VERIFCATION_EXPIRY_SECS)

    bg_tasks.add_task(
        em_service.send_email,
        user.email,
        "Confirm Your Username Change",
        f"Your verification code is: {verification_code}",
    )

    return {"message": "A verification code has been sent to your email."}


@router.post("/change-password", status_code=202)
async def change_password(
    body: UpdatePassword,
    bg_tasks: BackgroundTasks,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    global em_service

    user = await db_sess.scalar(select(Users).where(Users.user_id == jwt.sub))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    prefix = f"change_password:{jwt.sub}:"
    async for key in REDIS_CLIENT.scan_iter(f"{prefix}*"):
        await REDIS_CLIENT.delete(key)

    verification_code = gen_verification_code()

    payload = json.dumps(
        {
            "user_id": str(user.user_id),
            "action": "change_password",
            "new_value": body.password,
        }
    )

    await REDIS_CLIENT.set(
        f"{prefix}{verification_code}", payload, ex=REDIS_EMAIL_VERIFCATION_EXPIRY_SECS
    )

    bg_tasks.add_task(
        em_service.send_email,
        user.email,
        "Confirm Your Password Change",
        f"Your verification code is: {verification_code}",
    )

    return {"message": "A verification code has been sent to your email."}


@router.post("/change-email", status_code=202)
async def change_email(
    body: UpdateEmail,
    bg_tasks: BackgroundTasks,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    global em_service

    user = await db_sess.scalar(select(Users).where(Users.user_id == jwt.sub))
    if not user:
        raise HTTPException(status_code=404, detail="User not found")

    existing_user = await db_sess.scalar(select(Users).where(Users.email == body.email))
    if existing_user:
        raise HTTPException(status_code=400, detail="Email already exists.")

    prefix = f"change_email:{jwt.sub}:"
    async for key in REDIS_CLIENT.scan_iter(f"{prefix}*"):
        await REDIS_CLIENT.delete(key)

    verification_code = gen_verification_code()
    payload = json.dumps(
        {
            "user_id": str(user.user_id),
            "action": "change_email",
            "new_value": body.email,
        }
    )
    redis_key = f"{prefix}{verification_code}"
    await REDIS_CLIENT.set(redis_key, payload, ex=REDIS_EMAIL_VERIFCATION_EXPIRY_SECS)

    bg_tasks.add_task(
        em_service.send_email,
        body.email,
        "Confirm Your Email Change",
        f"Your verification code is: {verification_code}",
    )

    return {"message": "A verification code has been sent to your new email."}


@router.post("/verify-action")
async def verify_action(
    body: VerifyAction,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    global pw_hasher

    redis_key = f"{body.action}:{jwt.sub}:{body.code}"
    data_str = await REDIS_CLIENT.get(redis_key)
    if not data_str:
        raise HTTPException(
            status_code=400, detail="Invalid or expired verification code."
        )
    await REDIS_CLIENT.delete(redis_key)

    data = json.loads(data_str)
    user_id = data["user_id"]
    if user_id != jwt.sub:
        raise HTTPException(status_code=401, detail="Unauthorised request.")

    action = data["action"]
    new_value = data["new_value"]

    if action == "change_username":
        # Final check for username uniqueness to avoid race conditions
        existing_user = await db_sess.scalar(
            select(Users).where(Users.username == new_value)
        )
        if existing_user:
            raise HTTPException(status_code=400, detail="Username already taken.")

        await db_sess.execute(
            update(Users).where(Users.user_id == user_id).values(username=new_value)
        )
        message = "Username changed successfully."
        await db_sess.commit()

    elif action == "change_email":
        # Final check for email uniqueness to avoid race conditions
        existing_user = await db_sess.scalar(
            select(Users).where(Users.email == new_value)
        )
        if existing_user:
            raise HTTPException(status_code=400, detail="Email already taken.")

        user = await db_sess.scalar(select(Users).where(Users.user_id == user_id))
        await db_sess.execute(
            update(Users).where(Users.user_id == user_id).values(email=new_value)
        )

        # Update JWT with new email
        rsp = await JWTService.set_user_cookie(user, db_sess)
        message = "Email changed successfully."
        await db_sess.commit()
        return rsp

    elif action == "change_password":
        await db_sess.execute(
            update(Users)
            .where(Users.user_id == user_id)
            .values(password=pw_hasher.hash(new_value, salt=PW_HASH_SALT.encode()))
        )

        await db_sess.execute(
            update(Users).values(jwt=None).where(Users.user_id == user_id)
        )

        rsp = JSONResponse(
            status_code=200, content={"message": "Password changed successfully."}
        )
        rsp = JWTService.remove_cookie(rsp)
        await db_sess.commit()
        return rsp

    else:
        raise HTTPException(status_code=400, detail="Unknown action specified.")

    return {"message": message}
