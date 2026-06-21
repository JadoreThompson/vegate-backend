from fastapi import APIRouter, Depends, HTTPException, Response
from sqlalchemy import update
from sqlalchemy.ext.asyncio import AsyncSession

from module.api.dependencies import depends_class, depends_db_sess, depends_jwt
from module.jwt import JWTPayload, JWTService
from module.user.model import User
from .schema import (
    ChangeEmailRequest,
    ChangePasswordRequest,
    ChangeUsernameRequest,
    EmailVerificationRequest,
    ResetPasswordRequest,
    ResetPasswordVerificationRequest,
    RegisterUserRequest,
    LoginUserRequest,
    VerificationCode,
)
from .service import AuthService

router = APIRouter(prefix="/api/v1/auth", tags=["Auth"])


@router.post("/register")
async def register(
    body: RegisterUserRequest,
    db_sess: AsyncSession = Depends(depends_db_sess),
    auth_service: AuthService = Depends(depends_class(AuthService)),
):
    await auth_service.register_user(body, db_sess)
    await db_sess.commit()
    return Response(status_code=201)


@router.post("/login")
async def login(
    body: LoginUserRequest,
    db_sess: AsyncSession = Depends(depends_db_sess),
    auth_service: AuthService = Depends(depends_class(AuthService)),
    jwt_service: JWTService = Depends(depends_class(JWTService)),
):
    user = await auth_service.authenticate_user(body, db_sess)
    rsp = await jwt_service.set_cookie(user, db_sess)
    await db_sess.commit()
    return rsp


@router.post("/verify-email/request", status_code=201)
async def request_email_verification(
    body: EmailVerificationRequest,
    db_sess: AsyncSession = Depends(depends_db_sess),
    auth_service: AuthService = Depends(depends_class(AuthService)),
):
    await auth_service.request_email_verification(body, db_sess=db_sess)
    await db_sess.commit()


@router.post("/verify-email")
async def verify_email(
    body: VerificationCode,
    db_sess: AsyncSession = Depends(depends_db_sess),
    auth_service: AuthService = Depends(depends_class(AuthService)),
    jwt_service: JWTService = Depends(depends_class(JWTService)),
):
    user = await auth_service.verify_email(request=body, db_sess=db_sess)
    rsp = await jwt_service.set_cookie(user=user, db_sess=db_sess)
    await db_sess.commit()
    return rsp


@router.post("/logout")
async def logout(
    jwt: JWTPayload = Depends(depends_jwt),
    db_sess: AsyncSession = Depends(depends_db_sess),
    jwt_service: JWTService = Depends(depends_class(JWTService)),
):
    await db_sess.execute(update(User).values(jwt=None).where(User.id == jwt.sub))
    rsp = jwt_service.remove_cookie()
    await db_sess.commit()
    return rsp


@router.post("/change-username/request", status_code=201)
async def request_change_username(
    body: ChangeUsernameRequest,
    jwt: JWTPayload = Depends(depends_jwt),
    db_sess: AsyncSession = Depends(depends_db_sess),
    auth_service: AuthService = Depends(depends_class(AuthService)),
):
    await auth_service.request_username_change(
        request=body, user_id=jwt.sub, db_sess=db_sess
    )


@router.post("/change-username", status_code=202)
async def change_username(
    body: VerificationCode,
    jwt: JWTPayload = Depends(depends_jwt),
    db_sess: AsyncSession = Depends(depends_db_sess),
    auth_service: AuthService = Depends(depends_class(AuthService)),
):
    user = await auth_service.verify_username_change(
        request=body, user_id=jwt.sub, db_sess=db_sess
    )
    await db_sess.commit()


@router.post("/change-password/request", status_code=201)
async def request_change_password(
    body: ChangePasswordRequest,
    jwt: JWTPayload = Depends(depends_jwt),
    db_sess: AsyncSession = Depends(depends_db_sess),
    auth_service: AuthService = Depends(depends_class(AuthService)),
):
    await auth_service.request_password_change(
        request=body, user_id=jwt.sub, db_sess=db_sess
    )


@router.post("/change-password", status_code=202)
async def verify_change_password(
    body: VerificationCode,
    jwt: JWTPayload = Depends(depends_jwt),
    db_sess: AsyncSession = Depends(depends_db_sess),
    auth_service: AuthService = Depends(depends_class(AuthService)),
):
    try:
        await auth_service.verify_password_change(
            request=body, user_id=jwt.sub, db_sess=db_sess
        )
        await db_sess.commit()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/change-email/request", status_code=202)
async def request_change_email(
    body: ChangeEmailRequest,
    jwt: JWTPayload = Depends(depends_jwt),
    db_sess: AsyncSession = Depends(depends_db_sess),
    auth_service: AuthService = Depends(depends_class(AuthService)),
):
    await auth_service.request_email_change(
        request=body, user_id=jwt.sub, db_sess=db_sess
    )


@router.post("/change-email", status_code=202)
async def verify_change_email(
    body: VerificationCode,
    jwt: JWTPayload = Depends(depends_jwt),
    db_sess: AsyncSession = Depends(depends_db_sess),
    auth_service: AuthService = Depends(depends_class(AuthService)),
):
    try:
        user = await auth_service.verify_email_change(
            request=body, user_id=jwt.sub, db_sess=db_sess
        )
        await db_sess.commit()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/reset-password/request", status_code=201)
async def reset_password(
    body: ResetPasswordRequest,
    db_sess: AsyncSession = Depends(depends_db_sess),
    auth_service: AuthService = Depends(depends_class(AuthService)),
):
    await auth_service.request_reset_password(request=body, db_sess=db_sess)


@router.patch("/reset-password", status_code=200)
async def confirm_reset_password(
    body: ResetPasswordVerificationRequest,
    db_sess: AsyncSession = Depends(depends_db_sess),
    auth_service: AuthService = Depends(depends_class(AuthService)),
):
    try:
        await auth_service.verify_reset_password(request=body, db_sess=db_sess)
        await db_sess.commit()
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
