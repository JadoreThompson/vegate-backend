from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse
from sqlalchemy import insert, select
from sqlalchemy.ext.asyncio import AsyncSession

from db_models import Users
from server.dependencies import depends_db_sess
from server.services import JWTService
from .models import UserCreate, UserLogin


router = APIRouter(prefix="/auth", tags=["auth"])


@router.post("/register")
async def register(body: UserCreate, db_sess: AsyncSession = Depends(depends_db_sess)):
    res = await db_sess.execute(select(Users).where(Users.username == body.username))
    if res.first():
        return JSONResponse(
            status_code=401, content={"error": "User with username already exists."}
        )

    res = await db_sess.execute(
        insert(Users)
        .values(username=body.username, password=body.password)
        .returning(Users)
    )
    new_user = res.scalar_one()
    await db_sess.commit()

    return JWTService.set_cookie(new_user)


@router.post("/login")
async def login(body: UserLogin, db_sess: AsyncSession = Depends(depends_db_sess)):
    res = await db_sess.execute(
        select(Users).where(
            Users.username == body.username, Users.password == body.password
        )
    )
    user = res.scalar_one_or_none()

    if user is None:
        return JSONResponse(status_code=401, content={"error": "Invalid user."})

    return JWTService.set_cookie(user)
