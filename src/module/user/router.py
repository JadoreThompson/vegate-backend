from fastapi import APIRouter, Depends

from sqlalchemy.ext.asyncio import AsyncSession

from module.api.dependencies import depends_db_sess, depends_jwt
from module.jwt import JWTPayload
from .model import User
from .schema import UserResponse

router = APIRouter(prefix="/api/v1/users", tags=["Users"])


@router.get("/me", response_model=UserResponse)
async def get_me(
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    user = await db_sess.get(User, jwt.sub)
    return UserResponse(username=user.username)
