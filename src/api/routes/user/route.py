from fastapi import APIRouter, Depends

from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import depends_db_sess, depends_jwt
from api.routes.user.model import UserResponse
from api.types import JWTPayload
from infra.db.model.user import User

router = APIRouter(prefix="/users", tags=["Users"])

@router.get("/me", response_model=UserResponse)
async def get_me(
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    user = await db_sess.get(User, jwt.sub)
    return UserResponse(username=user.username, pricing_tier=user.pricing_tier)
