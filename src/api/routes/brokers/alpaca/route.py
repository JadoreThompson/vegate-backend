from fastapi import APIRouter, Depends
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import depends_db_sess, depends_jwt
from api.routes.brokers.models import GetOauthUrlResponse
from api.services.brokers.alpaca import AlpacaAPI
from api.typing import JWTPayload
from config import DOMAIN, SCHEME, SUB_DOMAIN
from engine.enums import BrokerPlatformType


router = APIRouter(prefix="/brokers/alpaca", tags=["Alpaca"])
alpaca_api = AlpacaAPI()


@router.get("/oauth", response_model=GetOauthUrlResponse)
async def get_oauth_url(jwt: JWTPayload = Depends(depends_jwt())):
    url = alpaca_api.get_oauth_url()
    return GetOauthUrlResponse(url=url)


@router.get("/oauth/callback")
async def oauth_callback(
    code: str | None = None,
    error: str | None = None,
    state: str | None = None,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    params = [("broker", BrokerPlatformType.ALPACA.value)]
    if code is not None:
        await alpaca_api.handle_oauth_callback(code, jwt.sub, db_sess)
    else:
        params.append(("error", error))

    query_params = "&".join(f"{k}={v}" for k, v in params)

    return RedirectResponse(
        f"{SCHEME}://{SUB_DOMAIN}{DOMAIN}/brokers/oauth?{query_params}"
    )
