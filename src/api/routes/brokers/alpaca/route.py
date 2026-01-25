from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import RedirectResponse
from sqlalchemy import insert
from sqlalchemy.ext.asyncio import AsyncSession

from api.dependencies import depends_db_sess, depends_jwt
from api.routes.brokers.models import GetOauthUrlResponse
from api.types import JWTPayload
from config import FRONTEND_DOMAIN, SCHEME, FRONTEND_SUB_DOMAIN
from enums import BrokerType
from infra.db.models import BrokerConnections
from services.alpaca import AlpacaService
from .models import AlpacaConnectRequest


router = APIRouter(prefix="/brokers/alpaca", tags=["Alpaca"])
alpaca_api = AlpacaService()


@router.get("/oauth", response_model=GetOauthUrlResponse)
async def get_oauth_url(jwt: JWTPayload = Depends(depends_jwt())):
    url = await alpaca_api.get_oauth_url_v2(jwt.sub, "paper")
    return GetOauthUrlResponse(url=url)


@router.get("/oauth/callback")
async def oauth_callback(
    code: str | None = None,
    state: str | None = None,
    error: str | None = None,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    params = [("broker", BrokerType.ALPACA.value)]
    if code is not None:
        await alpaca_api.handle_oauth_callback(code, state, jwt.sub, db_sess)
    else:
        params.append(("error", error))

    query_params = "&".join(f"{k}={v}" for k, v in params)

    return RedirectResponse(
        f"{SCHEME}://{FRONTEND_SUB_DOMAIN}{FRONTEND_DOMAIN}/brokers/oauth?{query_params}"
    )


@router.post("/connect")
async def connect_alpaca(
    body: AlpacaConnectRequest,
    jwt: JWTPayload = Depends(depends_jwt()),
    db_sess: AsyncSession = Depends(depends_db_sess),
):
    account = await alpaca_api.get_account(body.api_key, body.secret_key)
    account_id = account.get("account_number")
    if account_id is None:
        raise HTTPException(status_code=400, detail="Failed to fetch Alpaca account ID")
    
    await db_sess.execute(
        insert(BrokerConnections).values(
            broker=BrokerType.ALPACA,
            user_id=jwt.sub,
            api_key=body.api_key,
            secret_key=body.secret_key,
            broker_account_id=account_id,
        )
    )
    await db_sess.commit()
