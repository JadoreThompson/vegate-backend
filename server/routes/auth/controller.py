from fastapi import Response

from config import COOKIE_ALIAS
from server.utils.auth import generate_jwt


def set_cookie(rsp: Response | None = None, **kw) -> Response:
    if rsp is None:
        rsp = Response()

    rsp.set_cookie(COOKIE_ALIAS, generate_jwt(**kw))
    return rsp
