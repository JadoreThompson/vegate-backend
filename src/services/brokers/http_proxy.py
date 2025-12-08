from aiohttp import ClientResponse, ClientSession


class HTTProxy:
    def __init__(self, http_sess: ClientSession):
        self.http_sess = http_sess

    async def get(self, oauth_payload, url: str, **kw) -> ClientResponse:
        rsp = await self.http_sess.get(url, **kw)
        # TODO: Handle OAuth token expired
        rsp.raise_for_status()
        return rsp

    async def post(self, oauth_payload, url: str, **kw) -> ClientResponse: ...

    async def patch(self, oauth_payload, url: str, **kw) -> ClientResponse: ...

    async def delete(self, oauth_payload, url: str, **kw) -> ClientResponse: ...

    async def _refresh_access_token(self, oauth_payload): ...
