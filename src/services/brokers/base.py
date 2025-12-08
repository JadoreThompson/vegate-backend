from abc import ABC, abstractmethod
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession


class BaseBrokerAPI(ABC):
    async def get_oauth_url(self, *args, **kw) -> str:
        raise NotImplementedError()

    def get_oauth_url_sync(self, *args, **kw) -> str:
        raise NotImplementedError()

    @abstractmethod
    async def handle_oauth_callback(
        self, code: str, user_id: UUID, db_sess: AsyncSession
    ) -> None: ...
