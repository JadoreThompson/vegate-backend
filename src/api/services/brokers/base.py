from abc import ABC, abstractmethod
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession


class BaseBrokerAPI(ABC):
    @abstractmethod
    def get_oauth_url(self) -> str: ...

    @abstractmethod
    async def handle_oauth_callback(
        self, code: str, user_id: UUID, db_sess: AsyncSession
    ) -> None: ...
