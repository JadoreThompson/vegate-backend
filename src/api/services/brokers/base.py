from abc import ABC, abstractmethod
from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession

from engine.enums import BrokerPlatformType


class BaseBrokerAPI(ABC):
    @abstractmethod
    def get_oauth_url(self) -> str: ...

    @abstractmethod
    async def handle_oauth_callback(self, code: str) -> None: ...

    @abstractmethod
    async def get_account(
        self, user_id: UUID, account_id: UUID, db_sess: AsyncSession
    ) -> Any: ...

    @abstractmethod
    async def get_accounts(
        self, user_id: UUID, broker: BrokerPlatformType, db_sess: AsyncSession
    ) -> Any: ...
