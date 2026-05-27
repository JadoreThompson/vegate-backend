from uuid import UUID
from warnings import deprecated

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from module.api.schema import PaginatedResponse
from .agents import code_review_agent, strategy_gen_agent
from .agents.code_review import CodeReviewOutput
from .agents.strategy_gen import StrategyGenOutput
from .exception import (
    StrategyGenerationError,
    StrategyValidationException,
    StrategyNotFoundException,
    StrategyVersionNotFoundException,
    VersionForkDetectedException,
)
from .model import Strategy, StrategyVersion
from .schema import (
    CreateStrategyRequest,
    StrategyResponse,
    StrategyVersionResponse,
    UpdateStrategyRequest,
)


class StrategyService:

    def __init__(self):
        pass

    async def create(
        self, request: CreateStrategyRequest, user_id: UUID, db_sess: AsyncSession
    ) -> Strategy:
        new_strategy = Strategy(
            user_id=user_id,
            name=request.name,
            description=request.description,
        )

        db_sess.add(new_strategy)
        await db_sess.flush()

        new_version = StrategyVersion(strategy_id=new_strategy.strategy_id)
        db_sess.add(new_version)
        await db_sess.flush()

        new_strategy.cur_version_id = new_version.id
        return new_strategy

    @deprecated(
        "Strategy code is no longer generated. It's uploaded and editied by the user"
    )
    async def _generate_strategy_code(self, description: str) -> StrategyGenOutput:
        result = await strategy_gen_agent.run(description)
        output: StrategyGenOutput = result.output

        if output.error is not None:
            raise StrategyGenerationError(output.error)
        return output

    async def _validate_strategy_code(self, strategy_code: str) -> bool:
        max_attempts = 3
        for attempt in range(max_attempts):
            result = await code_review_agent.run(strategy_code)
            output: CodeReviewOutput = result.output
            if not output.is_valid:
                raise StrategyValidationException(output.errors)

        return True

    async def get_strategy(
        self, strategy_id: UUID, user_id: UUID, db_sess: AsyncSession
    ) -> Strategy:
        return await self.get_user_strategy(strategy_id, user_id, db_sess)

    async def get_strategies(
        self,
        user_id: UUID,
        db_sess: AsyncSession,
        *,
        page: int,
        limit: int,
        name: str | None = None,
    ) -> PaginatedResponse[StrategyResponse]:
        stmt = (
            select(Strategy)
            .where(Strategy.user_id == user_id)
            .order_by(Strategy.created_at.desc())
            .offset((page - 1) * limit)
            .limit(limit + 1)
        )

        if name is not None:
            stmt = stmt.where(Strategy.name.like(f"%{name}%"))

        result = await db_sess.execute(stmt)

        strategies = [
            StrategyResponse(
                id=strategy.strategy_id,
                name=strategy.name,
                description=strategy.description,
                created_at=strategy.created_at,
                updated_at=strategy.updated_at,
                cur_version_id=strategy.cur_version_id,
            )
            for strategy in result.scalars().all()
        ]

        return PaginatedResponse[StrategyResponse](
            page=page,
            size=min(limit, len(strategies)),
            has_next=len(strategies) > limit,
            data=strategies[:limit],
        )

    async def update(
        self,
        request: UpdateStrategyRequest,
        strategy_id: UUID,
        user_id: UUID,
        db_sess: AsyncSession,
    ) -> Strategy:
        strategy = await self.get_user_strategy(strategy_id, user_id, db_sess)

        if request.name is not None:
            strategy.name = request.name

        if request.description is not None:
            strategy.description = request.description

        return strategy

    async def update_code(
        self,
        strategy_id: UUID,
        user_id: UUID,
        code: str,
        db_sess: AsyncSession,
    ) -> StrategyVersion:
        strategy = await self.get_user_strategy(strategy_id, user_id, db_sess)
        return await self.create_version(
            strategy_id, user_id, strategy.cur_version_id, code, db_sess
        )

    async def delete(
        self, strategy_id: UUID, user_id: UUID, db_sess: AsyncSession
    ) -> None:
        strategy = await self.get_user_strategy(strategy_id, user_id, db_sess)
        await db_sess.delete(strategy)

    async def get_user_strategy(
        self, strategy_id: UUID, user_id: UUID, db_sess: AsyncSession
    ) -> Strategy:
        strategy = await db_sess.scalar(
            select(Strategy).where(
                Strategy.strategy_id == strategy_id, Strategy.user_id == user_id
            )
        )
        if strategy is None:
            raise StrategyNotFoundException()
        return strategy

    async def get_user_strategy_version(
        self, version_id: UUID, user_id: UUID, db_sess: AsyncSession
    ) -> StrategyVersion:
        version = await db_sess.scalar(
            select(StrategyVersion)
            .join(Strategy, Strategy.strategy_id == StrategyVersion.strategy_id)
            .where(StrategyVersion.id == version_id, Strategy.user_id == user_id)
        )
        if version is None:
            raise StrategyVersionNotFoundException()
        return version

    async def get_versions(
        self,
        strategy_id: UUID,
        user_id: UUID,
        db_sess: AsyncSession,
        *,
        page: int = 1,
        limit: int = 50,
    ) -> PaginatedResponse[StrategyVersionResponse]:
        await self.get_user_strategy(strategy_id, user_id, db_sess)

        stmt = (
            select(StrategyVersion)
            .where(StrategyVersion.strategy_id == strategy_id)
            .order_by(StrategyVersion.created_at.desc())
            .offset((page - 1) * limit)
            .limit(limit + 1)
        )

        result = await db_sess.execute(stmt)
        versions = [
            StrategyVersionResponse(
                id=v.id,
                strategy_id=v.strategy_id,
                prev_version=v.prev_version,
                code=v.code,
                created_at=v.created_at,
                updated_at=v.updated_at,
            )
            for v in result.scalars().all()
        ]

        return PaginatedResponse[StrategyVersionResponse](
            page=page,
            size=min(limit, len(versions)),
            has_next=len(versions) > limit,
            data=versions[:limit],
        )

    async def get_version(
        self,
        version_id: UUID,
        strategy_id: UUID,
        user_id: UUID,
        db_sess: AsyncSession,
    ) -> StrategyVersion:
        await self.get_user_strategy(strategy_id, user_id, db_sess)

        version = await db_sess.scalar(
            select(StrategyVersion).where(
                StrategyVersion.id == version_id,
                StrategyVersion.strategy_id == strategy_id,
            )
        )
        if version is None:
            raise StrategyVersionNotFoundException()
        return version

    async def create_version(
        self,
        strategy_id: UUID,
        user_id: UUID,
        prev_version_id: UUID,
        code: str,
        db_sess: AsyncSession,
    ) -> StrategyVersion:
        strategy = await self.get_user_strategy(strategy_id, user_id, db_sess)
        prev_version = await db_sess.scalar(
            select(StrategyVersion).where(
                StrategyVersion.id == prev_version_id,
                StrategyVersion.strategy_id == strategy_id,
            )
        )
        if prev_version is None:
            raise StrategyVersionNotFoundException()

        new_version = StrategyVersion(
            strategy_id=strategy_id,
            prev_version=prev_version_id,
            code=code,
        )
        db_sess.add(new_version)
        await db_sess.flush()

        strategy.cur_version_id = new_version.id
        return new_version
