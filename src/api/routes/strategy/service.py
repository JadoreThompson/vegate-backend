from uuid import UUID

from sqlalchemy import select, and_
from sqlalchemy.ext.asyncio import AsyncSession

from api.models import PaginatedResponse
from api.routes.strategy.agents.code_review import code_review_agent, CodeReviewOutput
from api.routes.strategy.agents.strategy import strategy_gen_agent, StrategyGenOutput
from api.routes.strategy.exception import (
    StrategyGenerationError,
    StrategyValidationException,
    StrategyNotFoundException
)
from api.routes.strategy.models import CreateStrategyRequest, UpdateStrategyRequest, StrategyResponse
from infra.db.model import Strategy


class StrategyService:

    def __init__(self):
        pass

    async def create(self, request: CreateStrategyRequest, user_id: UUID, db_sess: AsyncSession) -> Strategy:
        strategy_details = await self._generate_strategy(request.description)
        await self._validate_strategy_code(strategy_details.code)

        new_strategy = Strategy(
            user_id=user_id,
            name=strategy_details.name,
            description=strategy_details.description,
            code=strategy_details.code,
            prompt=request.description,
        )
        db_sess.add(new_strategy)
        await db_sess.flush()
        await db_sess.refresh(new_strategy)
        return new_strategy

    async def _generate_strategy(self, description: str) -> StrategyGenOutput:
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

    async def get_strategy(self, id: UUID, user_id: UUID, db_sess: AsyncSession) -> Strategy | None:
        return await db_sess.scalar(select(Strategy).where(and_(Strategy.strategy_id == id, Strategy.user_id == user_id)))

    async def get_strategies(
            self, user_id: UUID, db_sess: AsyncSession, *, page: int, limit: int
    ) -> PaginatedResponse[StrategyResponse]:
        result = await db_sess.execute(
            select(Strategy)
            .where(Strategy.user_id == user_id)
            .order_by(Strategy.created_at.desc())
            .offset((page - 1) * limit)
            .limit(limit + 1)
        )

        strategies = [
            StrategyResponse(
                id=strategy.strategy_id,
                name=strategy.name,
                description=strategy.description,
                prompt=strategy.prompt,
                created_at=strategy.created_at,
                updated_at=strategy.updated_at,
            )
            for strategy in result.scalars().all()
        ]

        return PaginatedResponse[StrategyResponse](
            page=page,
            size=min(limit, len(strategies)),
            has_next=len(strategies) > limit,
            data=strategies[:limit],
        )

    async def update_strategy(self, request: UpdateStrategyRequest, id: UUID, user_id: UUID, db_sess: AsyncSession) -> Strategy:
        strategy = await self.get_user_strategy(id, user_id, db_sess)

        if request.name is not None:
            strategy.name = request.name

        if request.description is not None:
            strategy.description = request.description

        return strategy

    async def delete_strategy(self, id: UUID, user_id: UUID, db_sess: AsyncSession) -> None:
        strategy = await self.get_user_strategy(id, user_id, db_sess)
        await db_sess.delete(strategy)

    async def get_user_strategy(self, id: UUID, user_id: UUID, db_sess: AsyncSession) -> Strategy:
        strategy = await db_sess.scalar(
            select(Strategy).where(and_(Strategy.strategy_id == id, Strategy.user_id == user_id)))
        if strategy is None:
            raise StrategyNotFoundException()
        return strategy
