from uuid import UUID

from fastapi import HTTPException
from pydantic import BaseModel
from pydantic_ai import Agent
from pydantic_ai.models.mistral import MistralModel
from pydantic_ai.providers.mistral import MistralProvider
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from api.exc import CustomValidationError
from config import LLM_API_KEY
from db_models import Strategies
from .models import StrategyCreate, StrategyUpdate


class StrategyGenOutput(BaseModel):
    code: str
    error: str | None = None


class CodeReviewOutput(BaseModel):
    valid: bool
    code: str
    error: str | None = None


class ValidationOutput(BaseModel):
    valid: bool
    error: str | None = None


strategy_gen_sys_prompt = """
You're a seasoned vetern in algorithmic trading having worked at WorldQuant, Dolat Group,
Golman Sachs, some of the best to ever do it and currently runnign your own algorithmic
trading firm with a 10 person team.

You and your team have built a platform which allows you guys to quickly backtest
a strategy and deploy said strategy by interacting with core interfaces and objects.

"""


strategy_gen_agent = Agent(model, sys_prompt)
code_review_agent = Agent(model, sys_prompt)
validation_agent = Agent(model, sys_prompt)


async def _create_strategy(user_id: UUID, data: StrategyCreate, db_sess: AsyncSession):
    exists = await db_sess.scalar(
        select(Strategies.strategy_id).where(
            Strategies.name == data.name, Strategies.user_id == user_id
        )
    )
    if exists:
        raise HTTPException(
            status_code=409, detail=f"Strategy with name '{data.name}' already exists"
        )

    strategy_code = None
    while ...:
        res = await strategy_gen_agent.run()
        tmp_strategy_code = res.output.code

        valid = False
        for _ in range(2):
            res = await code_review_agent.run()
            valid = res.output.valid
            if valid:
                break

        res = await validation_agent.run()
        if not res.output.valid:
            raise

    new_strategy = Strategies(
        user_id=user_id,
        name=data.name,
        description=data.description,
        code=strategy_code,
        prompt=data.prompt,
    )
    db_sess.add(new_strategy)
    await db_sess.flush()
    await db_sess.refresh(new_strategy)
