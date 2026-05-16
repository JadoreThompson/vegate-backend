from pydantic import BaseModel, Field
from pydantic_ai import Agent
from api.routes.strategy.agents._model import llm_model

SYSTEM_PROMPT = "System prompt"


class StrategyGenOutput(BaseModel):
    name: str | None = Field(None, description="Strategy name")
    description: str | None = Field(None, description="Description of the strategy")
    code: str | None = Field(
        None, description="The strategy class created from the provided prompt"
    )
    error: str | None = Field(None, description="Description of the error")


strategy_gen_agent = Agent(
    model=llm_model,
    output_type=StrategyGenOutput,
    retries=3,
    system_prompt=SYSTEM_PROMPT,
)