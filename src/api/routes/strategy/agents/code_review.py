from pydantic import BaseModel, Field
from pydantic_ai import Agent
from api.routes.strategy.agents._model import llm_model

SYSTEM_PROMPT = "System prompt"


class CodeReviewOutput(BaseModel):
    is_valid: bool = Field(description="Whether the code is syntactically correct")
    errors: list[str] = Field(
        default_factory=list, description="List of syntax or logical errors found"
    )
    recommendation: str | None = Field(
        None, description="Corrected code if errors were found"
    )


code_review_agent = Agent(
    model=llm_model,
    output_type=CodeReviewOutput,
    retries=3,
    system_prompt=SYSTEM_PROMPT,
)