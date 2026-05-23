from pydantic_ai.models.mistral import MistralModel
from pydantic_ai.providers.mistral import MistralProvider

from config import LLM_API_KEY

provider = MistralProvider(api_key=LLM_API_KEY)
LLM_MODEL = MistralModel("mistral-small-latest", provider=provider)
