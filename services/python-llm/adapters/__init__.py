import os 

from .mock import MockAdapter
from .openai_compat import OpenAICompatAdapter

def get_adapter():
    provider = os.getenv("LLM_PROVIDER", "mock").lower()
    if provider in ("openai", "openrouter", "groq"):
        return OpenAICompatAdapter()
    return MockAdapter()