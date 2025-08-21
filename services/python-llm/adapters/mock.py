from typing import Optional

class MockAdapter:
    async def generate(self, prompt: str, schema_hint: Optional[str] = None) -> str:
        return "SELECT name FROM users WHERE country = 'USA'"

