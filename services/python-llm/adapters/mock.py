class MockAdapter:
    async def generate(self, prompt: str) -> str:
        return "SELECT name FROM users WHERE country = 'USA'"

