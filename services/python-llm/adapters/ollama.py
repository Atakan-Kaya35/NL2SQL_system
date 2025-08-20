import os, httpx

class OllamaAdapter:
    def __init__(self):
        self.base_url = os.getenv("OLLAMA_URL", "http://localhost:11434")
        self.model = os.getenv("OLLAMA_MODEL", "llama3.1:8b-instruct")  # set to a local model tag that you have

    async def generate(self, prompt: str) -> str:
        async with httpx.AsyncClient(timeout=60) as client:
            resp = await client.post(f"{self.base_url}/api/generate", json={
                "model": self.model,
                "prompt": prompt,
                "stream": False
            })
            resp.raise_for_status()
            data = resp.json()
            return data.get("response", "")
