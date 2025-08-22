# adapters/ollama.py
import os, httpx

DEFAULT_BASE = os.getenv("OLLAMA_BASE_URL", "http://nl2sql_ollama:11434")
DEFAULT_MODEL = os.getenv("OLLAMA_MODEL", "llama3.1:8b-instruct-q4_K_M")

class OllamaAdapter:
    def __init__(self, base_url: str = DEFAULT_BASE, model: str = DEFAULT_MODEL):
        self.base_url = base_url.rstrip("/")
        self.model = model

    async def generate(
        self,
        prompt: str,
        *,
        schema_ddl: str | None = None,
        schema_hint: str | None = None,
        system: str | None = None,
        temperature: float = 0.2,
        max_tokens: int = 512,
    ) -> str:
        schema = schema_ddl or schema_hint  # ← accept both
        sys_prompt = system or (
            "You are a SQL generator for PostgreSQL. "
            "Return ONLY one valid SQL statement. No explanations."
        )
        if schema:
            sys_prompt += (
                "\n\n# DB_SCHEMA\n" + schema +
                "\n# Rules: Use only available tables/columns. SELECT only."
            )

        payload = {
            "model": self.model,
            "messages": [
                {"role": "system", "content": sys_prompt},
                {"role": "user",   "content": prompt},
            ],
            "stream": False,
            "options": {"temperature": temperature, "num_predict": max_tokens},
        }

        async with httpx.AsyncClient(timeout=600) as client:
            r = await client.post(f"{self.base_url}/api/chat", json=payload)
            r.raise_for_status()
            data = r.json()
        return (data.get("message") or {}).get("content", "").strip()
