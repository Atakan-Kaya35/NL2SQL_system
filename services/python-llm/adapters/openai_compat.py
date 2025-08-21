import os
import httpx
import json
from typing import Optional

DEFAULT_ENDPOINTS = {
    "openai": "https://api.openai.com/v1/chat/completions",
    "openrouter": "https://openrouter.ai/api/v1/chat/completions",
}

class OpenAICompatAdapter:
    """
    Minimal adapter for any "OpenAI-compatible" chat completions API.
    You control:
      - LLM_PROVIDER           (openai | openrouter | azure-openai | ...)
      - OPENAI_BASE_URL        (optional; overrides default)
      - OPENAI_MODEL           (e.g., gpt-4o-mini / llama-3.1-70b-instruct / etc.)
      - OPENAI_API_KEY         (or AZURE_OPENAI_API_KEY if you prefer)
      - OPENAI_ORG_ID          (optional; OpenAI only)
      - OPENAI_EXTRA_HEADERS   (optional JSON string with extra headers)
    """

    def __init__(self):
        # determines what LLM provider to be using, the mojor decision maker
        # is the LLM_PROVIDER env var in the docker compose file, dafault is openai
        self.provider = os.getenv("LLM_PROVIDER", "openai").lower()
        base_url_override = os.getenv("OPENAI_BASE_URL", "").strip()
        if base_url_override:
            self.url = base_url_override
        else:
            self.url = DEFAULT_ENDPOINTS.get(self.provider, DEFAULT_ENDPOINTS["openai"])

        self.model = os.getenv("OPENAI_MODEL", "gpt-4o-mini")
        self.api_key = os.getenv("OPENAI_API_KEY") or os.getenv("AZURE_OPENAI_API_KEY") or ""
        self.org_id = os.getenv("OPENAI_ORG_ID", "").strip()

        # Extra headers if the provider needs them (e.g., OpenRouter: "HTTP-Referer", "X-Title")
        self.extra_headers = {}
        try:
            if os.getenv("OPENAI_EXTRA_HEADERS"):
                self.extra_headers = json.loads(os.getenv("OPENAI_EXTRA_HEADERS"))
        except Exception:
            self.extra_headers = {}

    def _headers(self):
        # Azure OpenAI expects header 'api-key' instead of 'Authorization: Bearer'
        if self.provider == "azure-openai":
            headers = {"Content-Type": "application/json", "api-key": self.api_key}
        else:
            headers = {"Content-Type": "application/json", "Authorization": f"Bearer {self.api_key}"}

        if self.org_id and self.provider == "openai":
            headers["OpenAI-Organization"] = self.org_id

        headers.update(self.extra_headers)
        return headers

    def _build_messages(self, prompt: str, schema_hint: Optional[str] = None):
        system = (
            "You are a SQL generator for PostgreSQL. "
            "Return ONLY a single SQL statement, no markdown, no explanation. "
            "Only generate read-only SELECT queries. Never use DML/DDL."
        )
        # prompt based guardrail added above
        if schema_hint:
            system += f" Database schema:\n{schema_hint}\n"

        user = f"Question: {prompt}\nReturn a single SELECT ...;"

        return [
            {"role": "system", "content": system},
            {"role": "user", "content": user}
        ]

    async def generate(self, prompt: str, schema_hint: Optional[str] = None) -> str:
        payload = {
            "model": self.model,
            "messages": self._build_messages(prompt, schema_hint),
            "temperature": 0.1,
            "max_tokens": 300,
        }

        # Azure OpenAI often uses a different base URL pattern:
        # e.g. https://{resource}.openai.azure.com/openai/deployments/{deployment}/chat/completions?api-version=2024-02-15-preview
        # In that case set OPENAI_BASE_URL to that full endpoint and OPENAI_MODEL to your deployment name.

        # the request for the LLM is sent
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(self.url, headers=self._headers(), json=payload)
            resp.raise_for_status()
            data = resp.json()

        # OpenAI-style response
        try:
            content = data["choices"][0]["message"]["content"].strip()
        except Exception:
            # Some providers differ slightly—fallbacks can be added if needed
            content = ""

        # Hard safety net: enforce SELECT-only and ending semicolon
        sql = content.split(";")[0].strip()
        if not sql.lower().startswith("select"):
            sql = "SELECT 1;"
        else:
            sql = sql + ";"

        return sql
