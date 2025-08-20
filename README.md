# NL2SQL Stack Skeleton (Spring Boot + FastAPI + Postgres)

## Quick Start
```bash
# In the project root:
docker compose up --build
# Then:
curl -X POST http://localhost:8080/api/query -H "Content-Type: application/json" -d '{"question":"List last 5 missions by launch date"}'
curl http://localhost:8080/api/health
curl http://localhost:8000/health
```

## Profiles
- **Local**: run services separately (Postgres locally, Python on 8000, Java on 8080).
- **Docker**: use `docker compose up` (default).

## Environment
- `PY_SERVICE_BASEURL` (Java) defaults to `http://python-llm:8000` under docker.
- `LLM_BACKEND` (Python) options: `ollama`, `openai`, `mock`.

## Notes
- By default, Python only **generates and validates** SQL. Java executes queries via JDBC.
- Double-validation: Python uses `sqlglot` and Java uses `JSQLParser` to enforce `SELECT`-only.
- Add `pgvector` usage later for RAG grounding.
