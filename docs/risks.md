# Risk & Readiness Catalog

## Database Access Risks
- **Network isolation / wrong host**: Ensure `postgres` is only reachable on the internal Docker network and `DB_URL` points to `postgres:5432` in Docker, `localhost` otherwise.
- **Credentials / roles**: Use a read-only DB role for the API. Rotate credentials via env vars.
- **Long-running or full-scan queries**: Enforce SQL `SELECT`-only and prefer `LIMIT`. Add server-side statement timeouts.
- **Connection pool exhaustion**: HikariCP max pool size tuned (default 5). Monitor and adjust.
- **Migrations drift**: Use Flyway for future schema changes.

## LLM Access Risks
- **Model not present offline**: Preload weights in `ollama` volume before going offline.
- **VRAM/CPU limits**: 8B class models typically need 10–12GB VRAM for best performance; quantized CPU fallbacks are slower.
- **Tokenization/context mismatch**: Cap prompt length; compress schema grounding; shard by schema if needed.
- **Adapter unavailability**: The `python-llm /health` must reflect backend status; backend selection via `LLM_BACKEND`.
- **Prompt injection / overreach**: Double-validate SQL in Python and Java; never give DB credentials to the LLM.

## Cross-Service Risks
- **Schema drift vs. grounding**: Regenerate DDL snapshot per request or on a schedule.
- **Error propagation**: Return structured errors from Python; log with correlation IDs.
- **Resource overuse**: Rate-limit `/api/query`. Add request budgets per user/token.

## Observability & Audit
- Log the question, final SQL, row count, duration, and user ID.
- Keep **no raw PII** in logs; redact values. Store examples for evaluation separately.

## Security Controls
- API authentication (API keys or JWT) on `/api/*` (add Spring Security later).
- Only `SELECT` execution path; block `;`-chaining and multi-statement queries.
- Statement timeout at the DB level, e.g. `SET statement_timeout = '10s'` for the API role.
