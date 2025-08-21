import os
from typing import Optional, List, Dict
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlglot import parse_one, exp
import json

# we will choose adapter per-request using build_adapter()
def build_adapter():
    name = os.getenv("LLM_BACKEND", "mock").lower()
    if name == "ollama":
        from adapters.ollama import OllamaAdapter
        return OllamaAdapter()
    elif name == "openai":
        from adapters.openai_compat import OpenAICompatAdapter
        return OpenAICompatAdapter()
    else:
        from adapters.mock import MockAdapter
        return MockAdapter()

app = FastAPI(title="NL2SQL LLM Service", version="0.2.0")

class GenerateSQLRequest(BaseModel):
    question: str = Field(..., description="Natural language question")
    schema_ddl: Optional[str] = Field(None, description="Optional DDL snippet")
    context: Optional[str] = Field(None, description="Optional extra context (RAG)")
    backend: Optional[str] = Field(None, description="Backend override: mock|ollama|openai")

class GenerateSQLResponse(BaseModel):
    sql: str
    warnings: List[str] = []

class DraftAnswerRequest(BaseModel):
    question: str
    sql: str
    rows: List[Dict]

class DraftAnswerResponse(BaseModel):
    answer: str

def _has_node(tree, candidate):
    if candidate is None:
        return False
    return any(isinstance(node, candidate) for node in tree.walk())

def validate_safe_select(sql: str) -> list[str]:
    try:
        tree = parse_one(sql, read=None)
    except Exception as e:
        raise ValueError(f"Could not parse SQL: {e}")

    Insert     = getattr(exp, "Insert", None)
    Update     = getattr(exp, "Update", None)
    Delete     = getattr(exp, "Delete", None)
    Drop       = getattr(exp, "Drop", None)
    Alter      = getattr(exp, "Alter", None)
    AlterTable = getattr(exp, "AlterTable", None)
    Truncate   = getattr(exp, "Truncate", None)
    Create     = getattr(exp, "Create", None)
    Execute    = getattr(exp, "Execute", None)
    Transaction= getattr(exp, "Transaction", None)
    InsertOverwrite = getattr(exp, "InsertOverwrite", None)

    hard_block = any([
        _has_node(tree, Insert),
        _has_node(tree, InsertOverwrite),
        _has_node(tree, Update),
        _has_node(tree, Delete),
        _has_node(tree, Drop),
        _has_node(tree, Alter),
        _has_node(tree, AlterTable),
        _has_node(tree, Truncate),
        _has_node(tree, Create),
        _has_node(tree, Execute),
        _has_node(tree, Transaction),
    ])
    if hard_block:
        raise ValueError("Only SELECT queries are allowed by the guardrail.")

    warnings = []
    if not any(isinstance(node, exp.Select) for node in tree.find_all(exp.Select)):
        warnings.append("No SELECT found; query may not return rows.")
    return warnings

@app.post("/v1/generate-sql", response_model=GenerateSQLResponse)
async def generate_sql(req: GenerateSQLRequest):
    """
    Prompt the LLM to create a single, safe PostgreSQL SELECT query.
    """
    prompt = f"""You are an expert data analyst producing a single, safe PostgreSQL SELECT query.
- Only output a single SQL statement; no explanations.
- Use existing columns; do not invent.
- Prefer LIMIT 100 unless the question requests exact counts.
Question: {req.question}

Schema (may be partial):
{req.schema_ddl or '(schema omitted)'}

Context (optional):
{req.context or '(none)'}
"""
    adapter = build_adapter()
    sql_text = await adapter.generate(prompt, schema_hint=req.schema_ddl)

    sql = sql_text.strip().strip('`')
    if sql.lower().startswith("sql"):
        sql = sql.split(":", 1)[-1].strip()

    try:
        warnings = validate_safe_select(sql)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Validation failed: {e}")

    return GenerateSQLResponse(sql=sql, warnings=warnings)

@app.post("/v1/draft-answer", response_model=DraftAnswerResponse)
async def draft_answer(req: DraftAnswerRequest):
    backend_name = os.getenv("LLM_BACKEND", "mock").lower()
    if backend_name == "mock":
        count = len(req.rows)
        sample = req.rows[0] if count > 0 else {}
        return DraftAnswerResponse(answer=f"Found {count} rows. Example: {sample}")

    adapter = build_adapter(backend_name)
    prompt = (
        "You are a data assistant. Given a natural language question, the SQL used, and the result rows (JSON), "
        "write a short, clear answer in one or two sentences.\n\n"
        f"Question: {req.question}\nSQL: {req.sql}\nRows JSON: {json.dumps(req.rows)[:4000]}"
    )
    text = await adapter.generate(prompt)
    return DraftAnswerResponse(answer=text.strip())

@app.get("/health")
async def health():
    try:
        _ = await build_adapter(None).generate("Return OK")
        llm_ok = True
    except Exception:
        llm_ok = False
    return {"llm_ok": llm_ok}
