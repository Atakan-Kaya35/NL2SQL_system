import os
from typing import Optional, List, Dict
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlglot import parse_one, exp
import json

def build_adapter(name: Optional[str]):
    name = (name or os.getenv("LLM_BACKEND", "mock")).lower()
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
    backend: Optional[str] = None

class DraftAnswerResponse(BaseModel):
    answer: str

def _has_node(tree, candidate):
    """
    True if the parsed tree contains any node of the 'candidate' expression class.
    If the class doesn't exist in this sqlglot build, returns False.
    """
    if candidate is None:
        return False
    return any(isinstance(node, candidate) for node in tree.walk())

def validate_safe_select(sql: str) -> list[str]:
    """
    Guardrail: only allow read-only queries.
    Return a list of warnings (empty means OK). Raise ValueError for hard blocks.
    """
    try:
        tree = parse_one(sql, read=None)  # let sqlglot detect dialect
    except Exception as e:
        raise ValueError(f"Could not parse SQL: {e}")

    # Try to resolve classes that may or may not exist in this sqlglot version
    Insert     = getattr(exp, "Insert", None)
    Update     = getattr(exp, "Update", None)
    Delete     = getattr(exp, "Delete", None)
    Drop       = getattr(exp, "Drop", None)
    Alter      = getattr(exp, "Alter", None)        # might be None
    AlterTable = getattr(exp, "AlterTable", None)   # some versions use this
    Truncate   = getattr(exp, "Truncate", None)
    Create     = getattr(exp, "Create", None)
    Execute    = getattr(exp, "Execute", None)      # e.g., CALL/EXECUTE in some dialects
    Transaction= getattr(exp, "Transaction", None)  # BEGIN/COMMIT/ROLLBACK
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

    # Optional: warn if multiple SELECTs / CTE with writes (defensive)
    warnings = []
    if not any(isinstance(node, exp.Select) for node in tree.find_all(exp.Select)):
        warnings.append("No SELECT found; query may not return rows.")

    return warnings

@app.post("/v1/generate-sql", response_model=GenerateSQLResponse)
async def generate_sql(req: GenerateSQLRequest):
    """
    Place where the prompt for the LLM to create the SQL query is created and submitted.
    """
    adapter = build_adapter(req.backend)
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
    text = await adapter.generate(prompt)
    sql = text.strip().strip('`')
    # If adapters return "SQL: ..." normalize
    if sql.lower().startswith("sql"):
        sql = sql.split(":", 1)[-1].strip()
    try:
        warnings = validate_safe_select(sql)
    except ValueError as e:
        # Client error: the request led to a disallowed query
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        # Unexpected server error
        raise HTTPException(status_code=500, detail=f"Validation failed: {e}")

    return GenerateSQLResponse(sql=sql, warnings=warnings)

@app.post("/v1/draft-answer", response_model=DraftAnswerResponse)
async def draft_answer(req: DraftAnswerRequest):
    backend_name = (req.backend or os.getenv("LLM_BACKEND", "mock")).lower()
    if backend_name == "mock":
        count = len(req.rows)
        sample = req.rows[0] if count > 0 else {}
        return DraftAnswerResponse(answer=f"Found {count} rows. Example: {sample}")
    # real LLM summarize
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
