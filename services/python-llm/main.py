#docker compose up --build python-llm

import os
from typing import Optional, List, Dict
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field
from sqlglot import parse_one, exp
import json

current_rag_context = None
current_prompt = None
intuition_rags = []

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
    
class GenerateInferenceRequest(BaseModel):
    question: str
    context: str

class GenerateInferenceResponse(BaseModel):
    answer: str

class GenerateSQLResponse(BaseModel):
    response: str
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

    # Guardrail: non-SELECT statements are not allowed 
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

import os, sys, json, argparse, psycopg2, requests
from datetime import datetime
from psycopg2.extras import RealDictCursor

# --- config / env ---
RAG_PG_DSN  = os.getenv("RAG_PG_DSN",  "dbname=ragdb user=rag password=rag_pw host=rag-pg port=5432")
OLLAMA_URL  = os.getenv("OLLAMA_URL",  "http://nl2sql_ollama:11434")
EMBED_MODEL = os.getenv("EMBED_MODEL", "nomic-embed-text")   # you use nomic-embed-text (768-dim)

# --- helpers ---
def to_pgvector(vec):
    return "[" + ",".join(f"{float(x):.8f}" for x in vec) + "]"

def embed_query(text):
    r = requests.post(f"{OLLAMA_URL}/api/embeddings",
                      json={"model": EMBED_MODEL, "prompt": text}, timeout=60)
    if r.status_code >= 400:
        raise RuntimeError(f"Embeddings HTTP {r.status_code}: {r.text}")
    data = r.json()
    vec = data.get("embedding")
    if not isinstance(vec, list) or not vec:
        raise RuntimeError(f"Bad embedding payload: {data}")
    return vec

def mmr_select(cands, k, lam=0.5, per_item_cap=None):
    """
    cands: list of dicts with keys: id, item_id, chunk_ix, sim (higher better)
    returns selected list of indices into cands
    """
    selected, used_per_item = [], {}
    if not cands: 
        return selected
    # pick best first
    best_idx = max(range(len(cands)), key=lambda i: cands[i]["sim"])
    selected.append(best_idx)
    used_per_item[cands[best_idx]["item_id"]] = 1

    def same_item_penalty(i, selected, cands):
        for j in selected:
            if cands[i]["item_id"] == cands[j]["item_id"]:
                return 1.0   # found one; no need to continue scanning
        return 0.0
    
    while len(selected) < k and len(selected) < len(cands):
        best_i, best_score = None, -1e9
        for i in range(len(cands)):
            if i in selected: 
                continue
            # per-item cap
            if per_item_cap:
                if used_per_item.get(cands[i]["item_id"], 0) >= per_item_cap:
                    continue
            # MMR = λ * relevance - (1-λ) * redundancy
            score = lam*cands[i]["sim"] - (1.0-lam)*same_item_penalty(i, selected, cands)
            if score > best_score:
                best_score, best_i = score, i
        if best_i is None:
            break
        selected.append(best_i)
        used_per_item[cands[best_i]["item_id"]] = used_per_item.get(cands[best_i]["item_id"], 0) + 1
    return selected

def build_prompt(chunks, header=None, footer=None, cite=True):
    """
    chunks: list of dicts with keys (rank, kind, name, chunk_ix, text)
    """
    lines = []
    if header:
        lines.append(header.strip())
        lines.append("\n--- Retrieved context ---")
        
    for c in chunks:
        tag = f"[{c['kind']}:{c['name']}#{c['chunk_ix']}]" if cite else ""
        lines.append(f"{tag}\n{c['chunk_text']}")
        
    if footer:
        lines.append("\n--- Instructions ---")
        lines.append(footer.strip())
    return "\n\n".join(lines)

def trim(s, n=200):
    s = s.replace("\n"," ")
    return s if len(s)<=n else s[:n-1]+"…"

def search_rag_context(question: str, only_info_filed = False, for_intuition = False, same_query = False) -> Optional[str]:
    global intuirion_rags
    # arbitrary value of what is the least amount of similarity acceptable, found through testing
    min_sim = 0.60
    kinds = "table,column,key,info"
    candidates = 80
    topk = 12
    per_item_cap = 8
    mmr = 0.5

    if not same_query:
        print(">>> embedding query …")
        qvec = embed_query(question)
        qvec_txt = to_pgvector(qvec)

        kinds = [k.strip() for k in kinds.split(",") if k.strip()]
        kinds_sql = "(" + ",".join(["%s"]*len(kinds)) + ")"

        conn = psycopg2.connect(RAG_PG_DSN)
        cur  = conn.cursor(cursor_factory=RealDictCursor)

        """ # knobs for ANN
        if args.ivf_probes:
            cur.execute("SET LOCAL ivfflat.probes = %s;", (args.ivf_probes,))
        if args.hnsw_ef:
            cur.execute("SET LOCAL hnsw.ef_search = %s;", (args.hnsw_ef,))
        """
        # WHERE filters to check where we will be searching for similarities
        where = ["i.kind = ANY(%s)"]
        params = [kinds]
        if only_info_filed:
            where.append("(i.kind <> 'info' OR (i.kind='info' AND (i.metadata->>'dataset') = %s))")
            # ? what now
            params.append("filed")

        where_sql = " AND ".join(where)
        # cosine distance: smaller is better; similarity = 1 - dist
        sql = f"""
        SELECT i.id AS item_id, i.kind, i.name, c.chunk_ix, c.chunk_text,
                (c.embedding <=> %s::vector) AS dist,
                i.updated_at
        FROM rag.rag_chunk c
        JOIN rag.rag_item  i ON i.id = c.item_id
        WHERE {where_sql}
        ORDER BY c.embedding <=> %s::vector
        LIMIT %s
        """
        params_sql = [qvec_txt] + params + [qvec_txt, candidates]
        cur.execute(sql, params_sql)
        rows = cur.fetchall()
        conn.close()

        # score conversion
        cands = []
        for r in rows:
            dist = float(r["dist"])
            sim  = 1.0 - dist  # cosine distance -> similarity
            print("sim:", sim)
            if sim > min_sim:
                intuition_rags.append({
                    "item_id": r["item_id"],
                    "kind":    r["kind"],
                    "name":    r["name"],
                    "chunk_ix":r["chunk_ix"],
                    "chunk_text": r["chunk_text"],
                    "dist":    dist,
                    "sim":     sim,
                })
            cands.append({
                "item_id": r["item_id"],
                "kind":    r["kind"],
                "name":    r["name"],
                "chunk_ix":r["chunk_ix"],
                "chunk_text": r["chunk_text"],
                "dist":    dist,
                "sim":     sim,
            })

    if not for_intuition:
        # rerank with MMR + per-item cap
        sel_idxs = mmr_select(cands, k=topk, lam=mmr, per_item_cap=per_item_cap)
        final = [cands[i] for i in sel_idxs]
    else:
        sel_idxs = mmr_select(intuition_rags, k=topk, lam=mmr, per_item_cap=per_item_cap)
        final = [intuition_rags[i] for i in sel_idxs]

    # if decide to use ANN
    """ 
    if args.ivf_probes: print(f"ivfflat.probes={args.ivf_probes}")
    if args.hnsw_ef: print(f"hnsw.ef_search={args.hnsw_ef}")
    """
        
    for rnk, c in enumerate(final, 1):
        print(f"{rnk:>4}  {c['sim']:.4f}  {c['dist']:.4f}  {c['kind']:<6}  {c['name'][:30]:<30}  {c['chunk_ix']:>3}  {trim(c['chunk_text'], 60)}")

    # make prompt block
    prompt = build_prompt(
        [{"rank": i+1, **c} for i,c in enumerate(final)],
    )

    return prompt if final else None

@app.post("/v1/generate-sql", response_model=GenerateSQLResponse)
async def generate_sql(req: GenerateSQLRequest):
    global current_rag_context, current_prompt
    if req.question == current_prompt:
        rag_context = current_rag_context or "(none)"
        print("=== RAG Context ===")
        print(current_rag_context)
    else:
        rag_context = search_rag_context(req.question) or "(none)"
        print(">>> SQL Side RAG context for inference (not cached) (this indicates the the sql query generated before this call was for a different prompt):")
        print(rag_context)
        current_rag_context = rag_context
    current_prompt = req.question
        
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

Context from RAG (optional):
{rag_context or '(none)'}
"""
    adapter = build_adapter()
    sql_text = await adapter.sql_generate(prompt, schema_hint=req.schema_ddl)

    sql = sql_text.strip().strip('`')
    if sql.lower().startswith("sql"):
        sql = sql.split(":", 1)[-1].strip()

    try:
        warnings = validate_safe_select(sql)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Validation failed: {e}")

    return GenerateSQLResponse(response=sql, warnings=warnings)

@app.post("/v1/generate-intuition", response_model=GenerateInferenceResponse)
async def generate_intuition(req: GenerateInferenceRequest):
    """
    Given a question, context, and SQL results (as JSON), draft a short answer.
    """
    global current_rag_context, current_prompt
    
    if req.question == current_prompt:
        rag_context = search_rag_context(req.question, for_intuition= True, same_query=True) or "(none)"
    else: 
        rag_context = search_rag_context(req.question, for_intuition= True) or "(none)"
    print(">>> INTUITION Side RAG context for inference (not cached) (this indicates the the sql query generated before this call was for a different prompt):")
    print(rag_context)
    current_rag_context = rag_context
    current_prompt = req.question
        
    prompt = (
        "You are a data assistant. Given a natural language question and the result rows (JSON), "
        "write a short, clear answer in one or two sentences. Do not talk about SQL in your answer you are not meant to create queries.\n\n"
        f"Question: {req.question}\nContext: {rag_context}"
    )
    adapter = build_adapter()
    text = await adapter.sql_generate(prompt)
    return GenerateInferenceResponse(answer=text.strip())

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
