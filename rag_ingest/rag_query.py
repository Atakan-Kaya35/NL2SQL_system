"""
docker compose run --rm --entrypoint sh rag-ingest -lc "pip install --no-cache-dir psycopg2-binary requests >/dev/null && python -u /app/rag_query.py --query 'What does Sentinel-2 provide?' --dataset sat-info --kinds info,table,column --candidates 80 --topk 8 --per-item-cap 2 --mmr 0.6 --ivf-probes 10 --save-prompt /app/out/last_prompt.txt"
"""


# rag_query.py
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

def main():
    ap = argparse.ArgumentParser(description="Interactive RAG retrieval harness (tweak knobs & inspect)")
    ap.add_argument("--query", required=True, help="natural language question")
    ap.add_argument("--dataset", help="filter: only kind='info' with this dataset id")
    ap.add_argument("--kinds", default="table,column,key,info",
                    help="comma list of kinds to include (default: table,column,key,info)")
    ap.add_argument("--candidates", type=int, default=80, help="initial vector candidates (pre-rerank)")
    ap.add_argument("--topk", type=int, default=12, help="final K after reranking")
    ap.add_argument("--per-item-cap", type=int, default=2, help="max chunks per item in final set")
    ap.add_argument("--mmr", type=float, default=0.5, help="MMR lambda (0..1). Higher favors relevance, lower diversity.")
    ap.add_argument("--min-sim", type=float, default=None, help="drop results with sim below this (sim=1-dist for cosine)")
    ap.add_argument("--ops", default="cosine", choices=["cosine"], help="distance opclass (currently cosine)")
    ap.add_argument("--ivf-probes", type=int, default=None, help="SET ivfflat.probes for this session")
    ap.add_argument("--hnsw-ef", type=int, default=None, help="SET hnsw.ef_search for this session")
    ap.add_argument("--save-prompt", help="write final context to this path")
    ap.add_argument("--show-json", action="store_true", help="print JSON of final chunks")
    args = ap.parse_args()

    print(">>> embedding query …")
    qvec = embed_query(args.query)
    qvec_txt = to_pgvector(qvec)

    kinds = [k.strip() for k in args.kinds.split(",") if k.strip()]
    kinds_sql = "(" + ",".join(["%s"]*len(kinds)) + ")"

    conn = psycopg2.connect(RAG_PG_DSN)
    cur  = conn.cursor(cursor_factory=RealDictCursor)

    # knobs for ANN
    if args.ivf_probes:
        cur.execute("SET LOCAL ivfflat.probes = %s;", (args.ivf_probes,))
    if args.hnsw_ef:
        cur.execute("SET LOCAL hnsw.ef_search = %s;", (args.hnsw_ef,))

    # WHERE filters to check where we will be searching for similarities
    where = ["i.kind = ANY(%s)"]
    params = [kinds]
    if args.dataset:
        where.append("(i.kind <> 'info' OR (i.kind='info' AND (i.metadata->>'dataset') = %s))")
        params.append(args.dataset)

    where_sql = " AND ".join(where)
    # cosine distance: smaller is better; similarity = 1 - dist
    sql = f"""
      SELECT i.id AS item_id, i.kind, i.name, c.chunk_ix, c.chunk_text,
             (c.embedding <-> %s::vector) AS dist,
             i.updated_at
      FROM rag.rag_chunk c
      JOIN rag.rag_item  i ON i.id = c.item_id
      WHERE {where_sql}
      ORDER BY c.embedding <-> %s::vector
      LIMIT %s
    """
    params_sql = [qvec_txt] + params + [qvec_txt, args.candidates]
    cur.execute(sql, params_sql)
    rows = cur.fetchall()
    conn.close()

    # score conversion
    cands = []
    for r in rows:
        dist = float(r["dist"])
        sim  = 1.0 - dist  # cosine distance -> similarity
        if args.min_sim is not None and sim < args.min_sim:
            continue
        cands.append({
            "item_id": r["item_id"],
            "kind":    r["kind"],
            "name":    r["name"],
            "chunk_ix":r["chunk_ix"],
            "chunk_text": r["chunk_text"],
            "dist":    dist,
            "sim":     sim,
        })

    # rerank with MMR + per-item cap
    sel_idxs = mmr_select(cands, k=args.topk, lam=args.mmr, per_item_cap=args.per_item_cap)
    final = [cands[i] for i in sel_idxs]

    # debug table
    print("\n=== Retrieval Debug ===")
    print(f"Query: {args.query}")
    print(f"Filters: kinds={kinds} dataset={args.dataset or '(any)'}")
    if args.ivf_probes: print(f"ivfflat.probes={args.ivf_probes}")
    if args.hnsw_ef: print(f"hnsw.ef_search={args.hnsw_ef}")
    print(f"Candidates: {len(rows)}  →  After threshold: {len(cands)}  →  Final: {len(final)}")
    print("\nRank  sim     dist    kind    name                              ix   preview")
    print("----  ------  ------  ------  --------------------------------  ---  -------------------------------")
    for rnk, c in enumerate(final, 1):
        print(f"{rnk:>4}  {c['sim']:.4f}  {c['dist']:.4f}  {c['kind']:<6}  {c['name'][:30]:<30}  {c['chunk_ix']:>3}  {trim(c['chunk_text'], 60)}")

    # make prompt block
    prompt = build_prompt(
        [{"rank": i+1, **c} for i,c in enumerate(final)],
        header="Use only the facts below. If something is missing, say so.",
        footer="Answer succinctly; cite sources like [kind:name#ix] when you use them."
    )

    if args.save_prompt:
        with open(args.save_prompt, "w", encoding="utf-8") as f:
            f.write(prompt)
        print(f"\nSaved prompt context to: {os.path.abspath(args.save_prompt)}")

    if args.show_json:
        print("\n=== JSON ===")
        print(json.dumps(final, ensure_ascii=False, indent=2))

    print("\n--- Prompt Context Preview ---\n")
    print(trim(prompt, 800))
    print("\n(Use --save-prompt to write the full context to a file.)")

if __name__ == "__main__":
    main()
