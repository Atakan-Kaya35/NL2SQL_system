"""
This script attains the schema information fro the source database localed at APP_PG_DNG
and retreives the schema of the table to commit them tı the RAG_PG_DNG database for RAG use.
"""

# ingest_schema.py
import os, json, requests, psycopg2, argparse, csv
from psycopg2.extras import execute_values
from datetime import datetime

APP_PG_DSN = os.getenv("APP_PG_DSN")   # source DB with real tables
RAG_PG_DSN = os.getenv("RAG_PG_DSN")   # target DB that has rag.rag_item / rag.rag_chunk
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://nl2sql_ollama:11434")
EMBED_MODEL = os.getenv("EMBED_MODEL", "nomad-embed-text")  # pick one and match dimension in DB

# ----------------- Schema ingestion -------------------

def embed(texts):
    outs = []
    for t in texts:  # one call per chunk
        r = requests.post(
            f"{OLLAMA_URL}/api/embeddings",
            json={"model": EMBED_MODEL, "prompt": t},  # your server expects 'prompt'
            timeout=120
        )
        if r.status_code >= 400:
            raise RuntimeError(f"Embeddings HTTP {r.status_code}: {r.text}")
        data = r.json()
        vec = data.get("embedding")
        if not isinstance(vec, list) or not vec:
            raise RuntimeError(f"No/empty embedding in response: {data}")
        outs.append(vec)
    return outs

def to_pgvector(vec):
    # avoids needing a special psycopg2 adapter; Postgres will cast this text to vector
    return "[" + ",".join(f"{float(x):.8f}" for x in vec) + "]"

def connect(dsn):
    if not dsn:
        raise RuntimeError("Missing DSN (APP_PG_DSN or RAG_PG_DSN)")
    return psycopg2.connect(dsn)

def fetch_schema(app_conn):
    cur = app_conn.cursor()
    cur.execute("""
      SELECT c.table_schema, c.table_name, c.column_name, c.data_type
      FROM information_schema.columns c
      JOIN information_schema.tables t
        ON c.table_schema = t.table_schema AND c.table_name = t.table_name
      WHERE t.table_type = 'BASE TABLE'
        AND c.table_schema NOT IN ('pg_catalog','information_schema','pg_toast','rag')
      ORDER BY c.table_schema, c.table_name, c.ordinal_position;
    """)
    col_rows = cur.fetchall()

    cur.execute("""
      SELECT tc.table_schema, tc.table_name, kc.column_name, tc.constraint_type
      FROM information_schema.table_constraints tc
      JOIN information_schema.key_column_usage kc
        ON tc.constraint_name = kc.constraint_name
       AND tc.table_schema = kc.table_schema
       AND tc.table_name  = kc.table_name
      WHERE tc.constraint_type IN ('PRIMARY KEY','FOREIGN KEY')
        AND tc.table_schema NOT IN ('pg_catalog','information_schema','pg_toast','rag');
    """)
    key_rows = cur.fetchall()
    return col_rows, key_rows

def upsert_item(rag_conn, kind, name, body, metadata):
    cur = rag_conn.cursor()
    # ensure schema-qualified table names
    cur.execute("""
      INSERT INTO rag.rag_item(kind,name,body,metadata)
      VALUES (%s,%s,%s,%s)
      ON CONFLICT (id) DO NOTHING
      RETURNING id;
    """, (kind, name, body, json.dumps(metadata)))
    res = cur.fetchone()
    if res:
        rag_conn.commit()
        return res[0]

    cur.execute("SELECT id FROM rag.rag_item WHERE kind=%s AND name=%s", (kind, name))
    got = cur.fetchone()
    if got:
        cur.execute("""
          UPDATE rag.rag_item
             SET body=%s, metadata=%s, version=version+1
           WHERE id=%s
        """, (body, json.dumps(metadata), got[0]))
        rag_conn.commit()
        return got[0]

    rag_conn.commit()
    return got[0]  # type: ignore

def chunk_text(s, chunk_chars=1500):
    s = s.strip()
    if len(s) <= chunk_chars:
        return [s]
    return [s[i:i+chunk_chars] for i in range(0, len(s), chunk_chars)]

def upsert_chunks(rag_conn, item_id, chunks, embeddings):
    cur = rag_conn.cursor()
    rows = []
    for ix, (txt, emb) in enumerate(zip(chunks, embeddings)):
        rows.append((item_id, ix, txt, to_pgvector(emb)))  # string form; cast below

    cur.execute("DELETE FROM rag.rag_chunk WHERE item_id=%s", (item_id,))
    execute_values(cur,
        "INSERT INTO rag.rag_chunk(item_id,chunk_ix,chunk_text,embedding) VALUES %s",
        rows,
        template="(%s,%s,%s,%s::vector)")  # cast text -> vector
    rag_conn.commit()

def ingest():
    app_conn = connect(APP_PG_DSN)
    rag_conn = connect(RAG_PG_DSN)

    col_rows, key_rows = fetch_schema(app_conn)
    print(f"[DEBUG] columns rows: {len(col_rows)}, keys rows: {len(key_rows)}")
    if not col_rows:
        raise RuntimeError("No columns found in source DB (check APP_PG_DSN and filters).")

    # ---------- tables ----------
    by_table = {}
    for sch, tbl, col, dtype in col_rows:
        by_table.setdefault((sch, tbl), []).append((col, dtype))

    for (sch, tbl), cols_for_table in by_table.items():
        name = f"{sch}.{tbl}"
        body = "Table " + name + " has columns:\n" + "\n".join(
            f"- {c} ({t})" for c, t in cols_for_table
        )
        item_id = upsert_item(rag_conn, "table", name, body, {"schema": sch, "table": tbl})
        chunks = chunk_text(body)
        embs = embed(chunks)
        upsert_chunks(rag_conn, item_id, chunks, embs)

    # ---------- columns ----------
    for sch, tbl, col, dtype in col_rows:
        name = f"{sch}.{tbl}.{col}"
        body = f"Column {name} has type {dtype}."
        item_id = upsert_item(rag_conn, "column", name, body,
                              {"schema": sch, "table": tbl, "column": col, "type": dtype})
        embs = embed([body])
        upsert_chunks(rag_conn, item_id, [body], embs)

    # ---------- keys ----------
    for sch, tbl, col, ctype in key_rows:
        name = f"{sch}.{tbl}.{ctype}.{col}"
        body = f"{ctype} on {sch}.{tbl} column {col}."
        item_id = upsert_item(rag_conn, "key", name, body,
                              {"schema": sch, "table": tbl, "column": col, "type": ctype})
        embs = embed([body])
        upsert_chunks(rag_conn, item_id, [body], embs)

    app_conn.close()
    rag_conn.close()

RAG_PG_DSN = os.getenv("RAG_PG_DSN", "dbname=ragdb user=rag password=ragpass host=rag-pg port=5432")

def q(cur, sql, params=None):
    cur.execute(sql, params or ())
    cols = [c.name for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def ensure_dir(p):
    os.makedirs(p, exist_ok=True)

def write_csv(path, rows):
    if not rows:
        # still write headerless empty file to signal “no rows”
        open(path, "w", encoding="utf-8", newline="").close()
        return
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
        w.writeheader()
        for r in rows:
            w.writerow(r)

# end ----------------- Schema ingestion -------------------


# ----------------- Audit RAG DB -------------------
def audit_rag():
    """
    Creates csv and json file through auditing the RAG DB.
    Returns the entire database in the format that it is kept into files in the ./out directory.
    """
    ap = argparse.ArgumentParser(description="Audit snapshot of RAG DB (items + chunks + full texts)")
    ap.add_argument("--dsn", default=RAG_PG_DSN, help="Postgres DSN for RAG DB")
    ap.add_argument("--outdir", default="./out", help="Output directory")
    ap.add_argument("--preview", type=int, default=200, help="Preview char length printed to console")
    args = ap.parse_args()

    ensure_dir(args.outdir)

    conn = psycopg2.connect(args.dsn)
    cur  = conn.cursor()

    # Embedding column type / dimension
    embedding_info = q(cur, """
      SELECT format_type(a.atttypid, a.atttypmod) AS embedding_type
      FROM pg_attribute a
      WHERE a.attrelid='rag.rag_chunk'::regclass AND a.attname='embedding';
    """)
    emb_type = embedding_info[0]["embedding_type"] if embedding_info else "unknown"

    # All items (human-authored body)
    items = q(cur, """
      SELECT id, kind, name, body, metadata, version, created_at, updated_at
      FROM rag.rag_item
      ORDER BY kind, name;
    """)

    # All chunks (what retriever uses)
    chunks = q(cur, """
      SELECT item_id, chunk_ix, chunk_text, char_length(chunk_text) AS chars,
             created_at, updated_at
      FROM rag.rag_chunk
      ORDER BY item_id, chunk_ix;
    """)

    # Items with no chunks (invisible to retriever)
    missing = q(cur, """
      SELECT i.id, i.kind, i.name
      FROM rag.rag_item i
      LEFT JOIN rag.rag_chunk c ON c.item_id = i.id
      WHERE c.id IS NULL
      ORDER BY i.kind, i.name;
    """)

    # Full text per item (chunks stitched in order)
    full_texts = q(cur, """
      SELECT i.id, i.kind, i.name,
             COALESCE(string_agg(c.chunk_text, '' ORDER BY c.chunk_ix), '') AS full_text,
             COUNT(c.id) AS chunk_count
      FROM rag.rag_item i
      LEFT JOIN rag.rag_chunk c ON c.item_id = i.id
      GROUP BY i.id, i.kind, i.name
      ORDER BY i.kind, i.name;
    """)

    conn.close()

    # Build a JSON “context snapshot” (what you’ll likely pass to the LLM)
    chunks_by_item = {}
    for c in chunks:
        chunks_by_item.setdefault(c["item_id"], []).append({
            "chunk_ix": c["chunk_ix"],
            "chunk_text": c["chunk_text"],
            "chars": c["chars"],
            "created_at": c["created_at"].isoformat() if hasattr(c["created_at"], "isoformat") else c["created_at"],
            "updated_at": c["updated_at"].isoformat() if hasattr(c["updated_at"], "isoformat") else c["updated_at"],
        })

    snapshot = []
    for it in items:
        snapshot.append({
            "id": it["id"],
            "kind": it["kind"],
            "name": it["name"],
            "version": it["version"],
            "created_at": it["created_at"].isoformat() if hasattr(it["created_at"], "isoformat") else it["created_at"],
            "updated_at": it["updated_at"].isoformat() if hasattr(it["updated_at"], "isoformat") else it["updated_at"],
            "metadata": it["metadata"],  # JSONB comes out as dict via psycopg2
            "item_body": it["body"],
            "chunks": sorted(chunks_by_item.get(it["id"], []), key=lambda x: x["chunk_ix"]),
            "full_text": next((ft["full_text"] for ft in full_texts if ft["id"] == it["id"]), ""),
            "chunk_count": next((ft["chunk_count"] for ft in full_texts if ft["id"] == it["id"]), 0),
        })

    # ---------- Save to files ----------
    ts = datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    write_csv(os.path.join(args.outdir, f"rag_items_{ts}.csv"), items)
    write_csv(os.path.join(args.outdir, f"rag_chunks_{ts}.csv"), chunks)

    with open(os.path.join(args.outdir, f"rag_snapshot_{ts}.json"), "w", encoding="utf-8") as f:
        json.dump({
            "generated_at_utc": ts,
            "embedding_column_type": emb_type,
            "counts": {
                "items": len(items),
                "chunks": len(chunks),
                "missing_items": len(missing),
            },
            "missing_items": missing,
            "items": snapshot
        }, f, ensure_ascii=False, indent=2)

    # ---------- Console summary (sanity) ----------
    kinds = {}
    for it in items: kinds[it["kind"]] = kinds.get(it["kind"], 0) + 1

    print("\n=== RAG AUDIT SNAPSHOT ===")
    print(f"Embedding column type: {emb_type}")
    print(f"Items: {len(items)} | Chunks: {len(chunks)} | Items without chunks: {len(missing)}")
    print("By kind:", ", ".join([f"{k}={v}" for k,v in sorted(kinds.items())]))
    if missing:
        print("\nItems missing chunks:")
        for m in missing[:10]:
            print(f"  - [{m['kind']}] {m['name']} (id={m['id']})")
        if len(missing) > 10:
            print(f"  ... and {len(missing)-10} more")
    print("\nSample (first 5 items) with previews:")
    for it in snapshot[:5]:
        preview = (it["full_text"] or it["item_body"] or "")[:args.preview].replace("\n"," ")
        print(f"  - [{it['kind']}] {it['name']} | chunks={it['chunk_count']} | preview=\"{preview}\"")

    print(f"\nFiles written in {os.path.abspath(args.outdir)}:")
    print(f"  - rag_items_{ts}.csv")
    print(f"  - rag_chunks_{ts}.csv")
    print(f"  - rag_snapshot_{ts}.json")

# end of ----------------- Audit RAG DB -------------------

if __name__ == "__main__":
    #ingest()
    #audit_rag()
    pass