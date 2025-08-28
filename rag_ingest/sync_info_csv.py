"""
There is not a docker container for the rag-ingest service in the docker compose at the moment
This means that he psychopg2 needs to be installed and this python script must be run in the same command
progressively or a container with the module must be set up for the code to work

docker compose run --rm --entrypoint sh rag-ingest -lc " pip install --no-cache-dir psycopg2-binary requests >/dev/null && python -u /app/sync_info_csv.py --dataset sat-info --desired /app/info_items.csv --export-current /app/out/sat-info.current.csv --audit-json /app/out/sat-info.audit.json --apply"
"""


# sync_info_csv.py
import os, re, csv, json, argparse, psycopg2, requests
from datetime import datetime
from psycopg2.extras import execute_values

# --- env / defaults ---
RAG_PG_DSN  = os.getenv("RAG_PG_DSN",  "dbname=ragdb user=rag password=rag_pw host=rag-pg port=5432")
OLLAMA_URL  = os.getenv("OLLAMA_URL",  "http://ollama:11434")
EMBED_MODEL = os.getenv("EMBED_MODEL", "nomic-embed-text")   # 768-dim
CHUNK_CHARS = int(os.getenv("CHUNK_CHARS", "1500"))          # simple, char-based chunking

# --- helpers ---
ROW_NAME_FMT = "info::{dataset}::row::{idx}"  # 0-based index
ROW_NAME_RE  = re.compile(r"^info::(?P<ds>[^:]+)::row::(?P<ix>\d+)$")

def normalize_text(s: str) -> str:
    # A light normalization for diffing (no accidental updates due to whitespace/CRLF)
    return "\n".join(line.rstrip() for line in s.strip().replace("\r\n", "\n").replace("\r", "\n").split("\n")).strip()

def chunk_text(s: str, chunk_chars: int = CHUNK_CHARS):
    s = s.strip()
    if len(s) <= chunk_chars:
        return [s] if s else []
    return [s[i:i+chunk_chars] for i in range(0, len(s), chunk_chars)]

def to_pgvector(vec):
    # Send as text to be cast server-side to vector.
    return "[" + ",".join(f"{float(x):.8f}" for x in vec) + "]"

def embed_one(prompt: str):
    r = requests.post(f"{OLLAMA_URL}/api/embeddings",
                      json={"model": EMBED_MODEL, "prompt": prompt}, timeout=120)
    if r.status_code >= 400:
        raise RuntimeError(f"Embeddings HTTP {r.status_code}: {r.text}")
    data = r.json()
    vec = data.get("embedding")
    if not isinstance(vec, list) or not vec:
        raise RuntimeError(f"No/empty embedding in response: {data}")
    return vec

def embed_chunks(chunks):
    return [embed_one(c) for c in chunks]

def q(cur, sql, params=None):
    # execute sql querry and return list of dicts
    cur.execute(sql, params or ())
    cols = [c.name for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

def load_desired_csv(path, has_header=False, dedupe=False):
    rows = []
    # put the first rows of the first column into a list
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        rdr = csv.reader(f)
        for i, rec in enumerate(rdr):
            if i == 0 and has_header:
                continue
            if not rec:
                continue
            body = (rec[0] or "").strip()
            if not body:
                continue
            rows.append(body)
    
    # deduplicate if requested
    if dedupe:
        seen, out = set(), []
        for b in rows:
            key = normalize_text(b)
            if key in seen:  # skip exact dup
                continue
            seen.add(key)
            out.append(b)
        rows = out
    return rows

def export_current_to_csv(rows_by_ix, out_path):
    # writes a single-column CSV in index order (0..max)
    if not rows_by_ix:
        with open(out_path, "w", encoding="utf-8", newline="") as f:
            pass
        return
    # appends to the end
    max_ix = max(rows_by_ix.keys())
    with open(out_path, "w", encoding="utf-8", newline="") as f:
        w = csv.writer(f)
        # no header on purpose
        for ix in range(0, max_ix + 1):
            body = rows_by_ix.get(ix, "")
            w.writerow([body])

def parse_row_index(name, dataset):
    m = ROW_NAME_RE.match(name)
    if not m: 
        return None
    if m.group("ds") != dataset: 
        return None
    return int(m.group("ix"))

# --- DB operations (per-item transactions) ---
def upsert_item_and_chunks(conn, *, dataset, row_ix, body, apply_changes: bool, meta_extra=None):
    """
    Create/update info::<dataset>::row::<row_ix> with body; replace chunks+embeddings.
    If apply_changes=False, just returns the planned operations (no DB writes).
    """
    name = ROW_NAME_FMT.format(dataset=dataset, idx=row_ix)
    metadata = {"dataset": dataset, "identity": "position", "row": row_ix}
    if meta_extra: 
        metadata.update(meta_extra)
    chunks = chunk_text(body)
    if not chunks:
        # valid: empty after trims -> delete if exists
        return {"action": "delete_if_exists", "name": name}

    embs = embed_chunks(chunks)

    if not apply_changes:
        return {"action": "UPSERT", "name": name, "chunks": len(chunks)}

    cur = conn.cursor()
    try:
        cur.execute("SELECT id FROM rag.rag_item WHERE kind='info' AND name=%s", (name,))
        got = cur.fetchone()
        # if the item with the desired name with kind "info" already exists, it is updated
        # else a new item is created
        if got:
            item_id = got[0]
            cur.execute("""
              UPDATE rag.rag_item
                 SET body=%s, metadata=%s, version=version+1, updated_at=now()
               WHERE id=%s
            """, (body, json.dumps(metadata), item_id))
        else:
            cur.execute("""
              INSERT INTO rag.rag_item(kind, name, body, metadata)
              VALUES ('info', %s, %s, %s)
              RETURNING id
            """, (name, body, json.dumps(metadata)))
            item_id = cur.fetchone()[0]

        # replace chunks as the embeddings are not updateable
        cur.execute("DELETE FROM rag.rag_chunk WHERE item_id=%s", (item_id,))
        rows = [(item_id, ix, txt, to_pgvector(embs[ix])) for ix, txt in enumerate(chunks)]
        execute_values(cur,
            "INSERT INTO rag.rag_chunk(item_id,chunk_ix,chunk_text,embedding) VALUES %s",
            rows,
            template="(%s,%s,%s,%s::vector)")
        conn.commit()
        return {"action": "UPSERT_APPLIED", "name": name, "chunks": len(chunks)}
    except Exception:
        conn.rollback()
        raise

def delete_item(conn, *, dataset, row_ix, apply_changes: bool):
    name = ROW_NAME_FMT.format(dataset=dataset, idx=row_ix)
    if not apply_changes:
        return {"action": "DELETE", "name": name}
    cur = conn.cursor()
    try:
        # no need to worry about deleting from rag.rag_chunks as well as in the db 
        # an auto delete parallelization between rag_items and rag_chunks is set
        cur.execute("DELETE FROM rag.rag_item WHERE kind='info' AND name=%s", (name,))
        affected = cur.rowcount
        conn.commit()
        return {"action": "DELETE_APPLIED", "name": name, "rows": affected}
    except Exception:
        conn.rollback()
        raise

def main():
    ap = argparse.ArgumentParser(description="Sync CSV to RAG info items (position-based identity)")
    ap.add_argument("--dataset", default="ragdb", help="dataset id, used in name: info::<dataset>::row::<ix>")
    ap.add_argument("--desired", default="./info_items.csv", help="CSV path with desired bodies (first column only)")
    ap.add_argument("--has-header", action="store_true", help="treat first row as header")
    ap.add_argument("--dedupe", action="store_true", help="drop duplicate rows in desired (exact text match)")
    ap.add_argument("--apply", action="store_true", help="apply DB changes; otherwise dry-run")
    ap.add_argument("--export-current", help="write current DB state to this CSV (single column)")
    ap.add_argument("--audit-json", help="write an audit JSON with planned/applied ops")
    args = ap.parse_args()
    
    print("Got request with params", args)

    # load desired (which is basically the entire source)
    desired_rows = load_desired_csv(args.desired, has_header=args.has_header, dedupe=args.dedupe)
    # 0-based index
    # a dictionary of index -> normalized body text
    desired_map = {ix: normalize_text(body) for ix, body in enumerate(desired_rows)}

    conn = psycopg2.connect(RAG_PG_DSN)
    cur  = conn.cursor()

    # Read current info items for this dataset
    current_items = q(cur, """
      SELECT id, name, body, metadata, version, created_at, updated_at
      FROM rag.rag_item
      WHERE kind='info' AND name LIKE %s
      ORDER BY name
    """, (f"info::{args.dataset}::row::%",))

    current_map = {}
    for it in current_items:
        ix = parse_row_index(it["name"], args.dataset)
        if ix is None:  # skip any stray names
            continue
        current_map[ix] = normalize_text(it["body"] or "")

    # optional: export current to CSV (single col)
    if args.export_current:
        export_current_to_csv(current_map, args.export_current)

    # Diff
    desired_ixs = set(desired_map.keys())
    current_ixs = set(current_map.keys())

    # determine creates by what is in desired bu not in current
    # deletes by subtracting what is desired from what is there currently
    # updates by what is in both but has different bodies
    creates = sorted(list(desired_ixs - current_ixs))
    deletes = sorted(list(current_ixs - desired_ixs))
    updates = sorted([ix for ix in (desired_ixs & current_ixs) if desired_map[ix] != current_map[ix]])

    # Plan/apply
    ops = {
        "dataset": args.dataset,
        "ts_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "embedding_model": EMBED_MODEL,
        "apply": bool(args.apply),
        "counts": {"desired": len(desired_map), "current": len(current_map),
                   "creates": len(creates), "updates": len(updates), "deletes": len(deletes)},
        "actions": []
    }

    print(f"\n=== CSV→RAG SYNC (dataset={args.dataset}) ===")
    print(f"Desired rows: {len(desired_map)} | Current rows: {len(current_map)}")
    print(f"Plan → create: {len(creates)} | update: {len(updates)} | delete: {len(deletes)}")
    if not args.apply:
        print("(dry-run: use --apply to execute)\n")

    # Apply in stable order
    try:
        for ix in creates:
            body = desired_map[ix]
            res = upsert_item_and_chunks(conn, dataset=args.dataset, row_ix=ix, body=body, apply_changes=args.apply)
            ops["actions"].append(res)
            print(f"CREATE row {ix}: {res['action']}")
        for ix in updates:
            body = desired_map[ix]
            res = upsert_item_and_chunks(conn, dataset=args.dataset, row_ix=ix, body=body, apply_changes=args.apply)
            ops["actions"].append(res)
            print(f"UPDATE row {ix}: {res['action']}")
        for ix in deletes:
            res = delete_item(conn, dataset=args.dataset, row_ix=ix, apply_changes=args.apply)
            ops["actions"].append(res)
            print(f"DELETE row {ix}: {res['action']}")
    finally:
        conn.close()

    # Audit file
    if args.audit_json:
        with open(args.audit_json, "w", encoding="utf-8") as f:
            json.dump(ops, f, ensure_ascii=False, indent=2)

    # Final summary
    print("\nSummary:", json.dumps(ops["counts"], separators=(", ", ": ")))
    if not args.apply:
        print("Dry-run complete. No DB changes were made.")

if __name__ == "__main__":
    print("Running sync_info_csv.py")
    main()