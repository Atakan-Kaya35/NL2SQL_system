"""
There is not a docker container for the rag-ingest service in the docker compose at the moment
This means that he psychopg2 needs to be installed and this python script must be run in the same command
progressively or a container with the module must be set up for the code to work

A necessary upgrade to this code is to completely decouple each kind that wants to be managed, best way to implement the 
functionality is not to try and manage all of them together since that leads to indexing and ordering comlications
inside a structured dataset such as Postgres (SQL in general)

docker compose run --rm --entrypoint sh rag-ingest -lc " pip install --no-cache-dir psycopg2-binary requests >/dev/null && python -u /app/sync_info_csv.py --dataset redacted-info --desired info,example --export-current /app/out/redacted-info.current.csv --audit-json /app/out/redacted-info.audit.json --apply"
"""

# sync_info_csv.py
import os, re, csv, json, argparse, psycopg2, requests
from datetime import datetime
from psycopg2.extras import execute_values
from pathlib import Path

# --- env / defaults ---
RAG_PG_DSN  = os.getenv("RAG_PG_DSN",  "dbname=ragdb user=rag password=rag_pw host=rag-pg port=5432")
OLLAMA_URL  = os.getenv("OLLAMA_URL",  "http://ollama:11434")
EMBED_MODEL = os.getenv("EMBED_MODEL", "nomic-embed-text")   # 768-dim
CHUNK_CHARS = int(os.getenv("CHUNK_CHARS", "1500"))          # simple, char-based chunking

# --- helpers ---
ROW_NAME_FMT = "{kind}::{dataset}::row::{idx}"
ROW_NAME_RE  = re.compile(r"^(?P<kind>[^:]+)::(?P<ds>[^:]+)::row::(?P<ix>\d+)$")

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
    """
    returns a list of dictionaries with the key value pairs
    """
    # execute sql querry and return list of dicts
    cur.execute(sql, params or ())
    cols = [c.name for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]

# -------- NEW: per-kind desired loader (decoupled indexing) --------
def load_desired_csv_by_kind(desired_kinds, has_header=False, dedupe=False):
    """
    Load CSVs separately for each kind, preserving *per-kind* row ordering (0..N for that kind).
    Returns: dict[str, list[str]] mapping kind -> list of bodies (first column).
    """
    by_kind = {}
    for kind in desired_kinds:
        rows = []
        # put the first rows of the first column into a list
        with open(f"./{kind}_items.csv", "r", encoding="utf-8-sig", newline="") as f:
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

        if dedupe:
            # deduplicate *within the same kind* only
            seen, out = set(), []
            for b in rows:
                key = normalize_text(b)
                if key in seen:  # skip exact dup
                    continue
                seen.add(key)
                out.append(b)
            rows = out

        by_kind[kind] = rows
    return by_kind

def export_current_to_csv_by_kind(current_by_kind, out_path):
    """
    writes CSV(s) for current DB state:
      - if only one kind present → write exactly to out_path (single-column CSV, 0..max index)
      - if multiple kinds       → write sibling files `basename.<kind>.csv` next to out_path
    """
    out_path = Path(out_path)
    kinds = list(current_by_kind.keys())
    if not kinds:
        out_path.write_text("", encoding="utf-8")
        return

    def write_single(path, rows_by_ix):
        if not rows_by_ix:
            Path(path).write_text("", encoding="utf-8")
            return
        max_ix = max(rows_by_ix.keys())
        with open(path, "w", encoding="utf-8", newline="") as f:
            w = csv.writer(f)
            # no header on purpose
            for ix in range(0, max_ix + 1):
                body = rows_by_ix.get(ix, "")
                w.writerow([body])

    if len(kinds) == 1:
        k = kinds[0]
        write_single(out_path, current_by_kind[k])
    else:
        stem = out_path.with_suffix("")  # drop .csv once, we’ll add it back
        for k in kinds:
            write_single(stem.with_name(f"{stem.name}.{k}.csv"), current_by_kind[k])

def parse_row_index(name, dataset):
    m = ROW_NAME_RE.match(name)
    if not m: 
        return None
    if m.group("ds") != dataset: 
        return None
    return int(m.group("ix"))

# --- DB operations (per-item transactions) ---
def upsert_item_and_chunks(conn, *, dataset, row_ix, body, kind, apply_changes: bool, meta_extra=None):
    """
    Create/update {kind}::{dataset}::row::<row_ix> with body; replace chunks+embeddings.
    If apply_changes=False, just returns the planned operations (no DB writes).

    IMPORTANT: row_ix is now *per-kind* (i.e., 0..N inside that kind), not a global index
    across all kinds. This decouples kinds and avoids cross-kind reindexing side-effects.
    """
    name = ROW_NAME_FMT.format(kind=kind, dataset=dataset, idx=row_ix)
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
        cur.execute("SELECT id FROM rag.rag_item WHERE kind=%s AND name=%s", (kind, name,))
        got = cur.fetchone()
        # if the item with the desired name with kind already exists, it is updated
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
              VALUES (%s, %s, %s, %s)
              RETURNING id
            """, (kind, name, body, json.dumps(metadata)))
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

def delete_item(conn, *, dataset, row_ix, kind, apply_changes: bool):
    name = ROW_NAME_FMT.format(kind=kind, dataset=dataset, idx=row_ix)
    if not apply_changes:
        return {"action": "DELETE", "name": name}
    cur = conn.cursor()
    try:
        # no need to worry about deleting from rag.rag_chunks as well as in the db 
        # an auto delete parallelization between rag_items and rag_chunks is set
        cur.execute("DELETE FROM rag.rag_item WHERE kind=%s AND name=%s", (kind, name,))
        affected = cur.rowcount
        conn.commit()
        return {"action": "DELETE_APPLIED", "name": name, "rows": affected}
    except Exception:
        conn.rollback()
        raise

def main():
    ap = argparse.ArgumentParser(description="Sync CSV to RAG items with per-kind indexing (position-based identity)")
    ap.add_argument("--dataset", default="ragdb", help="dataset id, used in name: <kind>::<dataset>::row::<ix>")
    ap.add_argument("--desired", default="info,example", help="comma-separated kinds; reads ./<kind>_items.csv per kind (first column only)")
    ap.add_argument("--has-header", action="store_true", help="treat first row as header")
    ap.add_argument("--dedupe", action="store_true", help="drop duplicate rows within each kind (exact text match)")
    ap.add_argument("--apply", action="store_true", help="apply DB changes; otherwise dry-run")
    ap.add_argument("--export-current", help="write current DB state to CSV(s) (single column). If multiple kinds, writes basename.<kind>.csv")
    ap.add_argument("--audit-json", help="write an audit JSON with planned/applied ops")
    args = ap.parse_args()
    
    print("Got request with params", args)
    
    desired_kinds = [k.strip() for k in args.desired.split(",") if k.strip()]
    desired_kinds_tuple = tuple(desired_kinds)

    # -------- NEW: load desired per-kind (no global concatenation) --------
    desired_by_kind = load_desired_csv_by_kind(desired_kinds, has_header=args.has_header, dedupe=args.dedupe)
    # normalize per-kind, build maps: kind -> {ix: body}
    desired_maps = {
        kind: {ix: normalize_text(body) for ix, body in enumerate(rows)}
        for kind, rows in desired_by_kind.items()
    }

    conn = psycopg2.connect(RAG_PG_DSN)
    cur  = conn.cursor()

    # Read current items for these kinds & dataset
    current_items = q(cur, f"""
    SELECT id, kind, name, body, metadata, version, created_at, updated_at
      FROM rag.rag_item
     WHERE kind IN ({', '.join([f"%s" for _ in desired_kinds_tuple])})
       AND name LIKE %s
     ORDER BY kind, name
    """, (*desired_kinds_tuple, f"%::{args.dataset}::row::%"))

    # Build current maps: kind -> {ix: body}
    # ix is the local index that the info is ini inside of the host csv file
    current_maps = {k: {} for k in desired_kinds}
    for it in current_items:
        k = it["kind"]
        ix = parse_row_index(it["name"], args.dataset)
        if ix is None or k not in current_maps:
            continue
        current_maps[k][ix] = normalize_text(it["body"] or "")

    # optional: export current to CSV(s) (single col)
    if args.export_current:
        export_current_to_csv_by_kind(current_maps, args.export_current)

    # Plan/apply per kind
    ops = {
        "dataset": args.dataset,
        "ts_utc": datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ"),
        "embedding_model": EMBED_MODEL,
        "apply": bool(args.apply),
        "counts": {"desired": 0, "current": 0, "creates": 0, "updates": 0, "deletes": 0},
        "per_kind": {},
        "actions": []
    }

    print(f"\n=== CSV→RAG SYNC (dataset={args.dataset}) ===")
    total_desired = sum(len(m) for m in desired_maps.values())
    total_current = sum(len(m) for m in current_maps.values())
    print(f"Desired rows (all kinds): {total_desired} | Current rows (all kinds): {total_current}")
    if not args.apply:
        print("(dry-run: use --apply to execute)\n")

    # Run the "housekeeping" system for each kind independently
    try:
        for kind in desired_kinds:
            dmap = desired_maps.get(kind, {})
            cmap = current_maps.get(kind, {})

            desired_ixs = set(dmap.keys())
            current_ixs = set(cmap.keys())

            # determine creates/updates/deletes per-kind
            creates = sorted(list(desired_ixs - current_ixs))
            deletes = sorted(list(current_ixs - desired_ixs))
            updates = sorted([ix for ix in (desired_ixs & current_ixs) if dmap[ix] != cmap[ix]])

            # Totals for summary
            ops["counts"]["desired"] += len(dmap)
            ops["counts"]["current"] += len(cmap)
            ops["counts"]["creates"] += len(creates)
            ops["counts"]["updates"] += len(updates)
            ops["counts"]["deletes"] += len(deletes)

            ops["per_kind"][kind] = {
                "desired": len(dmap),
                "current": len(cmap),
                "creates": len(creates),
                "updates": len(updates),
                "deletes": len(deletes),
            }

            print(f"\n-- Kind: {kind} --")
            print(f"Plan → create: {len(creates)} | update: {len(updates)} | delete: {len(deletes)}")

            # Apply in stable order (per-kind)
            for ix in creates:
                body = dmap[ix]
                res = upsert_item_and_chunks(conn, dataset=args.dataset, kind=kind, row_ix=ix, body=body, apply_changes=args.apply)
                ops["actions"].append(res)
                print(f"CREATE {kind} row {ix}: {res['action']}")

            for ix in updates:
                body = dmap[ix]
                res = upsert_item_and_chunks(conn, dataset=args.dataset, kind=kind, row_ix=ix, body=body, apply_changes=args.apply)
                ops["actions"].append(res)
                print(f"UPDATE {kind} row {ix}: {res['action']}")

            for ix in deletes:
                res = delete_item(conn, dataset=args.dataset, kind=kind, row_ix=ix, apply_changes=args.apply)
                ops["actions"].append(res)
                print(f"DELETE {kind} row {ix}: {res['action']}")

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
