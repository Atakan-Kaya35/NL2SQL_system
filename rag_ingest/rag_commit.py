import os, json, requests, psycopg2, argparse, csv
from psycopg2.extras import execute_values
from datetime import datetime

APP_PG_DSN = os.getenv("APP_PG_DSN")   # source DB with real tables
RAG_PG_DSN = os.getenv("RAG_PG_DSN")   # target DB that has rag.rag_item / rag.rag_chunk
OLLAMA_URL = os.getenv("OLLAMA_URL", "http://nl2sql_ollama:11434")
EMBED_MODEL = os.getenv("EMBED_MODEL", "nomad-embed-text")  # pick one and match dimension in DB

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

import csv

def read_csv_to_matrix(file_path):
    """
    Reads a CSV file from a given path and returns its contents as a 2D list (matrix).

    Args:
        file_path (str): The full path to the CSV file.

    Returns:
        list: A 2D list representing the data from the CSV file.
              Returns an empty list if the file is not found.
    """
    matrix = []
    try:
        # Open the file in read mode ('r')
        with open(file_path, 'r', newline='') as csvfile:
            # Create a CSV reader object
            csv_reader = csv.reader(csvfile)
            
            # Iterate over each row in the CSV file
            for row in csv_reader:
                # Append each row (as a list) to our main matrix
                matrix.append(row)
    except FileNotFoundError:
        print(f"Error: The file at '{file_path}' was not found.")
        # Return an empty list to indicate an error
        return []
    
    return matrix

def main():
    """
    The csv file has to have the format where each row contaşns one entry, the structure of the row must be:
    kind, name, body, metadata_name1, metadata_content1, metadata_name2 ... etc
    """
    rag_conn = connect(RAG_PG_DSN)
    file_dir ="./new_entries.csv"
    
    matrix = read_csv_to_matrix(file_dir)
    
    
    for row in matrix:
        metadata = {}
        for i in range(3, len(row), 2):
            metadata[row[i]] = row[i+1]
        item_id = upsert_item(
            rag_conn,
            kind=row[0],
            name=row[1],
            body=row[2],
            metadata=metadata
        )
        chunks = chunk_text(row[2])
        embs = embed(chunks)
        upsert_chunks(rag_conn, item_id, chunks, embs)
        
if __name__ == "__main__":
    main()
