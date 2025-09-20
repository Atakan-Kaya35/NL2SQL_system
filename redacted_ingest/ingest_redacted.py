import os, time, sys
from datetime import datetime, timezone
from tenacity import retry, wait_exponential, stop_after_attempt
from spacetrack import SpaceTrackClient
import psycopg2
from psycopg2.extras import execute_values
import json, csv, io
from psycopg2.extras import execute_values, Json



ST_USER = os.environ.get("redacted")
ST_PASS = os.environ.get("redacted")
PG_DSN  = os.environ.get("PG_DSN", "postgresql://app:app_pw@postgres:5432/appdb")
PAGE_SIZE = int(os.environ.get("PAGE_SIZE", "1000"))
REQUEST_SLEEP = float(os.environ.get("REQUEST_SLEEP", "2.0"))

if not (ST_USER and ST_PASS):
    print("ERROR: set ST_USER and ST_PASS", file=sys.stderr); sys.exit(1)

st = SpaceTrackClient(identity=ST_USER, password=ST_PASS)

from itertools import islice

def batched(iterable, n):
    it = iter(iterable)
    while True:
        batch = list(islice(it, n))
        if not batch:
            return
        yield batch

def upsert_batch(conn, rows_json):
    def norm(j):
        def to_int(x):
            try: return int(x) if x is not None else None
            except: return None
        return (
            "redacted",
            datetime.now(timezone.utc)
        ), (to_int(j.get("redacted")), Json(j), datetime.now(timezone.utc))

    data = [norm(j) for j in rows_json]
    redacted = [d[0] for d in data]
    raw_rows    = [d[1] for d in data]

    with conn.cursor() as cur:
        execute_values(cur, """
            redacted
        """, redacted_rows)

        execute_values(cur, """
            redacted
        """, raw_rows)

    conn.commit()

def coerce_rows(obj):
    """
    Accepts: list[dict] | JSON string | CSV string
    Returns: list[dict] (redacted rows with UPPERCASE keys like redacted, REDACTED, ...)
    """
    # Already a parsed list?
    if isinstance(obj, list):
        if obj and isinstance(obj[0], dict):
            return obj
        return []

    # JSON string?
    if isinstance(obj, str):
        s = obj.strip()
        # Try JSON first
        if s.startswith('[') or s.startswith('{'):
            try:
                parsed = json.loads(s)
                if isinstance(parsed, list):
                    return parsed
                elif isinstance(parsed, dict):
                    return [parsed]
            except json.JSONDecodeError:
                pass
        # Fallback: assume CSV
        return list(csv.DictReader(io.StringIO(s)))

    # Unknown type
    return []

def main():
    conn = psycopg2.connect(PG_DSN)
    try:
        raw = st.redacted(orderby="redacted asc", format="json")  # library may still return text
        # Quick debug to know what we got:
        #print(f"[redacted] type from API: {type(raw)} ; preview={str(raw)[:120].replace('\n\n',' ')}")
        data = coerce_rows(raw)

        total = 0
        for chunk in batched(data, PAGE_SIZE):
            upsert_batch(conn, chunk)
            total += len(chunk)
            print(f"[redacted] upserted {len(chunk)} (total {total})")
    finally:
        conn.close()
    print(f"[redacted] done. total={total}")


if __name__ == "__main__":
    main()
