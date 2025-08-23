import os, time, sys
from datetime import datetime, timezone
from tenacity import retry, wait_exponential, stop_after_attempt
from spacetrack import SpaceTrackClient
import psycopg2
from psycopg2.extras import execute_values
import json, csv, io
from psycopg2.extras import execute_values, Json



ST_USER = os.environ.get("ST_USER")
ST_PASS = os.environ.get("ST_PASS")
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


""" @retry(wait=wait_exponential(multiplier=1, min=2, max=60), stop=stop_after_attempt(5))
def fetch_page(offset):
    return st.satcat(
        orderby="norad_cat_id asc",
        limit=PAGE_SIZE,
        offset=offset,
        format="json"
    ) """

def upsert_batch(conn, rows_json):
    def norm(j):
        def to_int(x):
            try: return int(x) if x is not None else None
            except: return None
        return (
            to_int(j.get("NORAD_CAT_ID")),   # PK
            j.get("SATNAME"),                # -> object_name
            j.get("INTLDES"),                # -> object_id (International Designator)
            j.get("COUNTRY"),
            j.get("SITE"),                   # -> launch_site
            j.get("LAUNCH"),                 # -> launch_date
            j.get("DECAY"),                  # -> decay_date
            j.get("PERIOD"),                 # minutes
            j.get("INCLINATION"),            # degrees
            j.get("APOGEE"),                 # km
            j.get("PERIGEE"),                # km
            j.get("RCS_SIZE"),
            j.get("OBJECT_TYPE"),
            datetime.now(timezone.utc)
        ), (to_int(j.get("NORAD_CAT_ID")), Json(j), datetime.now(timezone.utc))

    data = [norm(j) for j in rows_json]
    satcat_rows = [d[0] for d in data]
    raw_rows    = [d[1] for d in data]

    with conn.cursor() as cur:
        execute_values(cur, """
            INSERT INTO satcat (
                norad_cat_id, object_name, object_id, country, launch_site,
                launch_date, decay_date, period_min, inclination_deg,
                apogee_km, perigee_km, rcs_size, object_type, last_seen_utc
            ) VALUES %s
            ON CONFLICT (norad_cat_id) DO UPDATE SET
              object_name     = EXCLUDED.object_name,
              object_id       = EXCLUDED.object_id,
              country         = EXCLUDED.country,
              launch_site     = EXCLUDED.launch_site,
              launch_date     = EXCLUDED.launch_date,
              decay_date      = EXCLUDED.decay_date,
              period_min      = EXCLUDED.period_min,
              inclination_deg = EXCLUDED.inclination_deg,
              apogee_km       = EXCLUDED.apogee_km,
              perigee_km      = EXCLUDED.perigee_km,
              rcs_size        = EXCLUDED.rcs_size,
              object_type     = EXCLUDED.object_type,
              last_seen_utc   = EXCLUDED.last_seen_utc
        """, satcat_rows)

        execute_values(cur, """
            INSERT INTO satcat_raw (norad_cat_id, raw_json, fetched_at)
            VALUES %s
            ON CONFLICT (norad_cat_id) DO UPDATE SET
              raw_json  = EXCLUDED.raw_json,
              fetched_at= EXCLUDED.fetched_at
        """, raw_rows)

    conn.commit()

def coerce_rows(obj):
    """
    Accepts: list[dict] | JSON string | CSV string
    Returns: list[dict] (SATCAT rows with UPPERCASE keys like NORAD_CAT_ID, SATNAME, ...)
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
        raw = st.satcat(orderby="norad_cat_id asc", format="json")  # library may still return text
        # Quick debug to know what we got:
        #print(f"[satcat] type from API: {type(raw)} ; preview={str(raw)[:120].replace('\n\n',' ')}")
        data = coerce_rows(raw)

        total = 0
        for chunk in batched(data, PAGE_SIZE):
            upsert_batch(conn, chunk)
            total += len(chunk)
            print(f"[satcat] upserted {len(chunk)} (total {total})")
    finally:
        conn.close()
    print(f"[satcat] done. total={total}")


if __name__ == "__main__":
    main()
