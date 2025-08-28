"""
docker compose build --no-cache tle_ingest
docker compose run --rm -e MODE=once -e BATCH_SIZE=100 tle_ingest
"""

import os, sys, time, json, math, random, logging
from datetime import datetime, timezone
from typing import List, Dict
import requests
import psycopg2
import psycopg2.extras

ST_BASE = "https://www.space-track.org"
MU_EARTH = 398600.4418  # km^3/s^2
R_EARTH = 6378.135      # km (WGS‑72 for TLE conventions)

logging.basicConfig(level=logging.INFO, format='[%(asctime)s] %(levelname)s %(message)s')


def pg_conn():
    return psycopg2.connect(
        host=os.getenv('PGHOST', 'localhost'),
        dbname="appdb",
        user="app",
        password=os.getenv('PGPASSWORD', ''),
        port=int(os.getenv('PGPORT', '5432')),
    )


def st_session():
    u, p = os.environ['ST_USERNAME'], os.environ['ST_PASSWORD']
    s = requests.Session()
    r = s.post(f"{ST_BASE}/ajaxauth/login", data={'identity': u, 'password': p})
    r.raise_for_status()
    return s


def fetch_norads(cur) -> List[int]:
    cur.execute("SELECT norad_cat_id FROM satcat ORDER BY norad_cat_id")
    return [row[0] for row in cur.fetchall()]


def chunk(lst, n):
    for i in range(57048, len(lst), n):
        yield lst[i:i+n]


def to_float(v):
    try:
        return float(v) if v is not None else None
    except Exception:
        return None


def derived_from_mean_motion(mean_motion, eccentricity):
    if not mean_motion:
        return None, None, None, None
    # mean_motion in rev/day -> rad/s
    n_rad_s = (mean_motion * 2 * math.pi) / 86400.0
    a_km = (MU_EARTH / (n_rad_s ** 2)) ** (1.0/3.0)
    e = eccentricity or 0.0
    period_min = 1440.0 / mean_motion if mean_motion else None
    apogee_km = a_km * (1 + e) - R_EARTH
    perigee_km = a_km * (1 - e) - R_EARTH
    return period_min, a_km, apogee_km, perigee_km


UPSERT_HISTORY = """
INSERT INTO orbital.gp_history (
  norad_cat_id, epoch, creation_date, object_name, object_id, center_name,
  mean_motion, semimajor_axis_km, period_min, tle_line0, tle_line1, tle_line2
) VALUES (
  %(NORAD_CAT_ID)s, %(EPOCH)s, %(CREATION_DATE)s, %(OBJECT_NAME)s, %(OBJECT_ID)s, %(CENTER_NAME)s,
  %(MEAN_MOTION)s, %(SEMIMAJOR_AXIS)s, %(PERIOD)s, %(TLE_LINE0)s, %(TLE_LINE1)s, %(TLE_LINE2)s
)
ON CONFLICT (norad_cat_id, epoch) DO UPDATE SET
  creation_date       = GREATEST(EXCLUDED.creation_date, orbital.gp_history.creation_date),
  object_name         = EXCLUDED.object_name,
  object_id           = EXCLUDED.object_id,
  center_name         = EXCLUDED.center_name,
  mean_motion         = EXCLUDED.mean_motion,
  semimajor_axis_km   = COALESCE(EXCLUDED.semimajor_axis_km, orbital.gp_history.semimajor_axis_km),
  period_min          = COALESCE(EXCLUDED.period_min,        orbital.gp_history.period_min),
  tle_line0           = COALESCE(EXCLUDED.tle_line0,         orbital.gp_history.tle_line0),
  tle_line1           = COALESCE(EXCLUDED.tle_line1,         orbital.gp_history.tle_line1),
  tle_line2           = COALESCE(EXCLUDED.tle_line2,         orbital.gp_history.tle_line2);
"""
def _ts(v):
    return datetime.fromisoformat(v.replace('Z','+00:00')) if v else None

def fetch_gp_for_ids(sess: requests.Session, ids: List[int]) -> List[Dict]:
    ids_str = ",".join(str(i) for i in ids)
    url = (
        f"{ST_BASE}/basicspacedata/query/class/gp_history/"
        f"norad_cat_id/{ids_str}/"
        f"EPOCH/2024-08-24--2025-08-27/"
        f"format/json/emptyresult/show"
    )
    for attempt in range(3):
        logging.info("URL sent: %s", url)
        r = sess.get(url, timeout=60)
        # If the server hiccups, split the batch and try halves
        if r.status_code == 500 and len(ids) > 1:
            mid = max(1, len(ids) // 2)
            left  = fetch_gp_for_ids(sess, ids[:mid])
            right = fetch_gp_for_ids(sess, ids[mid:])
            return left + right
        # Gentle backoff on throttling / transient gateway issues
        if r.status_code in (429, 502, 503):
            logging.info("Error occured with code: %d", r.status_code)
            time.sleep(2 ** attempt)
            continue
        r.raise_for_status()
        return r.json()
    # Last raise if we exhaust retries
    r.raise_for_status()



def run_once():
    sess = st_session()
    with pg_conn() as conn:
        conn.autocommit = False
        cur = conn.cursor()
        norads = fetch_norads(cur)
        logging.info("NORADs: %d", len(norads))
        total = 0
        for batch in chunk(norads, int(os.getenv('BATCH_SIZE', '100'))):
            data = fetch_gp_for_ids(sess, batch)
            if not data:
                continue
            hist_payload = []
            for raw in data:
                rec = {
                    "NORAD_CAT_ID":  int(raw.get("NORAD_CAT_ID") or 0) or None,
                    "EPOCH":         _ts(raw.get("EPOCH")),
                    "CREATION_DATE": _ts(raw.get("CREATION_DATE")),
                    "OBJECT_NAME":   raw.get("OBJECT_NAME"),
                    "OBJECT_ID":     raw.get("OBJECT_ID"),
                    "CENTER_NAME":   raw.get("CENTER_NAME") or "EARTH",
                    "MEAN_MOTION":   to_float(raw.get("MEAN_MOTION")),
                    "SEMIMAJOR_AXIS":to_float(raw.get("SEMIMAJOR_AXIS")),
                    "PERIOD":        to_float(raw.get("PERIOD")),
                    "TLE_LINE0":     raw.get("TLE_LINE0"),
                    "TLE_LINE1":     raw.get("TLE_LINE1"),
                    "TLE_LINE2":     raw.get("TLE_LINE2"),
                }
                # Compute fallbacks if provider omits these:
                if rec["MEAN_MOTION"]:
                    if rec["PERIOD"] is None:
                        rec["PERIOD"] = 1440.0 / rec["MEAN_MOTION"]
                    if rec["SEMIMAJOR_AXIS"] is None:
                        n_rad_s = (rec["MEAN_MOTION"] * 2 * math.pi) / 86400.0
                        rec["SEMIMAJOR_AXIS"] = (MU_EARTH / (n_rad_s ** 2)) ** (1.0/3.0)

                # Skip rows missing mandatory keys
                if rec["NORAD_CAT_ID"] is None or rec["EPOCH"] is None or rec["CREATION_DATE"] is None:
                    continue

                hist_payload.append(rec)

            psycopg2.extras.execute_batch(cur, UPSERT_HISTORY, hist_payload)
            total += len(hist_payload)
            conn.commit()
            logging.info("Upserted history rows: %d", total)
            time.sleep(float(os.getenv('INTER_BATCH_SLEEP', '12')))  # be gentle
        logging.info("Upserted history rows: %d", total)


if __name__ == "__main__":
    mode = os.getenv('MODE', 'once')
    if mode == 'loop':
        # choose a stable random minute at startup
        rand_minute = random.randrange(0, 60)
        logging.info("Loop mode. Will run at minute %02d each hour.", rand_minute)
        while True:
            now = datetime.now(timezone.utc)
            if now.minute == rand_minute:
                try:
                    run_once()
                except Exception:
                    logging.exception("run_once failed")
                # avoid double-fire within the same minute
                time.sleep(70)
            time.sleep(5)
    else:
        run_once()