-- Core SATCAT (one row per cataloged object)
CREATE TABLE satcat (
  norad_cat_id     INTEGER PRIMARY KEY,
  object_name      TEXT,
  object_id        TEXT,          -- International Designator (e.g., "1998-067A")
  country          TEXT,
  launch_site      TEXT,
  launch_date      DATE,
  decay_date       DATE,
  period_min       NUMERIC,       -- minutes
  inclination_deg  NUMERIC,
  apogee_km        NUMERIC,
  perigee_km       NUMERIC,
  rcs_size         TEXT,          -- e.g., "LARGE/MEDIUM/SMALL/UNKNOWN"
  object_type      TEXT,          -- PAYLOAD/ROCKET BODY/DEBRIS/UNKNOWN...
  source           TEXT DEFAULT 'space-track',
  last_seen_utc    TIMESTAMP WITH TIME ZONE DEFAULT now()
);

-- Optional: track raw JSON for diff/debug
CREATE TABLE satcat_raw (
  norad_cat_id  INTEGER PRIMARY KEY,
  raw_json      JSONB NOT NULL,
  fetched_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Helpful indexes
CREATE INDEX ON satcat (country);
CREATE INDEX ON satcat (object_type);
CREATE INDEX ON satcat (launch_date);
CREATE INDEX ON satcat (decay_date);
