-- Nuke the wrong schema and recreate it clean
DROP SCHEMA IF EXISTS orbital CASCADE;
CREATE SCHEMA orbital;

-- gp_history: many rows per satellite (one per epoch)
CREATE TABLE orbital.gp_history (
  norad_cat_id        INT NOT NULL REFERENCES satcat(norad_cat_id) ON DELETE CASCADE,
  epoch               TIMESTAMPTZ NOT NULL,   -- from EPOCH (UTC)
  creation_date       TIMESTAMPTZ NOT NULL,   -- from CREATION_DATE (UTC)
  object_name         TEXT,                   -- from OBJECT_NAME
  object_id           TEXT,                   -- from OBJECT_ID (COSPAR)
  center_name         TEXT,                   -- from CENTER_NAME (e.g., 'EARTH')
  mean_motion         DOUBLE PRECISION,       -- from MEAN_MOTION (revs/day)
  semimajor_axis_km   DOUBLE PRECISION,       -- from SEMIMAJOR_AXIS (km)
  period_min          DOUBLE PRECISION,       -- from PERIOD (minutes)
  tle_line0           TEXT,                   -- from TLE_LINE0
  tle_line1           TEXT,                   -- from TLE_LINE1
  tle_line2           TEXT,                   -- from TLE_LINE2
  PRIMARY KEY (norad_cat_id, epoch)
);

-- Helpful indexes for common queries
CREATE INDEX IF NOT EXISTS gp_hist_by_id_epoch_desc   ON orbital.gp_history (norad_cat_id, epoch DESC);
CREATE INDEX IF NOT EXISTS gp_hist_by_creation_desc   ON orbital.gp_history (creation_date DESC);
CREATE INDEX IF NOT EXISTS gp_hist_by_object_id       ON orbital.gp_history (object_id);
CREATE INDEX IF NOT EXISTS gp_hist_by_object_name     ON orbital.gp_history (object_name);

-- Optional (uncomment if you want fast ILIKE searches and you can enable extensions)
-- CREATE EXTENSION IF NOT EXISTS pg_trgm;
-- CREATE INDEX IF NOT EXISTS gp_hist_name_trgm  ON orbital.gp_history USING GIN (object_name gin_trgm_ops);
-- CREATE INDEX IF NOT EXISTS gp_hist_cospar_trgm ON orbital.gp_history USING GIN (object_id gin_trgm_ops);
