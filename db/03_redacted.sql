-- Nuke the wrong schema and recreate it clean
DROP SCHEMA IF EXISTS redacted CASCADE;
CREATE SCHEMA orbital;

-- redacted: many rows per redacted (one per redacted)
CREATE TABLE redacted.redacted (
  redacted
);

-- Helpful indexes for common queries
CREATE INDEX IF NOT EXISTS redacted   ON redacted.redacted (redacted, redacted DESC);
CREATE INDEX IF NOT EXISTS redacted   ON redacted.redacted (redacted DESC);
CREATE INDEX IF NOT EXISTS redacted       ON redacted.redacted (redacted);
CREATE INDEX IF NOT EXISTS redacted     ON redacted.redacted (redacted);