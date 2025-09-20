#!/usr/bin/env bash
set -euo pipefail

if [[ "${RUN_MODE:-oneshot}" == "schedule" ]]; then
  # Expect CRON like: "10 20 * * * /app/run_once.sh"
  echo "${CRON:-10 20 * * * /app/run_once.sh}" > /app/cronfile
  cat >/app/run_once.sh <<'EOF'
#!/usr/bin/env bash
set -euo pipefail
python /app/ingest_redacted.py
EOF
  chmod +x /app/run_once.sh
  exec /usr/local/bin/supercronic -passthrough-logs /app/cronfile
else
  exec python /app/ingest_redacted.py
fi
