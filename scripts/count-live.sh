#!/usr/bin/env bash
# Prints a timestamp and how many streams are listed, once a second, so time-to-N is read straight
# off the terminal while scripts/load-senders.sh runs.
# Usage: scripts/count-live.sh [BASE_URL] [TOKEN] defaults http://localhost:8080 and $Live__Token.
set -euo pipefail

BASE="${1:-${LIVE_URL:-http://localhost:8080}}"
TOKEN="${2:-${Live__Token:-}}"

while true; do
  if body="$(curl -fsS -m 5 -H "X-Storage-Token: $TOKEN" "$BASE/api/live" 2>/dev/null)"; then
    # One "name" per stream object, and counting them with grep beats depending on jq for one
    # number.
    count="$(grep -o '"name"' <<<"$body" | wc -l || true)"
  else
    count=unreachable
  fi
  echo "$(date +%T) $count"
  sleep 1
done
