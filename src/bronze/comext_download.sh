#!/usr/bin/env bash
# Downloads the .7z dataset from COMEXT bulk download 
# This is intended to run inside a Docker container before comext_extract.sh
# Dont forget setting DRY_RUN to just print the download paths.

set -euo pipefail

# Helper function to print current step for observability
log() { echo "[download] $*"; }

#-------------------- PREREQUISITES
# Fail fast with a clear message if required tools are missing (if run ouside Docker).
command -v curl >/dev/null 2>&1 || { echo "[download] FAIL: curl is required" >&2; exit 2; }
command -v python >/dev/null 2>&1 || { echo "[download] FAIL: python is required" >&2; exit 2; }

DRY_RUN="${DRY_RUN:-0}"

#-------------------- SNAPSHOT IDENTIFICATION
# Optional snapshot id (used to create an immutable landing directory per run)
# Example: comext__20260201T064500
SNAPSHOT_ID="${SNAPSHOT_ID:-}"
if [[ -z "$SNAPSHOT_ID" ]]; then
  # Portable timestamp (macOS + Linux)
  SNAPSHOT_ID="$(date +%Y%m%dT%H%M%S)"
fi

log "SNAPSHOT_ID=$SNAPSHOT_ID"
log "DRY_RUN=$DRY_RUN"

# Read command line arguments
FROM="${1:-}"   # YYYY-MM
TO="${2:-}"     # YYYY-MM

# If one or both variables are missing log the error
if [[ -z "$FROM" || -z "$TO" ]]; then
  log "FAIL usage: $0 YYYY-MM YYYY-MM" >&2
  exit 2
fi

#-------------------- PERIOD VALIDATION
MIN_MONTH="2002-01"

if [[ "$FROM" < "$MIN_MONTH" || "$TO" < "$MIN_MONTH" ]]; then
  echo "[download] FAIL: months before $MIN_MONTH are not supported (FROM=$FROM TO=$TO)" >&2
  exit 2
fi

if [[ "$FROM" > "$TO" ]]; then
  echo "[download] FAIL: FROM must be <= TO (FROM=$FROM TO=$TO)" >&2
  exit 2
fi

#-------------------- TEMP FILE CLEANUP 
# Ensure partial downloads do not remain if the script exits unexpectedly.
cleanup() {
  if [[ -n "${tmp:-}" && -f "${tmp:-}" ]]; then
    rm -f "${tmp:-}"
  fi
}
trap cleanup EXIT

#-------------------- SOURCE CONFIGURATION AND LANDING ZONE
BASE_URL="https://ec.europa.eu/eurostat/api/dissemination/files"
OUT_DIR="data/raw/comext__${SNAPSHOT_ID}"

#-------------------- MONTH NAVIGATION
# Advance a YYYY-MM period by one month in a portable way
next_month() {
  python - "$1" <<'PY'
import sys
y, m = map(int, sys.argv[1].split('-'))
m += 1
if m == 13:
    y += 1
    m = 1
print(f"{y:04d}-{m:02d}")
PY
}

current="$FROM"

log "start snapshot=$SNAPSHOT_ID from=$FROM to=$TO out_dir=$OUT_DIR"

#-------------------- INGESTION LOOP (closed interval: FROM → TO)
# Iterates month by month, downloading immutable raw bulk files
while [[ "$current" < "$TO" || "$current" == "$TO" ]]; do
  yyyymm="${current/-/}"

  url="${BASE_URL}?file=comext%2FCOMEXT_DATA%2FPRODUCTS%2Ffull_v2_${yyyymm}.7z"

  # Deterministic layout requires full_YYYYMM.7z
  out_dir="${OUT_DIR}/${current}"
  out="${out_dir}/full_${yyyymm}.7z"
  tmp="${out}.part"

  if [[ -f "$out" ]]; then
    log "exists skip month=$current file=$out"
  else
    # Prints URLs on dry run mode. 
    if [[ "$DRY_RUN" == "1" ]]; then
      log "[dry mode] would_download month=$current"
      log "[dry mode] would_write file=$out"
    else
      mkdir -p "$out_dir"
      log "downloading month=$current url=$url"
      curl --fail --location  --show-error\
        --retry 3 --retry-delay 2 \
        --connect-timeout 20 --max-time 600 \
        -o "$tmp" "$url"
      mv -f "$tmp" "$out"
      log "success month=$current file=$out"
      sleep 1
    fi
  fi

  current="$(next_month "$current")"
done

#-------------------- RUN COMPLETION
log "end"