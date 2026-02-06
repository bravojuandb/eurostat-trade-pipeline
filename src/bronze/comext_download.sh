#!/usr/bin/env bash
# Downloads full_v2_YYYYMM.7z datasets from COMEXT bulk download, according to selected month interval.
# Intended to run inside a Docker container before comext_extract.sh
# Usage for debugging from root:
#   DRY_RUN=1 bash src/bronze/comext_download.sh 2002-11 2002-12

set -euo pipefail

# Helper function to print current step for observability
log() { echo "[download] $*"; }

#-------------------- PREREQUISITES
# Fail fast with a clear message if required tools are missing (if run ouside Docker).
command -v curl >/dev/null 2>&1 || { echo "[download] FAIL: curl is required" >&2; exit 2; }
command -v python >/dev/null 2>&1 || { echo "[download] FAIL: python is required" >&2; exit 2; }

# Dry run switch
DRY_RUN="${DRY_RUN:-0}"
log "DRY_RUN=$DRY_RUN"

#-------------------- COMMAND LINE INPUT ARGUMENTS
# Read
FROM="${1:-}"   # YYYY-MM
TO="${2:-}"     # YYYY-MM

# LOg the error if one or both arguments is missing
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

#-------------------- SOURCE CONFIGURATION AND LANDING ZONE
BASE_URL="https://ec.europa.eu/eurostat/api/dissemination/files"
OUT_DIR="data/raw/comext_products"

#-------------------- TEMP FILE CLEANUP 
# Ensure partial downloads do not remain if the script exits unexpectedly.
cleanup() {
  if [[ -n "${tmp:-}" && -f "${tmp:-}" ]]; then
    rm -f "${tmp:-}"
  fi
}
trap cleanup EXIT

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

log "start from=$FROM to=$TO out_dir=$OUT_DIR"

#-------------------- INGESTION LOOP (closed interval: FROM → TO)
# Iterates month by month, downloading immutable raw bulk files
while [[ "$current" < "$TO" || "$current" == "$TO" ]]; do
  yyyymm="${current/-/}"

  # Comext file name full_v2_YYYYMM.7z is preserved after download
  url="${BASE_URL}?file=comext%2FCOMEXT_DATA%2FPRODUCTS%2Ffull_v2_${yyyymm}.7z"

  out_dir="${OUT_DIR}/${current}"
  out="${out_dir}/full_v2_${yyyymm}.7z"
  tmp="${out}.part"

  # Idempotency check
  if [[ -f "$out" ]]; then
    log "exists skip month=$current file=$out"
  else
    # Prints URLs on dry run mode. 
    if [[ "$DRY_RUN" == "1" ]]; then
      log "[dry mode] month=$current"
      log "[dry mode] file=$out"
      log "[dry mode] url=$url"
    else
     # Ensure month-specific landing directory exists
      mkdir -p "$out_dir"

      log "downloading month=$current"
      log "may take a while..."

      # Download and capture HTTP status explicitly (so 404 can be logged cleanly)
      http_code=$(curl -S -L --progress-bar\
        --retry 3 --retry-delay 2 \
        --connect-timeout 20 --max-time 600 \
        -o "$tmp" -w "%{http_code}" "$url" || true)
      echo ""

      if [[ "$http_code" == "200" ]]; then
        # Atomic rename: only mark file as complete after successful download
        mv -f "$tmp" "$out"
        log "success month=$current file=$out"
        sleep 1

      elif [[ "$http_code" == "404" ]]; then
        rm -f "$tmp"
        log "NOT AVAILABLE (404) month=$current url=$url"
        log "Tip: dataset not published yet for this month"
        exit 22

      else
        rm -f "$tmp"
        log "ERROR download failed month=$current http=$http_code url=$url"
        exit 1
      fi
    fi
  fi

  current="$(next_month "$current")"
done

#-------------------- RUN COMPLETION
log "end"