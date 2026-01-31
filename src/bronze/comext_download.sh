#!/usr/bin/env bash
set -euo pipefail

# This prints the job name for observability.
log() { echo "[download] $*"; }

DRY_RUN="${DRY_RUN:-0}"

log "DRY_RUN=$DRY_RUN"

# Read command line arguments
FROM="$1"   # YYYY-MM
TO="$2"     # YYYY-MM

# Set configuration variables: source and landig zone
BASE_URL="https://ec.europa.eu/eurostat/api/dissemination/files"
OUT_DIR="data/raw/comext_products"

# Portable next_month function to advance time safely
# Solves: Given a month in YYYY-MM format, what is the next month?
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

while [[ "$current" < "$TO" || "$current" == "$TO" ]]; do
  yyyymm="${current/-/}"

  url="${BASE_URL}?file=comext%2FCOMEXT_DATA%2FPRODUCTS%2Ffull_v2_${yyyymm}.7z"

  # Deterministic layout requires full_YYYYMM.7z
  out_dir="${OUT_DIR}/${current}"
  out="${out_dir}/full_${yyyymm}.7z"
  tmp="${out}.part"

  mkdir -p "$out_dir"

  if [[ -f "$out" ]]; then
    log "exists skip month=$current file=$out"
  else
    if [[ "$DRY_RUN" == "1" ]]; then
      log "$url"
    else
      log "downloading month=$current url=$url"
      curl -fL -o "$tmp" "$url"
      mv -f "$tmp" "$out"
      log "success month=$current file=$out"
      sleep 1
    fi
  fi

  current="$(next_month "$current")"
done

log "end"