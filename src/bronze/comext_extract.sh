#!/usr/bin/env bash
set -euo pipefail

# Helper function to print current step for observability
log() { echo "[extract] $*"; }

BASE_DIR="data/raw/comext_products"

#-------------------- SNAPSHOT RESOLUTION (select latest snapshot)
SNAPSHOT_ID="${SNAPSHOT_ID:-}"

if [[ -z "$SNAPSHOT_ID" ]]; then
  # Default: extract latest snapshot by lexical order
  BASE_DIR="$(ls -d data/raw/comext__* 2>/dev/null | sort | tail -n 1)"
else
  BASE_DIR="data/raw/comext__${SNAPSHOT_ID}"
fi

if [[ -z "$BASE_DIR" || ! -d "$BASE_DIR" ]]; then
  echo "[extract] FAIL base_dir not found: $BASE_DIR" >&2
  exit 2
fi

log "start snapshot=${SNAPSHOT_ID:-auto-latest} base_dir=$BASE_DIR"

#-------------------- EXTRACTION LOOP
# Extracts raw bulk archives into working .dat files
find "$BASE_DIR" -type f -name "full_*.7z" | sort | while read -r archive; do
  dir="$(dirname "$archive")"
  base="$(basename "$archive")"          # full_YYYYMM.7z
  yyyymm="${base#full_}"; yyyymm="${yyyymm%.7z}"

  dat="${dir}/full_${yyyymm}.dat"
  tmp_dir="${dir}/_tmp_extract_${yyyymm}"

  if [[ -f "$dat" ]]; then
    log "exists skip file=$dat"
    continue
  fi

  log "extracting archive=$archive"
  rm -rf "$tmp_dir"
  mkdir -p "$tmp_dir"

  # Extract into a temp folder so partial extraction doesn't look “done”
  7z x "$archive" -o"$tmp_dir" >/dev/null

  # Find the extracted .dat (source often names it full_v2_YYYYMM.dat)
  extracted_dat="$(find "$tmp_dir" -maxdepth 1 -type f -name "*.dat" | head -n 1 || true)"
  if [[ -z "$extracted_dat" ]]; then
    log "FAIL no .dat found after extracting $archive"
    exit 3
  fi

  mv -f "$extracted_dat" "$dat"
  rm -rf "$tmp_dir"
  log "success dat=$dat"
done

#-------------------- RUN COMPLETION
log "end"