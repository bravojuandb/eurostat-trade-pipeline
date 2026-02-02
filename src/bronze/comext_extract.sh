#!/usr/bin/env bash
# Extracts raw .7z bulk archives into .dat files for a single snapshot.
# Intended to run after comext_download.sh

set -euo pipefail

# Helper function to print current step for observability
log() { echo "[extract] $*"; }

#-------------------- PREREQUISITES
# Fail fast with a clear message if required tools are missing (if run ouside Docker).
command -v 7z >/dev/null 2>&1 || { echo "[extract] FAIL: 7z is required" >&2; exit 2; }
command -v find >/dev/null 2>&1 || { echo "[extract] FAIL: find is required" >&2; exit 2; }

#-------------------- SNAPSHOT RESOLUTION (select snapshot to extract)
# By default, extract targets the latest snapshot. 
# SNAPSHOT_ID is optional and intended for re-extraction of an existing snapshot.

RAW_ROOT="data/raw"
SNAPSHOT_PREFIX="comext__"
SNAPSHOT_ID="${SNAPSHOT_ID:-}"

if [[ -z "$SNAPSHOT_ID" ]]; then
  # Default: extract the latest snapshot directory by lexical order (timestamped names)
  BASE_DIR="$(find "$RAW_ROOT" -maxdepth 1 -type d -name "${SNAPSHOT_PREFIX}*" 2>/dev/null | sort | tail -n 1)"
else
  BASE_DIR="${RAW_ROOT}/${SNAPSHOT_PREFIX}${SNAPSHOT_ID}"
fi

if [[ -z "$BASE_DIR" || ! -d "$BASE_DIR" ]]; then
  echo "[extract] FAIL base_dir not found: $BASE_DIR" >&2
  exit 2
fi

log "start snapshot=${SNAPSHOT_ID:-auto-latest} base_dir=$BASE_DIR"

#-------------------- EXTRACTION LOOP
# Extracts raw bulk archives into working .dat files
while read -r archive; do
  [[ -z "$archive" ]] && continue
  dir="$(dirname "$archive")"
  base="$(basename "$archive")"          # full_YYYYMM.7z
  yyyymm="${base#full_}"; yyyymm="${yyyymm%.7z}"

  tmp_dir=""

  tmp_dir="${dir}/_tmp_extract_${yyyymm}"

  cleanup_tmp() {
    if [[ -n "${tmp_dir:-}" && -d "${tmp_dir:-}" ]]; then
      rm -rf "${tmp_dir:-}"
    fi
  }
  trap cleanup_tmp EXIT

  dat="${dir}/full_${yyyymm}.dat"

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
  trap - EXIT
  log "success dat=$dat"
done < <(find "$BASE_DIR" -maxdepth 2 -type f -name "full_*.7z" | sort)

#-------------------- RUN COMPLETION
log "end"