#!/usr/bin/env bash
# Extracts full_v2_YYYYMM.7z bulk archives into full_v2_YYYYMM.dat files.
# Intended to run inside a Docker container after comext_download.sh YYYY-MM YYYY-MM
# Usage for debugging from repo root:
#   bash src/bronze/comext_extract.sh

set -euo pipefail

# Helper function to print current step for observability
log() { echo "[extract] $*"; }

#-------------------- PREREQUISITES
# Fail fast with a clear message if required tools are missing (when running ouside Docker).
command -v 7z >/dev/null 2>&1 || { echo "[extract] FAIL: 7z is required" >&2; exit 2; }
command -v find >/dev/null 2>&1 || { echo "[extract] FAIL: find is required" >&2; exit 2; }

#-------------------- SOURCE CONFIGURATION
BASE_DIR="data/raw/comext_products"

if [[ -z "$BASE_DIR" || ! -d "$BASE_DIR" ]]; then
  echo "[extract] FAIL base_dir not found: $BASE_DIR" >&2
  exit 2
fi

log "base_dir=$BASE_DIR"

#-------------------- TEMP FILE CLEANUP 
# Ensure partial downloads do not remain if the script exits unexpectedly.
tmp_dir=""
cleanup_tmp() {
  if [[ -n "${tmp_dir:-}" && -d "${tmp_dir:-}" ]]; then
    rm -rf "${tmp_dir:-}"
  fi
}
trap cleanup_tmp EXIT

#-------------------- EXTRACTION LOOP
# Extracts raw bulk archives into working .dat files
while read -r archive; do
  [[ -z "$archive" ]] && continue
  dir="$(dirname "$archive")"
  base="$(basename "$archive")"          # full_v2_YYYYMM.7z
  stem="${base%.7z}"                      # full_v2_YYYYMM

  tmp_dir="${dir}/_tmp_extract_${stem}"
  dat="${dir}/${stem}.dat"

  if [[ -f "$dat" ]]; then
    log "exists skip file=$dat"
    tmp_dir=""
    continue
  fi

  log "extracting archive=$archive"
  rm -rf "$tmp_dir"
  mkdir -p "$tmp_dir"

  # Extract into a temp folder so partial extraction doesn't look “done”
  7z x "$archive" -o"$tmp_dir" >/dev/null

  # Find the extracted .dat (should match the archive stem)
  extracted_dat="$(find "$tmp_dir" -maxdepth 1 -type f -name "*.dat" | head -n 1 || true)"
  if [[ -z "$extracted_dat" ]]; then
    log "FAIL no .dat found after extracting $archive"
    exit 3
  fi

  mv -f "$extracted_dat" "$dat"
  rm -rf "$tmp_dir"
  tmp_dir=""  # prevent cleanup from touching unrelated paths
  log "success dat=$dat"
done < <(find "$BASE_DIR" -maxdepth 2 -type f -name "full_v2_*.7z" | sort)

#-------------------- RUN COMPLETION
log "end"