#!/usr/bin/env bash
set -euo pipefail

BASE_DIR="data/raw/comext_products"

log() { echo "[extract] $*"; }

log "start base_dir=$BASE_DIR"

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

log "end"