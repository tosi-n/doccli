#!/usr/bin/env bash
set -euo pipefail

# Copy (do not move) selected jack-2 files into vendor/jack2/ for reference.

JACK2_ROOT="${1:-${JACK2_ROOT:-/Users/tosi-n/Documents/Dev/Jenesys/jack-2}}"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if [ ! -d "${JACK2_ROOT}" ]; then
  echo "ERROR: jack-2 root not found: ${JACK2_ROOT}" >&2
  exit 1
fi

DEST_ROOT="${REPO_ROOT}/vendor/jack2"
mkdir -p "${DEST_ROOT}"

copy_file() {
  local rel="$1"
  local src="${JACK2_ROOT}/${rel}"
  local dst="${DEST_ROOT}/${rel}"
  if [ ! -f "${src}" ]; then
    echo "ERROR: missing upstream file: ${src}" >&2
    exit 1
  fi
  mkdir -p "$(dirname "${dst}")"
  cp -f "${src}" "${dst}"
}

FILES=(
  "backend/app/services/banking/bank_statement.py"
  "backend/app/core/enums/banking/bank_statement.py"
  "backend/app/services/banking/conversion/docuclipper.py"
)

for f in "${FILES[@]}"; do
  copy_file "${f}"
done

echo "Synced ${#FILES[@]} files from ${JACK2_ROOT} -> ${DEST_ROOT}"

