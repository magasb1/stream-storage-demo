#!/usr/bin/env bash
# Fetches Esri's "Sample video for Full Motion Video" tutorial transport stream, which the MISB
# decoder test uses as its one piece of real-world evidence.
# Run it once; the result is gitignored and stays out of the repository, which carries no binary
# fixtures.
# 96 MB. Esri publish the item with no stated licence, which is the other reason it is fetched
# rather than committed.
set -euo pipefail

ITEM="${FMV_SAMPLE_ITEM:-55ec6f32d5e342fcbfba376ca2cc409a}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="$ROOT/data/fmv"
URL="https://www.arcgis.com/sharing/rest/content/items/$ITEM/data"

mkdir -p "$TARGET"

echo "Fetching $URL"
curl -fsSL -o "$TARGET/esri-fmv-sample.zip" "$URL"

echo "Extracting"
unzip -oq "$TARGET/esri-fmv-sample.zip" -d "$TARGET"

if [ -f "$TARGET/FMV tutorial data/Truck.ts" ]; then
  echo "Installed to $TARGET/FMV tutorial data/Truck.ts"
else
  echo "WARNING: the archive did not contain 'FMV tutorial data/Truck.ts'"
  exit 1
fi
