#!/usr/bin/env bash
# Puts a Windows libsrt next to the FFmpeg that scripts/fetch-ffmpeg.sh fetches, so
# [DllImport("srt")] resolves from the application's own folder and no machine-wide install is
# involved.
# It copies rather than downloads: upstream publishes no usable Windows archive, only a 136 MB
# installer, so vcpkg is the route.
# The version floor is 1.5 for the same kind of reason fetch-ffmpeg.sh pins FFmpeg: the listener
# depends on srt_listen_callback and the SRT_REJX_* rejection codes, both of which arrived in 1.5.0.
set -euo pipefail

MIN_VERSION="1.5"
TRIPLET="x64-windows"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="$ROOT/ffmpeg/win-x64"

VCPKG_ROOT="${VCPKG_ROOT:-}"
if [ -z "$VCPKG_ROOT" ] && command -v vcpkg >/dev/null 2>&1; then
  VCPKG_ROOT="$(cd "$(dirname "$(command -v vcpkg)")" && pwd)"
fi

if [ ! -d "${VCPKG_ROOT:-/nonexistent}/installed" ]; then
  echo "No vcpkg found. Set VCPKG_ROOT, or install it:"
  echo "  git clone https://github.com/microsoft/vcpkg ~/vcpkg && ~/vcpkg/bootstrap-vcpkg.bat"
  echo "  export VCPKG_ROOT=~/vcpkg"
  exit 1
fi

BIN="$VCPKG_ROOT/installed/$TRIPLET/bin"
if [ ! -f "$BIN/srt.dll" ]; then
  echo "libsrt is not installed in $VCPKG_ROOT. Run:"
  echo "  \"$VCPKG_ROOT/vcpkg\" install libsrt:$TRIPLET"
  exit 1
fi

# vcpkg records the installed version only in this file name: libsrt_1.5.6_x64-windows.list, with an
# optional #port-revision suffix on the version.
VERSION="$(basename "$(ls "$VCPKG_ROOT"/installed/vcpkg/info/libsrt_*_"$TRIPLET".list | head -1)" .list)"
VERSION="${VERSION#libsrt_}"
VERSION="${VERSION%_$TRIPLET}"
VERSION="${VERSION%%#*}"

if [ "$(printf '%s\n%s\n' "$MIN_VERSION" "$VERSION" | sort -V | head -1)" != "$MIN_VERSION" ]; then
  echo "WARNING: vcpkg has libsrt $VERSION; the listener needs $MIN_VERSION or later"
fi

mkdir -p "$TARGET"
# srt.dll links OpenSSL dynamically, so its two DLLs travel with it.
cp "$BIN/srt.dll" "$BIN"/libcrypto-*.dll "$BIN"/libssl-*.dll "$TARGET/"

echo "Installed libsrt $VERSION to $TARGET"

if [ -f "$TARGET/srt.dll" ]; then
  echo "srt.dll is in place"
else
  echo "WARNING: srt.dll did not land in $TARGET"
fi
