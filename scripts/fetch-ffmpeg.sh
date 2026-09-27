#!/usr/bin/env bash
# Fetches an FFmpeg build with libsrt, which the bundled LGPL package does not have.
# Run it once and rebuild; the result is copied into the application output and used in place of the
# bundled libraries.
# The version is pinned because the FFmpeg.AutoGen bindings are: 8.1 carries the same library majors
# as 8.0 (avcodec 62, avutil 60, avformat 62), which is what the bindings expect.
set -euo pipefail

VERSION="${FFMPEG_VERSION:-n8.1}"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET="$ROOT/ffmpeg/win-x64"
URL="https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/ffmpeg-$VERSION-latest-win64-gpl-shared-${VERSION#n}.zip"

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

echo "Fetching $URL"
curl -fsSL -o "$WORK/ffmpeg.zip" "$URL"

echo "Extracting"
unzip -q "$WORK/ffmpeg.zip" -d "$WORK"

mkdir -p "$TARGET"
cp "$WORK"/ffmpeg-*/bin/*.dll "$WORK"/ffmpeg-*/bin/ffmpeg.exe "$WORK"/ffmpeg-*/bin/ffprobe.exe "$TARGET/"

echo "Installed to $TARGET"

if "$TARGET/ffmpeg.exe" -hide_banner -protocols | tr ',' '\n' | tr -d ' ' | grep -qix srt; then
  echo "SRT is available"
else
  echo "WARNING: this build has no SRT"
fi
