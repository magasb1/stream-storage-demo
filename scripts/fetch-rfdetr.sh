#!/usr/bin/env bash
# Produces models/rf-detr-nano.onnx: RF-DETR Nano, COCO weights, Apache-2.0, with uint8 input and
# ImageNet normalisation baked into the graph.
# Roboflow publishes no pre-exported ONNX, so this builds a Python venv under .venv/ (gitignored),
# installs the pinned exporter stack on the CPU torch wheels, and runs scripts/export-rfdetr.py,
# which downloads the MD5-verified weights, exports, rewrites the graph input, and proves the result
# with onnxruntime at batch 1 and 2 and on a sample image.
# Versions are pinned because the exported graph is what the .NET runner is tested against; a
# different rfdetr may change tensor names or shapes silently.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV="$ROOT/.venv"
PINS=(rfdetr==1.10.1 torch==2.14.0+cpu torchvision==0.29.0+cpu onnxruntime==1.30.0 onnx==1.22.0)
TORCH_INDEX="https://download.pytorch.org/whl/cpu"

if command -v uv >/dev/null; then
  [ -d "$VENV" ] || uv venv "$VENV"
  uv pip install --python "$VENV" --extra-index-url "$TORCH_INDEX" --index-strategy unsafe-best-match "${PINS[@]}"
else
  [ -d "$VENV" ] || python -m venv "$VENV"
  "$VENV"/Scripts/python -m pip install --quiet --extra-index-url "$TORCH_INDEX" "${PINS[@]}"
fi

PY="$VENV/Scripts/python"
[ -x "$PY" ] || PY="$VENV/bin/python"

"$PY" "$ROOT/scripts/export-rfdetr.py"

MODEL="$ROOT/models/rf-detr-nano.onnx"
if [ -s "$MODEL" ]; then
  echo "Installed $MODEL ($(du -h "$MODEL" | cut -f1))"
else
  echo "ERROR: $MODEL was not produced" >&2
  exit 1
fi
