#!/usr/bin/env python
"""Exports YOLO26 Nano (COCO weights, **AGPL-3.0 or Ultralytics Enterprise**) to
models/yolo26-nano.onnx with preprocessing baked into the graph, then proves the contract with
onnxruntime. Run via scripts/fetch-yolo.sh; see models/README.md for the verified tensor contract
and the licence position.

Same shape of job as scripts/export-rfdetr.py, with two differences the research records:

  * YOLO normalises by **/255 only** — no ImageNet mean or std. Getting that wrong degrades
    quietly, so verify() compares the baked graph against the stock float export fed x/255 and
    asserts the difference is nil.
  * YOLO **letterboxes** (uniform scale, pad 114, centred) where RF-DETR stretches, so the inverse
    is different. LetterBox and scale_boxes are taken from ultralytics rather than reimplemented,
    including its round-half-down padding.

    input (uint8, B x 640 x 640 x 3, NHWC)
      -> Cast(float32) -> Transpose(NCHW) -> Div(255) -> original graph

nms=False selects YOLO26's NMS-free one-to-one head, which emits (B, 300, 6) directly. Nothing to
suppress at runtime, and the export stamps end2end=True plus the class names into metadata_props,
so the .NET runner configures itself from the file instead of from a shipped class table.
"""

import sys
from importlib.metadata import version
from pathlib import Path

import numpy as np
import onnx
import onnxruntime as ort
from onnx import TensorProto, helper, numpy_helper

ROOT = Path(__file__).resolve().parent.parent
MODELS = ROOT / "models"
WEIGHTS = MODELS / "yolo26n.pt"  # downloaded by ultralytics from its own assets release
OUT = MODELS / "yolo26-nano.onnx"
RES = 640
MAX_DET = 300
SAMPLE = MODELS / "dog-2.jpeg"  # the same image scripts/export-rfdetr.py proves RF-DETR on


def export_float() -> Path:
    from ultralytics import YOLO

    model = YOLO(str(WEIGHTS))
    # nms=False -> one-to-one head; dynamic=True is the only way to get a dynamic batch axis; opset
    # pinned because ultralytics otherwise picks it from the installed torch/onnx.
    path = model.export(format="onnx", nms=False, dynamic=True, imgsz=RES, opset=18, simplify=False, verbose=False)
    return Path(path)


def bake_preprocessing(float_path: Path, out_path: Path) -> None:
    m = onnx.load(float_path)
    g = m.graph
    assert g.input[0].name == "images", [i.name for i in g.input]
    assert [o.name for o in g.output] == ["output0"], [o.name for o in g.output]

    for n in g.node:
        for i, name in enumerate(n.input):
            if name == "images":
                n.input[i] = "images_normalised"
    g.input.remove(g.input[0])
    g.input.insert(0, helper.make_tensor_value_info("images", TensorProto.UINT8, ["batch", RES, RES, 3]))
    g.initializer.append(numpy_helper.from_array(np.float32(255.0), "scale_255"))
    pre = [
        helper.make_node("Cast", ["images"], ["images_f32"], to=TensorProto.FLOAT),
        helper.make_node("Transpose", ["images_f32"], ["images_nchw"], perm=[0, 3, 1, 2]),
        helper.make_node("Div", ["images_nchw", "scale_255"], ["images_normalised"]),
    ]
    for i, n in enumerate(pre):
        g.node.insert(i, n)

    # The exporter labels output dim 2 "anchors", which is true of the raw head and false of this
    # one; the one-to-one head emits a fixed (batch, 300, 6).
    g.output.remove(g.output[0])
    g.output.insert(0, helper.make_tensor_value_info("output0", TensorProto.FLOAT, ["batch", MAX_DET, 6]))

    m.metadata_props.add(key="preprocessing", value="uint8 NHWC RGB; /255 is in the graph (no mean/std)")
    onnx.checker.check_model(m)
    onnx.save(m, out_path)


def letterbox(img_bgr):
    """Ultralytics' own LetterBox, so the padding rounding matches what the model was fed."""
    from ultralytics.data.augment import LetterBox

    return LetterBox((RES, RES), auto=False)(image=img_bgr)


def verify(float_path: Path, out_path: Path) -> None:
    import cv2
    from ultralytics.utils.ops import scale_boxes

    sess = ort.InferenceSession(str(out_path), providers=["CPUExecutionProvider"])
    ref = ort.InferenceSession(str(float_path), providers=["CPUExecutionProvider"])
    (inp,) = sess.get_inputs()
    print(f"input  {inp.name} {inp.type} {inp.shape}")
    for o in sess.get_outputs():
        print(f"output {o.name} {o.type} {o.shape}")
    assert inp.name == "images" and inp.type == "tensor(uint8)" and inp.shape[1:] == [RES, RES, 3]

    rng = np.random.default_rng(0)
    for b in (1, 2):
        x = rng.integers(0, 256, (b, RES, RES, 3), dtype=np.uint8)
        (y,) = sess.run(["output0"], {"images": x})
        assert y.shape == (b, MAX_DET, 6), y.shape
        (y_ref,) = ref.run(["output0"], {"images": x.transpose(0, 3, 1, 2).astype(np.float32) / 255})
        diff = float(np.abs(y - y_ref).max())
        print(f"batch {b}: output0 {y.shape}, max |baked - float| = {diff:.2e}")
        assert diff < 1e-4, diff
        # No NMS to run and no padding-row filter beyond score: rows come back sorted by score.
        scores = y[..., 4]
        assert scores.min() >= 0 and scores.max() <= 1, (scores.min(), scores.max())
        assert (np.diff(scores, axis=1) <= 1e-6).all(), "rows are not score-sorted"
        assert set(np.unique(y[..., 5])) <= set(range(80)), "class column is not a 0..79 id"

    meta = {p.key: p.value for p in onnx.load(out_path).metadata_props}
    print("metadata_props the .NET runner reads instead of being configured:")
    for k in ("task", "head", "end2end", "imgsz", "stride", "channels", "batch", "version", "license"):
        print(f"  {k} = {meta[k]}")
    names = eval(meta["names"])  # noqa: S307 - ultralytics writes a python dict literal
    print(f"  names = {len(names)} classes, 0-based contiguous: {names[0]!r} .. {names[79]!r}, 16 = {names[16]!r}")
    assert meta["end2end"] == "True" and meta["task"] == "detect" and meta["imgsz"] == f"[{RES}, {RES}]"

    blank = np.zeros((1, RES, RES, 3), dtype=np.uint8)
    (y,) = sess.run(["output0"], {"images": blank})
    print(f"blank image: max score {y[0, 0, 4]:.3f} ({names[int(y[0, 0, 5])]})")
    assert y[0, 0, 4] < 0.05, y[0, 0]  # much quieter than RF-DETR's 0.115 on the same input

    img = cv2.imread(str(SAMPLE))  # BGR, as ultralytics' own predict path reads it
    assert img is not None, f"{SAMPLE} missing - run scripts/export-rfdetr.py first, it fetches it"
    h0, w0 = img.shape[:2]
    x = cv2.cvtColor(letterbox(img), cv2.COLOR_BGR2RGB)[None]
    (y,) = sess.run(["output0"], {"images": np.ascontiguousarray(x)})
    boxes = scale_boxes((RES, RES), y[0, :, :4].copy(), (h0, w0))  # undoes gain and pad, clips
    top = [(float(r[4]), int(r[5]), names[int(r[5])], b) for r, b in zip(y[0], boxes) if r[4] > 0.25]
    print(f"{SAMPLE.name} ({w0}x{h0}), detections >0.25 (score, class, x1 y1 x2 y2 in original pixels):")
    for score, cid, name, b in top:
        print(f"  {score:.3f} {cid:2d} {name:<12} {b[0]:6.1f} {b[1]:6.1f} {b[2]:6.1f} {b[3]:6.1f}")
    # Not a dog: yolo26n's one-to-one head does not find the beagle RF-DETR scores at 0.686, and
    # neither does the .pt through its one-to-many head.
    assert top[0][2] == "dining table" and top[0][0] > 0.8, top[0]
    assert {"umbrella", "chair", "person", "cup"} <= {name for _, _, name, _ in top}, top

    # Strongest single check: the whole chain (letterbox, /255 in the graph, decode, inverse) must
    # land where ultralytics' own predict lands.
    from ultralytics import YOLO

    r = YOLO(float_path).predict(img, imgsz=RES, conf=0.25, rect=False, verbose=False)[0].boxes
    ref_top = sorted(zip(r.conf.tolist(), r.cls.tolist(), r.xyxy.tolist()), reverse=True)
    assert len(ref_top) == len(top), (len(ref_top), len(top))
    for (s, c, _, b), (s_r, c_r, b_r) in zip(top, ref_top):
        assert c == int(c_r) and abs(s - s_r) < 1e-4 and np.abs(np.array(b) - b_r).max() < 1e-2, (s, c, b, s_r, c_r, b_r)
    print(f"matches ultralytics predict() on the float export: {len(top)} detections, same classes, boxes to 0.01 px")


def main() -> int:
    MODELS.mkdir(exist_ok=True)
    float_path = export_float()
    bake_preprocessing(float_path, OUT)
    verify(float_path, OUT)
    float_path.unlink()
    print(f"OK {OUT} ({OUT.stat().st_size / 1e6:.1f} MB), ultralytics {version('ultralytics')}, onnxruntime {version('onnxruntime')}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
