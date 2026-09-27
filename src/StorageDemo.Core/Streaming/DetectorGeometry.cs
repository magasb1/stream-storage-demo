namespace StorageDemo.Core.Streaming;

/// <summary>How a frame becomes a model input and how a model's box comes back out, as one object.</summary>
public abstract record DetectorGeometry(int InputSize)
{
    /// <summary>Where the frame's pixels land on the canvas.</summary>
    public abstract (int Left, int Top, int Width, int Height) Place(int frameWidth, int frameHeight);

    /// <summary>A frame box onto the canvas, edges in canvas pixels.</summary>
    public (double X1, double Y1, double X2, double Y2) ToModel(VmtiDetection box, int frameWidth, int frameHeight)
        // 1-based inclusive columns L..R cover the continuous span [L - 1, R).
        => Forward(box.Left - 1, box.Top - 1, box.Right, box.Bottom, frameWidth, frameHeight);

    /// <summary>A canvas box, edges in canvas pixels (YOLO's xyxy), back to the frame.</summary>
    public VmtiDetection ToFrame(int id, (double X1, double Y1, double X2, double Y2) box, int frameWidth, int frameHeight)
    {
        var (x1, y1, x2, y2) = Inverse(box.X1, box.Y1, box.X2, box.Y2, frameWidth, frameHeight);

        // Continuous edges to ST 0903's pixels: a left edge at 100.0 makes 0-based column 100 the
        // first inside, which is column 101; a right edge at 140.0 makes 139 the last, which is
        // 140.
        var left = Math.Clamp((int)Math.Round(x1) + 1, 1, frameWidth);
        var top = Math.Clamp((int)Math.Round(y1) + 1, 1, frameHeight);

        return new VmtiDetection(
            id,
            left,
            top,
            Math.Max(left, Math.Clamp((int)Math.Round(x2), 1, frameWidth)),
            Math.Max(top, Math.Clamp((int)Math.Round(y2), 1, frameHeight)));
    }

    /// <summary>
    /// RF-DETR's <c>dets</c> box: centre, width and height, each 0..1 of the canvas
    /// (research/detector-models.md section 1, "Output").
    /// </summary>
    public VmtiDetection ToFrameNormalised(int id, (double Cx, double Cy, double W, double H) box, int frameWidth, int frameHeight)
        => ToFrame(
            id,
            ((box.Cx - box.W / 2) * InputSize, (box.Cy - box.H / 2) * InputSize,
             (box.Cx + box.W / 2) * InputSize, (box.Cy + box.H / 2) * InputSize),
            frameWidth,
            frameHeight);

    protected abstract (double X1, double Y1, double X2, double Y2) Forward(double x1, double y1, double x2, double y2, int frameWidth, int frameHeight);

    protected abstract (double X1, double Y1, double X2, double Y2) Inverse(double x1, double y1, double x2, double y2, int frameWidth, int frameHeight);

    /// <summary>
    /// RF-DETR: the whole frame resized onto the whole canvas, aspect ratio destroyed, no padding.
    /// </summary>
    public sealed record Stretch(int InputSize) : DetectorGeometry(InputSize)
    {
        public override (int Left, int Top, int Width, int Height) Place(int frameWidth, int frameHeight)
            => (0, 0, InputSize, InputSize);

        // Half-pixel centres (align_corners=False) mean frame edge to canvas edge, so a pixel's
        // centre i + 0.5 lands at (i + 0.5) * InputSize / frameWidth.
        protected override (double X1, double Y1, double X2, double Y2) Forward(double x1, double y1, double x2, double y2, int frameWidth, int frameHeight)
            => (x1 * InputSize / frameWidth, y1 * InputSize / frameHeight, x2 * InputSize / frameWidth, y2 * InputSize / frameHeight);

        // src/rfdetr/models/postprocess.py, PostProcess._gather_and_scale_boxes: the normalised box
        // times [W0, H0, W0, H0] of the ORIGINAL image, no gain, no pad, per axis.
        protected override (double X1, double Y1, double X2, double Y2) Inverse(double x1, double y1, double x2, double y2, int frameWidth, int frameHeight)
            => (x1 / InputSize * frameWidth, y1 / InputSize * frameHeight, x2 / InputSize * frameWidth, y2 / InputSize * frameHeight);
    }

    /// <summary>
    /// YOLO: the frame scaled uniformly by the long side and centred on a canvas of <paramref
    /// name="PadValue"/>.
    /// </summary>
    public sealed record Letterbox(int InputSize, int PadValue = 114) : DetectorGeometry(InputSize)
    {
        public override (int Left, int Top, int Width, int Height) Place(int frameWidth, int frameHeight)
        {
            // LetterBox.__call__:
            //   r = min(new_shape[0] / shape[0], new_shape[1] / shape[1])
            //   new_unpad = round(shape[1] * r), round(shape[0] * r)
            //   dw, dh = (new_shape[1] - new_unpad[0]) / 2, (new_shape[0] - new_unpad[1]) / 2
            //   top, bottom = round(dh - 0.1), round(dh + 0.1)
            //   left, right = round(dw - 0.1), round(dw + 0.1)
            // The 0.1 makes an x.5 pad round down on the leading edge and up on the trailing one,
            // so the two always sum to the whole gap. Python's round is half-to-even, as is
            // Math.Round.
            var gain = Gain(frameWidth, frameHeight);
            var width = (int)Math.Round(frameWidth * gain);
            var height = (int)Math.Round(frameHeight * gain);

            return (
                (int)Math.Round((InputSize - width) / 2.0 - 0.1),
                (int)Math.Round((InputSize - height) / 2.0 - 0.1),
                width,
                height);
        }

        // Frame pixels times the gain, plus the pad.
        protected override (double X1, double Y1, double X2, double Y2) Forward(double x1, double y1, double x2, double y2, int frameWidth, int frameHeight)
        {
            var gain = Gain(frameWidth, frameHeight);
            var (padX, padY, _, _) = Place(frameWidth, frameHeight);

            return (x1 * gain + padX, y1 * gain + padY, x2 * gain + padX, y2 * gain + padY);
        }

        // ultralytics/utils/ops.py, scale_boxes with ratio_pad=None:
        //   gain  = min(img1_shape[0] / img0_shape[0], img1_shape[1] / img0_shape[1])
        //   pad_x = round((img1_shape[1] - round(img0_shape[1] * gain)) / 2 - 0.1)
        //   pad_y = round((img1_shape[0] - round(img0_shape[0] * gain)) / 2 - 0.1)
        //   boxes[..., [0, 2]] -= pad_x; boxes[..., [1, 3]] -= pad_y; boxes /= gain
        //   clip_boxes(boxes, img0_shape)
        // pad_x and pad_y are the same expression as Place's left and top, so they come from it.
        protected override (double X1, double Y1, double X2, double Y2) Inverse(double x1, double y1, double x2, double y2, int frameWidth, int frameHeight)
        {
            var gain = Gain(frameWidth, frameHeight);
            var (padX, padY, _, _) = Place(frameWidth, frameHeight);

            return ((x1 - padX) / gain, (y1 - padY) / gain, (x2 - padX) / gain, (y2 - padY) / gain);
        }

        private double Gain(int frameWidth, int frameHeight)
            => Math.Min((double)InputSize / frameHeight, (double)InputSize / frameWidth);
    }
}
