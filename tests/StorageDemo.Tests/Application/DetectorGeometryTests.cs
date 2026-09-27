using StorageDemo.Core.Streaming;

namespace StorageDemo.Tests.Application;

/// <summary>Detection-plan phase D0.</summary>
public sealed class DetectorGeometryTests
{
    private static readonly DetectorGeometry.Stretch RfDetr = new(560);
    private static readonly DetectorGeometry.Letterbox Yolo = new(640);

    [Fact]
    public void A_box_survives_forward_and_back()
    {
        var cases =
            from geometry in new DetectorGeometry[] { RfDetr, Yolo }
            // Wider than square, taller than square, and an odd height so the letterbox gap is odd.
            from size in new[] { (W: 1920, H: 1080), (W: 1080, H: 1920), (W: 1280, H: 721) }
            from box in new VmtiDetection[]
            {
                new(1, 1, 1, size.W, size.H),                 // every edge
                new(1, 1, 1, 1, 1),                           // top-left pixel
                new(1, size.W, size.H, size.W, size.H),       // bottom-right pixel
                new(1, 1, 300, 50, 400),                      // left edge
                new(1, 700, 1, 900, 40),                      // top edge
                new(1, size.W - 99, 500, size.W, 640),        // right edge
                new(1, 100, size.H - 199, 400, size.H),       // bottom edge
                new(1, 401, 301, 800, 600),                   // mid-frame
            }
            select (geometry, size, box);

        Assert.All(cases, c =>
        {
            var (geometry, (w, h), box) = c;

            // Exact, which is stronger than the plan's "within a pixel": the only loss on the way
            // round is floating-point, and the nearest-edge rounding absorbs it.
            Assert.Equal(box, geometry.ToFrame(1, geometry.ToModel(box, w, h), w, h));
        });
    }

    [Fact]
    public void Stretch_maps_the_whole_frame_onto_the_whole_canvas_per_axis()
    {
        // detr.py predict: F.resize(t, [560, 560], antialias=False).
        Assert.Equal((0, 0, 560, 560), RfDetr.Place(1920, 1080));
        Assert.Equal((0, 0, 560, 560), RfDetr.ToModel(new VmtiDetection(1, 1, 1, 1920, 1080), 1920, 1080));

        // Half-pixel centres: frame edge to canvas edge, so the top-left pixel's far corner (1, 1)
        // lands at the per-axis scale and not at 1 as a corner-aligned mapping would put it.
        var (x1, y1, x2, y2) = RfDetr.ToModel(new VmtiDetection(1, 1, 1, 1, 1), 1920, 1080);

        Assert.Equal((0.0, 0.0), (x1, y1));
        Assert.Equal(560.0 / 1920, x2, 12);
        Assert.Equal(560.0 / 1080, y2, 12);
    }

    [Fact]
    public void Stretch_inverse_is_the_normalised_box_times_the_original_size()
    {
        // postprocess.py: (cx - w/2) * W0, (cy - h/2) * H0, ...
        Assert.Equal(
            new VmtiDetection(1, 201, 201, 300, 300),
            RfDetr.ToFrameNormalised(1, (Cx: 0.25, Cy: 0.5, W: 0.1, H: 0.2), 1000, 500));
    }

    [Fact]
    public void Letterbox_rounds_an_odd_pad_half_down()
    {
        // 1920 x 1071 at 640: r = 1/3, new_unpad = (640, 357), dh = 283 / 2 = 141.5, top =
        // round(141.4) = 141.
        Assert.Equal((0, 141, 640, 357), Yolo.Place(1920, 1071));

        // And the same gap on the other axis for a tall frame: 1071 x 1920 -> left = 141.
        Assert.Equal((141, 0, 357, 640), Yolo.Place(1071, 1920));

        // scale_boxes with that pad: y = (498 - 141) * 3 = 1071, the frame's last row.
        Assert.Equal(
            new VmtiDetection(1, 1, 1, 1920, 1071),
            Yolo.ToFrame(1, (X1: 0, Y1: 141, X2: 640, Y2: 498), 1920, 1071));
    }

    [Fact]
    public void Letterbox_resize_target_uses_pythons_half_to_even_round()
    {
        // 1280 x 721 at 640: r = 0.5, 721 * 0.5 = 360.5, Python round -> 360 (even), dh = 140.
        Assert.Equal((0, 140, 640, 360), Yolo.Place(1280, 721));

        // Even gap, wide frame: 1920 x 1080 -> (640, 360), dh = 140 exactly.
        Assert.Equal((0, 140, 640, 360), Yolo.Place(1920, 1080));

        // A square frame fills the canvas.
        Assert.Equal((0, 0, 640, 640), Yolo.Place(500, 500));
    }

    [Fact]
    public void Letterbox_clips_to_the_frame()
    {
        // (-5 - 0) * 3 = -15 and (700 - 0) * 3 = 2100 both leave a 1920 x 1080 frame; clip_boxes
        // clamps them to its edges.
        Assert.Equal(
            new VmtiDetection(1, 1, 1, 1920, 1080),
            Yolo.ToFrame(1, (X1: -5, Y1: 100, X2: 700, Y2: 600), 1920, 1080));
    }

    /// <summary>
    /// A rectangle covering 0-based columns 480..959 and rows 270..539 of a 1920 x 1080 frame, so
    /// ST 0903 pixels (481, 271) to (960, 540).
    /// </summary>
    [Fact]
    public void A_known_rectangle_returns_from_each_model_space()
    {
        var rectangle = new VmtiDetection(1, 481, 271, 960, 540);

        // Stretch to 560: x * 7/24 = 140..280, y * 14/27 = 140..280.
        Assert.Equal((140.0, 140.0, 280.0, 280.0), RfDetr.ToModel(rectangle, 1920, 1080));
        Assert.Equal(rectangle, RfDetr.ToFrame(1, (X1: 140, Y1: 140, X2: 280, Y2: 280), 1920, 1080));

        // As RF-DETR emits it: cx = cy = 210/560, w = h = 140/560.
        Assert.Equal(rectangle, RfDetr.ToFrameNormalised(1, (Cx: 0.375, Cy: 0.375, W: 0.25, H: 0.25), 1920, 1080));

        // Letterbox to 640: gain 1/3, pad (0, 140).
        Assert.Equal((160.0, 230.0, 320.0, 320.0), Yolo.ToModel(rectangle, 1920, 1080));
        Assert.Equal(rectangle, Yolo.ToFrame(1, (X1: 160, Y1: 230, X2: 320, Y2: 320), 1920, 1080));
    }

    [Fact]
    public void The_same_class_id_names_different_things_in_each_family()
    {
        Assert.Equal("dog", CocoClasses.RfDetr[18]);
        Assert.Equal("sheep", CocoClasses.Yolo[18]);

        Assert.Equal(80, CocoClasses.RfDetr.Count);
        Assert.Equal(80, CocoClasses.Yolo.Count);
        Assert.False(CocoClasses.RfDetr.ContainsKey(0));
        Assert.False(CocoClasses.RfDetr.ContainsKey(12));
        Assert.Equal("toothbrush", CocoClasses.RfDetr[90]);
        Assert.Equal("toothbrush", CocoClasses.Yolo[79]);
    }

    [Fact]
    public void Detection_configuration_canonicalises_models_and_coco_filters()
    {
        Assert.Equal(DetectionModels.RfDetr, DetectionModels.Normalize("RFDETR"));
        Assert.Equal(DetectionModels.Yolo26, DetectionModels.Normalize("yolo"));
        Assert.Null(DetectionModels.Normalize("default"));

        Assert.Equal(["person", "car", "truck"], CocoClasses.Normalize(["TRUCK", "car", "person", "car"]));
        Assert.Empty(CocoClasses.Normalize([]));
        Assert.Throws<ArgumentException>(() => CocoClasses.Normalize(["vehicle"]));
        Assert.Throws<ArgumentException>(() => DetectionModels.Normalize("mystery"));
    }
}
