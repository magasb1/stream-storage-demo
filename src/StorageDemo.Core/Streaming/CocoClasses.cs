namespace StorageDemo.Core.Streaming;

/// <summary>
/// The COCO class tables the two detector families emit, kept apart on purpose: RF-DETR's COCO
/// checkpoints emit the sparse COCO category id (1..90, slot 0 background), YOLO emits a contiguous
/// 0..79 index, and the same integer names a different animal in each.
/// </summary>
public static class CocoClasses
{
    private static readonly (int Id, string Name)[] Coco =
    [
        (1, "person"), (2, "bicycle"), (3, "car"), (4, "motorcycle"), (5, "airplane"),
        (6, "bus"), (7, "train"), (8, "truck"), (9, "boat"), (10, "traffic light"),
        (11, "fire hydrant"), (13, "stop sign"), (14, "parking meter"), (15, "bench"),
        (16, "bird"), (17, "cat"), (18, "dog"), (19, "horse"), (20, "sheep"),
        (21, "cow"), (22, "elephant"), (23, "bear"), (24, "zebra"), (25, "giraffe"),
        (27, "backpack"), (28, "umbrella"), (31, "handbag"), (32, "tie"), (33, "suitcase"),
        (34, "frisbee"), (35, "skis"), (36, "snowboard"), (37, "sports ball"), (38, "kite"),
        (39, "baseball bat"), (40, "baseball glove"), (41, "skateboard"), (42, "surfboard"),
        (43, "tennis racket"), (44, "bottle"), (46, "wine glass"), (47, "cup"), (48, "fork"),
        (49, "knife"), (50, "spoon"), (51, "bowl"), (52, "banana"), (53, "apple"),
        (54, "sandwich"), (55, "orange"), (56, "broccoli"), (57, "carrot"), (58, "hot dog"),
        (59, "pizza"), (60, "donut"), (61, "cake"), (62, "chair"), (63, "couch"),
        (64, "potted plant"), (65, "bed"), (67, "dining table"), (70, "toilet"), (72, "tv"),
        (73, "laptop"), (74, "mouse"), (75, "remote"), (76, "keyboard"), (77, "cell phone"),
        (78, "microwave"), (79, "oven"), (80, "toaster"), (81, "sink"), (82, "refrigerator"),
        (84, "book"), (85, "clock"), (86, "vase"), (87, "scissors"), (88, "teddy bear"),
        (89, "hair drier"), (90, "toothbrush"),
    ];

    /// <summary>RF-DETR COCO checkpoint: logit slot to name.</summary>
    public static readonly IReadOnlyDictionary<int, string> RfDetr = Coco.ToDictionary(c => c.Id, c => c.Name);

    /// <summary>YOLO: class index to name, 0..79.</summary>
    public static readonly IReadOnlyList<string> Yolo = [.. Coco.Select(c => c.Name)];

    /// <summary>The common label vocabulary, in COCO display order.</summary>
    public static IReadOnlyList<string> All => Yolo;

    /// <summary>
    /// Canonicalises an operator-supplied filter, rejecting typos rather than silently producing an
    /// empty stream.
    /// </summary>
    public static IReadOnlyList<string> Normalize(IEnumerable<string>? labels)
    {
        if (labels is null)
        {
            return [];
        }

        var requested = labels
            .Where(label => !string.IsNullOrWhiteSpace(label))
            .Select(label => label.Trim().ToLowerInvariant())
            .ToHashSet(StringComparer.Ordinal);
        var unknown = requested.Except(All, StringComparer.Ordinal).Order().ToArray();

        if (unknown.Length > 0)
        {
            throw new ArgumentException($"Unknown COCO label(s): {string.Join(", ", unknown)}.");
        }

        return [.. All.Where(requested.Contains)];
    }
}

public static class DetectionModels
{
    public const string RfDetr = "rf-detr";
    public const string Yolo26 = "yolo26";

    public static string? Normalize(string? model)
        => string.IsNullOrWhiteSpace(model) || model.Equals("default", StringComparison.OrdinalIgnoreCase)
            ? null
            : model.Trim().ToLowerInvariant() switch
            {
                "rf-detr" or "rfdetr" => RfDetr,
                "yolo26" or "yolo" => Yolo26,
                var value => throw new ArgumentException(
                    $"Unknown detection model '{value}'. Choose rf-detr, yolo26, or the worker default."),
            };
}
