using System.Diagnostics;
using Microsoft.Extensions.Logging;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Detection;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>
/// YOLO26 Nano through the same runner, against the sample image models/README.md documents.
/// </summary>
[Collection(OnnxCollection.Name)]
public sealed class YoloDetectorTests(ITestOutputHelper output) : DetectorTests
{
    private static readonly string Model = Path.Combine(Root, "models", "yolo26-nano.onnx");
    private static readonly string RfDetr = Path.Combine(Root, "models", "rf-detr-nano.onnx");
    private static readonly string Dog = Path.Combine(Root, "models", "dog-2.jpeg");

    private const string NoModel = "models/yolo26-nano.onnx is absent; run scripts/fetch-yolo.sh";

    /// <summary>
    /// models/README.md, "Expected result on the sample image", everything above 0.7, in original
    /// pixels.
    /// </summary>
    private static readonly (string Class, int Percent, double X1, double Y1, double X2, double Y2)[] Expected =
    [
        ("dining table", 86, 0.0, 881.8, 717.8, 1278.8),
        ("cup", 85, 179.0, 843.4, 262.4, 1006.7),
        ("chair", 80, 0.3, 673.5, 83.8, 919.9),
        ("chair", 80, 580.5, 669.5, 720.0, 975.3),
        ("person", 79, 13.0, 490.3, 176.7, 691.1),
        ("umbrella", 78, 29.6, 1.0, 719.5, 307.4),
    ];

    /// <summary>
    /// The reference letterboxes with OpenCV's INTER_LINEAR and this runner uses swscale's fast
    /// bilinear, so scores move by a few hundredths and edges by a few pixels; the README says as
    /// much for RF-DETR and the same applies here.
    /// </summary>
    private const int Pixels = 8;
    private const int Percent = 6;

    [Fact]
    public void The_dog_picture_yields_the_table_the_cup_the_chairs_and_no_dog()
    {
        Assert.SkipUnless(File.Exists(Model), NoModel);

        using var detector = Detector(threshold: 0.7f);

        var detections = detector.Detect(Decode(Dog));

        foreach (var detection in detections)
        {
            output.WriteLine($"{detection.ConfidencePercent,3} {detection.OntologyClass,-14} {detection.Left,5} {detection.Top,5} {detection.Right,5} {detection.Bottom,5}");
        }

        Assert.Equal(Expected.Length, detections.Length);

        // Order is the model's own, score descending, and the decode preserves it.
        foreach (var (expected, actual) in Expected.Zip(detections))
        {
            Assert.Equal(expected.Class, actual.OntologyClass);
            Assert.InRange(actual.ConfidencePercent ?? 0, expected.Percent - Percent, expected.Percent + Percent);
            Assert.InRange(actual.Left, (int)expected.X1 + 1 - Pixels, (int)expected.X1 + 1 + Pixels);
            Assert.InRange(actual.Top, (int)expected.Y1 + 1 - Pixels, (int)expected.Y1 + 1 + Pixels);
            Assert.InRange(actual.Right, (int)expected.X2 - Pixels, (int)expected.X2 + Pixels);
            Assert.InRange(actual.Bottom, (int)expected.Y2 - Pixels, (int)expected.Y2 + Pixels);
        }
    }

    /// <summary>The difference between the two models, pinned rather than remembered.</summary>
    [Fact]
    public void There_is_no_dog_at_any_score()
    {
        Assert.SkipUnless(File.Exists(Model), NoModel);

        using var detector = Detector(threshold: 0.01f);

        Assert.DoesNotContain(detector.Detect(Decode(Dog)), d => d.OntologyClass == "dog");
    }

    [Fact]
    public void A_blank_frame_yields_nothing()
    {
        Assert.SkipUnless(File.Exists(Model), NoModel);

        // A blank frame tops out at 0.001 here, far quieter than RF-DETR's 0.115, so the threshold
        // that has to hold is a hundredth rather than a fifth.
        using var detector = Detector(threshold: 0.01f);

        Assert.Empty(detector.Detect(Blank(640, 640)));
    }

    /// <summary>
    /// The dynamic batch axis took at export, and the second frame is decoded as its own picture.
    /// </summary>
    [Fact]
    public void A_batch_of_two_gives_each_frame_its_own_identical_result()
    {
        Assert.SkipUnless(File.Exists(Model), NoModel);

        using var detector = Detector(threshold: 0.7f);
        var frame = Decode(Dog);

        var single = detector.Detect(frame);
        var batch = detector.Detect([frame, frame]);

        Assert.Equal(2, batch.Length);
        Assert.Equal(single, batch[0]);
        Assert.Equal(single, batch[1]);
        Assert.Contains(single, d => d.OntologyClass == "dining table");

        // Timing with this the only session alive, for the record rather than for an assertion, the
        // same shape OnnxDetectorTests prints for RF-DETR so the two are comparable.
        var stopwatch = Stopwatch.StartNew();
        const int Rounds = 5;

        for (var i = 0; i < Rounds; i++)
        {
            detector.Detect(frame);
        }

        var one = stopwatch.Elapsed / Rounds;
        stopwatch.Restart();

        for (var i = 0; i < Rounds; i++)
        {
            detector.Detect([frame, frame]);
        }

        output.WriteLine($"{detector.Provider}: one frame {one.TotalMilliseconds:F0} ms, batch of two {(stopwatch.Elapsed / Rounds).TotalMilliseconds:F0} ms");
    }

    /// <summary>
    /// The incompatibility pinned at the point it would ship: 18 is a sheep in this file's own
    /// metadata and a dog in RF-DETR's sparse table.
    /// </summary>
    [Fact]
    public void Class_18_is_a_sheep_here_and_a_dog_in_rf_detr()
    {
        Assert.SkipUnless(File.Exists(Model), NoModel);

        using var detector = Detector(threshold: 0.7f);

        Assert.Contains(Log, line => line.Contains("Class names read from the model's own metadata: 80", StringComparison.Ordinal));
        Assert.Equal(80, detector.Classes.Count);
        Assert.Equal("sheep", detector.Classes[18]);
        Assert.Equal("person", detector.Classes[0]);
        Assert.Equal("toothbrush", detector.Classes[79]);

        Assert.Equal("dog", DetectorDescriptor.RfDetrNano.Classes[18]);
    }

    /// <summary>
    /// The seam itself: two models, two geometries, two decodes, one <see cref="IDetector"/> and
    /// one runner, in one test.
    /// </summary>
    [Fact]
    public void Both_descriptors_run_through_the_same_runner()
    {
        Assert.SkipUnless(File.Exists(Model), NoModel);
        Assert.SkipUnless(File.Exists(RfDetr), "models/rf-detr-nano.onnx is absent; run scripts/fetch-rfdetr.sh");

        var frame = Decode(Dog);

        using var yolo = new OnnxDetector(Model, DetectorDescriptor.Yolo26Nano, 0.7f, new ListLogger(Log), "cpu");
        using var rfdetr = new OnnxDetector(RfDetr, DetectorDescriptor.RfDetrNano, 0.7f, new ListLogger(Log), "cpu");

        foreach (IDetector detector in (IDetector[])[yolo, rfdetr])
        {
            var detections = detector.Detect(frame);

            Assert.NotEmpty(detections);
            Assert.All(detections, d => Assert.InRange(d.Right, d.Left, 720));
            Assert.All(detections, d => Assert.InRange(d.Bottom, d.Top, 1280));

            // Both find the umbrella across the top of the picture, which is the one thing they
            // agree on well enough to assert through the interface: the same object, at the same
            // place, through two geometries.
            var umbrella = Assert.Single(detections, d => d.OntologyClass == "umbrella");
            Assert.InRange(umbrella.Right, 700, 720);
            Assert.InRange(umbrella.Bottom, 290, 320);
        }

        // Interleaved, because perf-detection.md's own list of traps says this laptop drifts 50
        // percent across an hour and a before-and-after pair differs by more than any real change.
        const int Rounds = 10;
        var times = new double[2][] { new double[Rounds], new double[Rounds] };
        IDetector[] both = [yolo, rfdetr];

        foreach (var detector in both)
        {
            detector.Detect(frame);
        }

        for (var round = 0; round < Rounds; round++)
        {
            for (var i = 0; i < both.Length; i++)
            {
                var stopwatch = Stopwatch.StartNew();
                both[i].Detect(frame);
                times[i][round] = stopwatch.Elapsed.TotalMilliseconds;
            }
        }

        string[] names = ["YOLO26 Nano ", "RF-DETR Nano"];

        for (var i = 0; i < both.Length; i++)
        {
            Array.Sort(times[i]);
            output.WriteLine($"{names[i]}  median {times[i][Rounds / 2]:F0} ms, best {times[i][0]:F0} ms");
        }
    }

    private OnnxDetector Detector(float threshold)
        => new(Model, DetectorDescriptor.Yolo26Nano, threshold, new ListLogger(Log), "cpu");
}
