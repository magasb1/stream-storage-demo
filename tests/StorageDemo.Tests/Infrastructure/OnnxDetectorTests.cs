using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Runtime.InteropServices;
using FFmpeg.AutoGen.Abstractions;
using Microsoft.Extensions.Logging;
using StorageDemo.Core.Streaming;
using StorageDemo.Infrastructure.Detection;
using StorageDemo.Infrastructure.Media;
using StorageDemo.Infrastructure.Streaming;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>
/// Inference saturates every core for seconds at a time, and the live-stream integration tests
/// measure grace periods on a wall clock; run alongside, five of them time out.
/// </summary>
[CollectionDefinition(Name, DisableParallelization = true)]
public sealed class OnnxCollection
{
    public const string Name = "onnx";
}

/// <summary>
/// What both detectors' tests need and neither owns: the checkout, a picture decoded by libav, a
/// blank frame, and a log the test can read back.
/// </summary>
public abstract unsafe class DetectorTests : IDisposable
{
    protected static readonly string Root = FindRoot();

    protected readonly List<string> Log = [];

    private readonly List<IntPtr> _frames = [];

    public void Dispose()
    {
        foreach (var frame in _frames)
        {
            var pointer = (AVFrame*)frame;
            ffmpeg.av_frame_free(&pointer);
        }

        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// The first picture in a file, decoded by libav; a JPEG arrives as yuvj420p, which is the
    /// runner's problem.
    /// </summary>
    protected IntPtr Decode(string path)
    {
        FfmpegLibrary.EnsureLoaded();

        AVFormatContext* format = null;
        AVCodecContext* codec = null;
        AVPacket* packet = null;

        try
        {
            Assert.True(ffmpeg.avformat_open_input(&format, path, null, null) >= 0, $"could not open {path}");
            Assert.True(ffmpeg.avformat_find_stream_info(format, null) >= 0);

            AVCodec* decoder = null;
            var stream = ffmpeg.av_find_best_stream(format, AVMediaType.AVMEDIA_TYPE_VIDEO, -1, -1, &decoder, 0);
            Assert.True(stream >= 0 && decoder is not null, "no video stream");

            codec = ffmpeg.avcodec_alloc_context3(decoder);
            Assert.True(ffmpeg.avcodec_parameters_to_context(codec, format->streams[stream]->codecpar) >= 0);
            Assert.True(ffmpeg.avcodec_open2(codec, decoder, null) >= 0);

            packet = ffmpeg.av_packet_alloc();
            var frame = ffmpeg.av_frame_alloc();
            _frames.Add((IntPtr)frame);

            while (ffmpeg.av_read_frame(format, packet) >= 0)
            {
                if (packet->stream_index == stream && ffmpeg.avcodec_send_packet(codec, packet) >= 0
                    && ffmpeg.avcodec_receive_frame(codec, frame) == 0)
                {
                    break;
                }

                ffmpeg.av_packet_unref(packet);
            }

            Assert.True(frame->width > 0, $"nothing decoded from {path}");

            return (IntPtr)frame;
        }
        finally
        {
            if (packet is not null)
            {
                ffmpeg.av_packet_free(&packet);
            }

            if (codec is not null)
            {
                ffmpeg.avcodec_free_context(&codec);
            }

            if (format is not null)
            {
                ffmpeg.avformat_close_input(&format);
            }
        }
    }

    protected IntPtr Blank(int width, int height)
    {
        FfmpegLibrary.EnsureLoaded();

        var frame = ffmpeg.av_frame_alloc();
        _frames.Add((IntPtr)frame);

        frame->format = (int)AVPixelFormat.AV_PIX_FMT_RGB24;
        frame->width = width;
        frame->height = height;
        Assert.True(ffmpeg.av_frame_get_buffer(frame, 0) >= 0);

        NativeMemory.Clear(frame->data[0], (nuint)(frame->linesize[0] * height));

        return (IntPtr)frame;
    }

    /// <summary>Walks up from the test binaries to the checkout, which is where models/ lives.</summary>
    private static string FindRoot()
    {
        var directory = new DirectoryInfo(AppContext.BaseDirectory);

        while (directory is not null && !Directory.Exists(Path.Combine(directory.FullName, "models")))
        {
            directory = directory.Parent;
        }

        return directory?.FullName ?? AppContext.BaseDirectory;
    }

    /// <summary>Captures formatted log lines so a test can read what the runner said about itself.</summary>
    protected sealed class ListLogger(List<string> lines) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
            => lines.Add(formatter(state, exception));
    }
}

/// <summary>
/// RF-DETR Nano through the real runner, against the sample image models/README.md documents.
/// </summary>
[Collection(OnnxCollection.Name)]
public sealed class OnnxDetectorTests(ITestOutputHelper output) : DetectorTests
{
    private static readonly string Model = Path.Combine(Root, "models", "rf-detr-nano.onnx");
    private static readonly string Dog = Path.Combine(Root, "models", "dog-2.jpeg");

    private const string NoModel = "models/rf-detr-nano.onnx is absent; run scripts/fetch-rfdetr.sh";

    [Fact]
    public void The_dog_picture_yields_the_dog_the_cup_and_the_umbrella_where_the_readme_says()
    {
        Assert.SkipUnless(File.Exists(Model), NoModel);

        using var detector = Detector(threshold: 0.5f);

        var detections = detector.Detect(Decode(Dog));

        foreach (var detection in detections)
        {
            output.WriteLine($"{detection.ConfidencePercent,3} {detection.OntologyClass,-14} {detection.Left,5} {detection.Top,5} {detection.Right,5} {detection.Bottom,5}");
        }

        // README, "Expected result on the sample image": 0.686 18 dog 158.2 493.1 459.1 850.0.
        var dog = Assert.Single(detections, d => d.OntologyClass == "dog");

        Assert.True(dog.ConfidencePercent > 50, $"dog scored {dog.ConfidencePercent}");
        Assert.InRange(dog.Left, 158 - 30, 158 + 30);
        Assert.InRange(dog.Top, 493 - 30, 493 + 30);
        Assert.InRange(dog.Right, 459 - 30, 459 + 30);
        Assert.InRange(dog.Bottom, 850 - 30, 850 + 30);

        Assert.Contains(detections, d => d.OntologyClass == "cup" && d.ConfidencePercent > 70);
        Assert.Contains(detections, d => d.OntologyClass == "umbrella" && d.ConfidencePercent > 70);
    }

    [Fact]
    public void A_blank_frame_yields_nothing()
    {
        Assert.SkipUnless(File.Exists(Model), NoModel);

        // The README says an all-black frame scores 0.000; the file as exported does not: fed zeros
        // from Python with the same runtime it says "potted plant" at 0.115 over the whole frame,
        // and this runner says exactly the same, which is the actual evidence that the bytes arrive
        // as intended.
        using var detector = Detector(threshold: 0.2f);

        Assert.Empty(detector.Detect(Blank(384, 384)));
    }

    /// <summary>
    /// The dynamic batch axis took at export, and the second frame is decoded as its own picture.
    /// </summary>
    [Fact]
    public void A_batch_of_two_gives_each_frame_its_own_identical_result()
    {
        Assert.SkipUnless(File.Exists(Model), NoModel);

        using var detector = Detector(threshold: 0.5f);
        var frame = Decode(Dog);

        var single = detector.Detect(frame);
        var batch = detector.Detect([frame, frame]);

        Assert.Equal(2, batch.Length);
        Assert.Equal(single, batch[0]);
        Assert.Equal(single, batch[1]);
        Assert.Contains(single, d => d.OntologyClass == "dog");

        // Timing, for the record rather than for an assertion: a laptop under load is not a
        // benchmark, and a failing assertion here would only ever fail on the wrong machine.
        var stopwatch = Stopwatch.StartNew();
        const int rounds = 5;

        for (var i = 0; i < rounds; i++)
        {
            detector.Detect(frame);
        }

        var one = stopwatch.Elapsed / rounds;
        stopwatch.Restart();

        for (var i = 0; i < rounds; i++)
        {
            detector.Detect([frame, frame]);
        }

        var two = stopwatch.Elapsed / rounds;

        output.WriteLine($"{detector.Provider}: one frame {one.TotalMilliseconds:F0} ms, batch of two {two.TotalMilliseconds:F0} ms");
    }

    /// <summary>The one number the worker has no other way to see.</summary>
    [Fact]
    public void A_detection_is_timed_onto_the_pods_meter_by_provider_and_batch()
    {
        Assert.SkipUnless(File.Exists(Model), NoModel);

        var measured = new List<(double Ms, string Provider, int Batch)>();

        using var listener = new MeterListener();

        listener.InstrumentPublished = (instrument, self) =>
        {
            if (instrument.Meter.Name == LiveMetrics.MeterName && instrument.Name == "live.detection.duration")
            {
                Assert.Equal("ms", instrument.Unit);
                self.EnableMeasurementEvents(instrument);
            }
        };

        listener.SetMeasurementEventCallback<double>((_, value, tags, _) =>
        {
            var read = tags.ToArray().ToDictionary(tag => tag.Key, tag => tag.Value);
            measured.Add((value, (string)read["provider"]!, (int)read["batch"]!));
        });

        listener.Start();

        using var detector = Detector(threshold: 0.5f);
        var frame = Decode(Dog);

        detector.Detect(frame);
        detector.Detect([frame, frame]);

        Assert.Collection(
            measured,
            one =>
            {
                Assert.Equal(detector.Provider, one.Provider);
                Assert.Equal(1, one.Batch);
                Assert.True(one.Ms > 0, "a detection that took no time at all");
            },
            two => Assert.Equal(2, two.Batch));
    }

    /// <summary>
    /// Where a wrong table would ship: the README's sparse id, not a position in an 80-name list.
    /// </summary>
    [Fact]
    public void Class_18_is_the_dog()
    {
        Assert.Equal("dog", DetectorDescriptor.RfDetrNano.Classes[18]);
        Assert.False(DetectorDescriptor.RfDetrNano.Classes.ContainsKey(0), "slot 0 is background and must not be a class");
    }

    [Fact]
    public void Explicit_processor_selection_is_deterministic()
    {
        Assert.SkipUnless(File.Exists(Model), NoModel);

        using var detector = Detector(threshold: 0.5f);

        Assert.Equal("CPU", detector.Provider);
        Assert.Contains(Log, line => line.Contains("on the CPU execution provider", StringComparison.Ordinal));
    }

    private OnnxDetector Detector(float threshold)
        => new(Model, DetectorDescriptor.RfDetrNano, threshold, new ListLogger(Log), "cpu");
}
