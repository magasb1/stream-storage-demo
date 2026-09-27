using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;
using StorageDemo.Infrastructure.Media;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>
/// The two frame defects the fan-out design carries over: a poster frame seek that was absolute
/// rather than relative to where the media starts, and the absence of any "picture now" verb.
///
/// Both are exercised against media whose timestamps do not start at zero, because that is what a
/// recording cut from a rolling buffer looks like and it is the only case where the old code was
/// wrong. A clip starting at zero passes either way, which is why this went unnoticed.
/// </summary>
public sealed class MediaAnalyzerFrameTests : IDisposable
{
    private readonly string _directory = Path.Combine(
        Path.GetTempPath(),
        "storage-demo-frame-tests",
        Guid.NewGuid().ToString("N"));

    public MediaAnalyzerFrameTests() => Directory.CreateDirectory(_directory);

    private static LibavMediaAnalyzer Analyzer(double videoFrameSeconds = 3)
        => new(
            Options.Create(new MediaOptions { VideoFrameSeconds = videoFrameSeconds }),
            NullLogger<LibavMediaAnalyzer>.Instance);

    /// <summary>
    /// The counted testsrc pattern prints its frame number, so two pictures from the same clip
    /// are byte-identical only when they are the same moment. That is the whole assertion here.
    /// </summary>
    private string Clip(string name, double startSeconds, double duration = 10)
    {
        var path = Path.Combine(_directory, name);

        Run(
            BundledFfmpeg.Tool.Ffmpeg,
            [
                "-hide_banner", "-loglevel", "error",
                "-f", "lavfi", "-i", $"testsrc=size=320x240:duration={duration}:rate=10",
                "-c:v", "mpeg2video", "-b:v", "800k", "-g", "10",
                // Where the stream claims to begin. A rolling-buffer cut lands at a big number.
                "-output_ts_offset", startSeconds.ToString(System.Globalization.CultureInfo.InvariantCulture),
                "-muxdelay", "0", "-muxpreload", "0",
                "-f", "mpegts",
                "-y", path,
            ]);

        return path;
    }

    private static async Task<byte[]?> PosterAsync(string path, double videoFrameSeconds)
    {
        await using var content = File.OpenRead(path);

        return (await Analyzer(videoFrameSeconds).AnalyzeAsync(content, "clip.ts", "video/mp2t")).Thumbnail;
    }

    private static async Task<byte[]?> LatestAsync(string path)
    {
        await using var content = File.OpenRead(path);

        return await Analyzer().LatestFrameAsync(content, "clip.ts");
    }

    /// <summary>
    /// Defect one. The seek target is an offset into the media, so the same offset picks the same
    /// picture whether the media starts at zero or at an hour in.
    /// </summary>
    [Fact]
    public async Task The_poster_frame_is_the_same_moment_wherever_the_timestamps_start()
    {
        var atZero = await PosterAsync(Clip("at-zero.ts", 0), videoFrameSeconds: 5);
        var farIn = await PosterAsync(Clip("far-in.ts", 3600), videoFrameSeconds: 5);

        Assert.NotNull(atZero);
        Assert.NotNull(farIn);

        Assert.True(
            atZero.SequenceEqual(farIn),
            "the same offset into two identical clips produced different pictures, so the seek is "
            + "still absolute and any recording cut from the buffer gets its poster frame from "
            + "frame zero");
    }

    /// <summary>
    /// And it still steers. A test that only compared two clips would pass just as well if the
    /// seek had been removed altogether and both had returned frame zero.
    /// </summary>
    [Fact]
    public async Task The_poster_frame_moves_when_the_offset_does()
    {
        var path = Clip("steering.ts", 3600);

        var early = await PosterAsync(path, videoFrameSeconds: 1);
        var late = await PosterAsync(path, videoFrameSeconds: 8);

        Assert.NotNull(early);
        Assert.NotNull(late);
        Assert.False(early.SequenceEqual(late), "the seek did not move the picture at all");
    }

    /// <summary>
    /// Defect two. The picture now, which is a different question from the poster frame and the
    /// one a preview and a snapshot are actually asking.
    /// </summary>
    [Fact]
    public async Task The_latest_frame_is_the_end_of_the_media_not_the_poster_frame()
    {
        var path = Clip("latest.ts", 3600);

        var poster = await PosterAsync(path, videoFrameSeconds: 3);
        var latest = await LatestAsync(path);

        Assert.NotNull(latest);
        Assert.NotNull(poster);
        Assert.False(latest.SequenceEqual(poster), "the latest frame is the poster frame again");

        // Same clip, so the answer is stable; only the question differs.
        Assert.True((await LatestAsync(path))!.SequenceEqual(latest));
    }

    /// <summary>Full source resolution, because a snapshot is opened rather than tiled.</summary>
    [Fact]
    public async Task The_latest_frame_keeps_the_source_resolution()
    {
        var latest = await LatestAsync(Clip("full-size.ts", 0));

        Assert.NotNull(latest);
        Assert.Equal((320, 240), Measure(latest));
    }

    [Fact]
    public async Task Media_with_no_picture_in_it_has_no_latest_frame()
    {
        await using var content = new MemoryStream("not a transport stream"u8.ToArray());

        Assert.Null(await Analyzer().LatestFrameAsync(content, "broken.ts"));
    }

    private (int Width, int Height) Measure(byte[] image)
    {
        var path = Path.Combine(_directory, $"{Guid.NewGuid():N}.jpg");
        File.WriteAllBytes(path, image);

        var parts = Run(
            BundledFfmpeg.Tool.Ffprobe,
            [
                "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=width,height",
                "-of", "csv=p=0",
                path,
            ]).Trim().Split(',');

        return (int.Parse(parts[0]), int.Parse(parts[1]));
    }

    private static string Run(BundledFfmpeg.Tool tool, string[] arguments)
    {
        using var process = BundledFfmpeg.Start(tool, arguments, readOutput: true);
        var output = process.StandardOutput.ReadToEnd();
        process.WaitForExit();

        Assert.True(process.ExitCode == 0, $"{tool} failed: {BundledFfmpeg.Complaints(process)}");

        return output;
    }

    public void Dispose()
    {
        if (Directory.Exists(_directory))
        {
            try
            {
                Directory.Delete(_directory, recursive: true);
            }
            catch (IOException)
            {
            }
        }
    }
}
