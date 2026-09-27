using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text.RegularExpressions;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>
/// Real <c>ffmpeg</c> processes as SRT callers, shared by every test that needs one.
///
/// A listener can only be tested against something that actually speaks the handshake, and the
/// fetched FFmpeg has SRT as a caller, so it is the sender. Nothing here knows what is being tested;
/// it knows how to start a caller, how to tell whether one was turned away, and how to say what the
/// callers complained about when a test fails.
/// </summary>
internal static class SrtSenders
{
    /// <summary>
    /// FFmpeg's own line when a connection could not be opened, from <c>libsrt.c</c>. It is all a
    /// test can lean on: FFmpeg's caller path never asks libsrt for the rejection reason, so a
    /// refusal and a dead port read the same on stderr. Kept here so an FFmpeg that rewords it
    /// breaks one place rather than three tests.
    /// </summary>
    private const string RefusalPrefix = "Connection to srt://";

    /// <summary>An encoder pushing into a listening port. Null presents no identifier at all.</summary>
    /// <param name="callerOptions">
    /// Further SRT options for the caller, written as they would be in the URL, for a test about
    /// what the two ends negotiate. FFmpeg's time options are microseconds.
    /// </param>
    /// <param name="file">
    /// A transport stream to push as it is, every stream in it, instead of a synthetic picture.
    /// This is how a stream carrying something the command line cannot synthesise, such as KLV,
    /// reaches the service.
    /// </param>
    public static Process StartSender(int port, string? streamId, string? callerOptions = null, string? file = null)
        => Start(
        [
            "-hide_banner", "-loglevel", "error",
            // Paced at wall-clock speed: SRT is a connection, and a burst that ends at once looks
            // to the far end like a peer hanging up mid-handshake.
            "-re",
            .. file is null
                ? (string[])["-f", "lavfi", "-i", "testsrc=size=320x240:rate=15", "-c:v", "mpeg2video", "-b:v", "600k", "-g", "15"]
                : ["-i", file, "-map", "0", "-c", "copy"],
            "-f", "mpegts", Target(port, streamId, callerOptions),
        ]);

    /// <summary>
    /// One caller per name, all out of one process: the same file read once and muxed to each.
    ///
    /// It exists for the load rig, where a process per stream is what runs out first. Two hundred
    /// senders is ten gigabytes of resident ffmpeg and several cores of process overhead before the
    /// service has done anything; twenty processes pushing ten streams each is the same bytes on the
    /// wire for a twentieth of that. A multi-channel encoder is also a real thing, which is what
    /// makes it a fair rig rather than a trick.
    ///
    /// The cost is shared fate: one output blocking holds up its siblings, since ffmpeg writes them
    /// from one thread. That is why the rig reports its own processor share beside the service's -
    /// senders that fell behind and a service at its knee both show as less media arriving, and only
    /// the kernel's drop counter and the transport's loss figures tell them apart.
    /// </summary>
    public static Process StartMultiSender(int port, IReadOnlyList<string> names, string file)
        => Start(
        [
            "-hide_banner", "-loglevel", "error",
            "-re", "-i", file,
            .. names.SelectMany(name => (string[])
            [
                "-map", "0", "-c", "copy",
                "-f", "mpegts", Target(port, $"#!::r={name},m=publish", null),
            ]),
        ]);

    /// <summary>
    /// Viewers that never decode: one connection per name, all in one process, each discarded.
    ///
    /// A player that decodes costs more than the service does per stream, so a rig built out of them
    /// measures the rig. This is what a viewer costs the service - a socket, a subscription and a
    /// muxer - with nothing on this side but a read.
    /// </summary>
    public static Process StartCopyPlayers(int port, IReadOnlyList<string> names)
        => Start(
        [
            "-hide_banner", "-loglevel", "error",
            .. names.SelectMany(name => (string[])
                ["-i", Target(port, $"#!::r={name},m=request", null)]),

            // One output per input rather than every input mapped into one: ffmpeg interleaves the
            // streams of a single output by timestamp, so one slow viewer would hold up the rest of
            // them inside this process and the rig would be measuring itself.
            .. names.Select((_, index) => index).SelectMany(index => (string[])
            [
                "-map", index.ToString(CultureInfo.InvariantCulture), "-c", "copy",
                "-f", "mpegts", OperatingSystem.IsWindows() ? "NUL" : "/dev/null",
            ]),
        ]);

    /// <summary>A video-only transport stream of the synthetic picture, at the settings <see cref="StartSender"/> sends.</summary>
    /// <param name="image">
    /// A still to show for the whole duration instead of the synthetic picture, at its own size
    /// and near-lossless, so a detector sees the picture the file holds rather than the codec's
    /// idea of it. This is how a known image becomes a stream.
    /// </param>
    /// <param name="picture">
    /// The lavfi source to encode, for a test that needs a particular bitrate rather than the
    /// smallest thing that is a video. The default is what every sender here sends; the load test
    /// asks for the busier, larger pattern <c>scripts/load-senders.sh</c> uses, so its figures can
    /// be read beside the ones measured with that script.
    /// </param>
    /// <param name="bitrate">
    /// What to aim the encoder at. It is a ceiling rather than a promise - a synthetic pattern that
    /// compresses well will undershoot it, which is why the rigs measure what the file actually holds
    /// instead of trusting this. Raising it with a larger picture is how a camera-rate stream is made:
    /// the scale rig's second question is packet rate, not stream count.
    /// </param>
    public static void Render(
        string path,
        int seconds,
        string? image = null,
        string? picture = null,
        string? bitrate = null)
    {
        string[] arguments =
        [
            "-hide_banner", "-loglevel", "error", "-y",
            .. image is null
                ? (string[])
                [
                    "-f", "lavfi", "-i", picture ?? "testsrc=size=320x240:rate=15",
                    "-c:v", "mpeg2video", "-b:v", bitrate ?? "800k",
                ]
                : ["-loop", "1", "-framerate", "15", "-i", image, "-c:v", "mpeg2video", "-q:v", "2", "-pix_fmt", "yuv420p"],
            "-g", "15",
            "-t", seconds.ToString(CultureInfo.InvariantCulture),
            "-f", "mpegts", path,
        ];

        using var process = BundledFfmpeg.Start(BundledFfmpeg.Tool.Ffmpeg, arguments);

        // The parameterless overload, which also waits for the drain, so a failure message has
        // whatever ffmpeg said in it rather than the first half of it.
        process.WaitForExit();

        Assert.True(
            process.ExitCode == 0,
            $"ffmpeg could not render the video: {BundledFfmpeg.Complaints(process)}");
    }

    /// <summary>
    /// A caller that connects and then waits to be sent something. It is also the only way to get a
    /// connected SRT peer that sends no application bytes at all: an encoder always writes a header
    /// the moment the socket opens, and libsrt's caller side is not exposed to this test assembly.
    /// </summary>
    /// <inheritdoc cref="StartSender" path="/param[@name='callerOptions']"/>
    public static Process StartPlayer(int port, string streamId, string? callerOptions = null)
        => Start([
            "-hide_banner", "-loglevel", "error",
            "-i", Target(port, streamId, callerOptions),
            "-f", "null", "-",
        ]);

    /// <summary>
    /// A player that decodes what it is given and reports how far it has got.
    ///
    /// The progress goes through <c>-progress</c> rather than ffmpeg's own statistics line, which is
    /// tied to the log level and would have to be turned back on. This is the difference between
    /// proving a connection was made and proving media came down it.
    /// </summary>
    public static Process StartViewer(int port, string streamId)
        => Start([
            "-hide_banner", "-loglevel", "error", "-progress", "pipe:2",
            "-i", Target(port, streamId, null),
            "-f", "null", "-",
        ]);

    /// <summary>
    /// The far end for a listening SRT forward this test suite dials out to: mode=listener rather
    /// than <see cref="Target"/>'s caller, which is the shape a forward opened with
    /// <c>?mode=listener</c> in its own URL expects on the other side of the wire. Decodes and
    /// reports progress the same way <see cref="StartViewer"/> does, proving media arrived rather
    /// than only that a connection was accepted.
    /// </summary>
    public static Process StartListener(int port, string? streamId = null)
        => Start([
            "-hide_banner", "-loglevel", "error", "-progress", "pipe:2",
            "-i", $"srt://127.0.0.1:{port}?mode=listener"
                + (streamId is null ? string.Empty : $"&streamid={Escape(streamId)}"),
            "-f", "null", "-",
        ]);

    /// <summary>
    /// The rate of the packets inside a transport stream, as the bundled ffprobe adds them up.
    ///
    /// The duration is the caller's to supply rather than ffprobe's to report, because the caller is
    /// the one that rendered the file and knows what it asked for; passing a figure that does not
    /// match silently scales every comparison made against the result.
    ///
    /// It is the payload rather than the file's size because that is what a demultiplexer publishes
    /// and therefore what the service counts: transport headers, the program tables and any padding
    /// are not in the figure. Compared against the file instead, a replica delivering everything it
    /// was sent reads as one delivering four fifths of it. Nothing is decoded to get it.
    /// </summary>
    public static double PayloadMbps(string path, int seconds)
    {
        string[] arguments = ["-v", "error", "-show_entries", "packet=size", "-of", "csv=p=0", path];

        // Through the same helper as every other launch here, so the library path is set in one
        // place: this is the seventh site, and it arrived with the load rigs after the other six had
        // already been consolidated. A launcher of its own would have quietly made BundledFfmpeg's
        // "only place that starts the bundled binary" untrue while merging without a conflict.
        using var probe = BundledFfmpeg.Start(BundledFfmpeg.Tool.Ffprobe, arguments, readOutput: true);

        // Standard output is read here while the helper drains standard error on another thread. One
        // pipe read to completion before the other is started deadlocks the moment the unread one
        // fills, and a packet listing for a long file is easily large enough to make that real.
        var sizes = probe.StandardOutput.ReadToEnd();

        probe.WaitForExit();

        Assert.True(
            probe.ExitCode == 0,
            $"ffprobe could not read '{path}': {BundledFfmpeg.Complaints(probe)}");

        // One line per packet, and the size is its first field: ffprobe's CSV writer still emits the
        // separator for the side-data column it was not asked about, so "8058," is a whole line.
        var bytes = sizes
            .Split('\n', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Sum(line => long.TryParse(line.Split(',')[0], CultureInfo.InvariantCulture, out var packet)
                ? packet
                : 0);

        Assert.True(bytes > 0, $"'{path}' holds no packets");

        return bytes * 8 / (double)seconds / 1_000_000;
    }

    /// <summary>Frames a <see cref="StartViewer"/> player has decoded so far.</summary>
    public static int Decoded(Process player)
        => Regex.Matches(Said(player), "^frame=([0-9]+)", RegexOptions.Multiline)
            .Select(match => int.Parse(match.Groups[1].Value, CultureInfo.InvariantCulture))
            .DefaultIfEmpty(0)
            .Max();

    /// <summary>
    /// Whether this caller was turned away rather than served: it gave up quickly and said so.
    ///
    /// Corroboration only. A caller that was accepted and then dropped complains in exactly the same
    /// words, so whoever calls this must also assert that the accept handler never fired.
    /// </summary>
    public static async Task<bool> WasRefused(Process caller, TimeSpan? within = null)
    {
        using var deadline = new CancellationTokenSource(within ?? TimeSpan.FromSeconds(2));

        try
        {
            await caller.WaitForExitAsync(deadline.Token);
        }
        catch (OperationCanceledException)
        {
            // Still running, so it got in. A rejection is immediate; there is nothing to wait for.
            return false;
        }

        // The exit code says nothing useful and the last stderr line may still be in flight: the
        // pipe is drained on another thread, and only the parameterless overload waits for it.
        caller.WaitForExit();

        var said = Said(caller);

        return said.Contains(RefusalPrefix, StringComparison.Ordinal)
            && said.Contains("failed", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Everything the callers complained about, for a failure message worth reading.</summary>
    public static string Complaints(IEnumerable<Process> callers)
    {
        var said = callers
            .Select(Said)
            .Where(text => text.Length > 0)
            .ToArray();

        return said.Length == 0 ? "the callers said nothing" : string.Join(" | ", said);
    }

    public static void Kill(Process caller)
    {
        try
        {
            if (!caller.HasExited)
            {
                caller.Kill(entireProcessTree: true);
            }
        }
        catch (InvalidOperationException)
        {
        }

        caller.Dispose();
    }

    /// <param name="describe">
    /// What to say when it never came true. Worth passing: "the condition was false" sends the
    /// next reader back to the source to work out which half of it failed.
    /// </param>
    public static async Task WaitUntilAsync(
        Func<bool> condition,
        TimeSpan timeout,
        Func<string>? describe = null)
    {
        var deadline = DateTime.UtcNow + timeout;

        while (DateTime.UtcNow < deadline)
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(100));
        }

        Assert.Fail($"{describe?.Invoke() ?? "the condition was still false"} after {timeout}");
    }

    /// <summary>
    /// A free port from a low, fixed range rather than an ephemeral one. Windows reserves stretches
    /// of the dynamic range, so a port can be handed out and then refuse an explicit bind moments
    /// later, which looks exactly like a listener that will not start.
    /// </summary>
    private static int _nextPort = 9400;

    public static int FreePort()
    {
        var start = Interlocked.Add(ref _nextPort, 10);

        for (var port = start; port < start + 200; port++)
        {
            try
            {
                using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Dgram, ProtocolType.Udp);

                socket.Bind(new IPEndPoint(IPAddress.Any, port));

                return port;
            }
            catch (SocketException)
            {
            }
        }

        throw new InvalidOperationException("No free port in the test range.");
    }

    private static string Target(int port, string? streamId, string? callerOptions)
        => $"srt://127.0.0.1:{port}?mode=caller"
            + (streamId is null ? string.Empty : $"&streamid={Escape(streamId)}")
            + (callerOptions is null ? string.Empty : $"&{callerOptions}");

    /// <summary>
    /// Only the hash, which is the one character of the Access Control envelope a URL would read as
    /// the start of a fragment. FFmpeg 7 and later percent-decode the identifier before the
    /// handshake; an older one passes <c>%23</c> straight through, and <c>StreamName</c> understands
    /// that form too, so escaping this way works either side of that change.
    /// </summary>
    private static string Escape(string streamId)
        => streamId.Replace("#", "%23", StringComparison.Ordinal);

    private static Process Start(string[] arguments)
        => BundledFfmpeg.Start(BundledFfmpeg.Tool.Ffmpeg, arguments);

    /// <summary>
    /// What this caller complained about. Drained from the moment it started, which is not a detail:
    /// a pipe nobody reads fills and stops the caller, and the test then fails as "nothing was
    /// accepted" with the reason sitting unread in the pipe.
    /// </summary>
    private static string Said(Process caller) => BundledFfmpeg.Complaints(caller);
}
