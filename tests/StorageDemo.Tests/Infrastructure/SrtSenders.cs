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

    /// <summary>A video-only transport stream of the synthetic picture, at the settings <see cref="StartSender"/> sends.</summary>
    /// <param name="image">
    /// A still to show for the whole duration instead of the synthetic picture, at its own size
    /// and near-lossless, so a detector sees the picture the file holds rather than the codec's
    /// idea of it. This is how a known image becomes a stream.
    /// </param>
    public static void Render(string path, int seconds, string? image = null)
    {
        string[] arguments =
        [
            "-hide_banner", "-loglevel", "error", "-y",
            .. image is null
                ? (string[])["-f", "lavfi", "-i", "testsrc=size=320x240:rate=15", "-c:v", "mpeg2video", "-b:v", "800k"]
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
