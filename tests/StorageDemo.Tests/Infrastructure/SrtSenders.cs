using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.RegularExpressions;
using StorageDemo.Infrastructure.Media;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>Real <c>ffmpeg</c> processes as SRT callers, shared by every test that needs one.</summary>
internal static class SrtSenders
{
    /// <summary>FFmpeg's own line when a connection could not be opened, from <c>libsrt.c</c>.</summary>
    private const string RefusalPrefix = "Connection to srt://";

    private static readonly ConcurrentDictionary<int, StringBuilder> Stderr = new();

    /// <summary>An encoder pushing into a listening port.</summary>
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
    /// A video-only transport stream of the synthetic picture, at the settings <see
    /// cref="StartSender"/> sends.
    /// </summary>
    public static void Render(string path, int seconds, string? image = null)
    {
        var startInfo = new ProcessStartInfo(Ffmpeg.ExecutablePath)
        {
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };

        foreach (var argument in (string[])
                 [
                     "-hide_banner", "-loglevel", "error", "-y",
                     .. image is null
                         ? (string[])["-f", "lavfi", "-i", "testsrc=size=320x240:rate=15", "-c:v", "mpeg2video", "-b:v", "800k"]
                         : ["-loop", "1", "-framerate", "15", "-i", image, "-c:v", "mpeg2video", "-q:v", "2", "-pix_fmt", "yuv420p"],
                     "-g", "15",
                     "-t", seconds.ToString(CultureInfo.InvariantCulture),
                     "-f", "mpegts", path,
                 ])
        {
            startInfo.ArgumentList.Add(argument);
        }

        if (!OperatingSystem.IsWindows())
        {
            startInfo.Environment["LD_LIBRARY_PATH"] = Ffmpeg.Directory;
        }

        using var process = Process.Start(startInfo)!;
        var complaints = process.StandardError.ReadToEnd();
        process.WaitForExit();

        Assert.True(process.ExitCode == 0, $"ffmpeg could not render the video: {complaints}");
    }

    /// <summary>A caller that connects and then waits to be sent something.</summary>
    public static Process StartPlayer(int port, string streamId, string? callerOptions = null)
        => Start([
            "-hide_banner", "-loglevel", "error",
            "-i", Target(port, streamId, callerOptions),
            "-f", "null", "-",
        ]);

    /// <summary>A player that decodes what it is given and reports how far it has got.</summary>
    public static Process StartViewer(int port, string streamId)
        => Start([
            "-hide_banner", "-loglevel", "error", "-progress", "pipe:2",
            "-i", Target(port, streamId, null),
            "-f", "null", "-",
        ]);

    /// <summary>
    /// The far end for a listening SRT forward this test suite dials out to: mode=listener rather
    /// than <see cref="Target"/>'s caller, which is the shape a forward opened with
    /// <c>?mode=listener</c> in its own URL expects on the other side of the wire.
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
            // Still running, so it got in.
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

    /// <param name="describe">What to say when it never came true.</param>
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

    /// <summary>A free port from a low, fixed range rather than an ephemeral one.</summary>
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
    /// the start of a fragment.
    /// </summary>
    private static string Escape(string streamId)
        => streamId.Replace("#", "%23", StringComparison.Ordinal);

    private static Process Start(string[] arguments)
    {
        var startInfo = new ProcessStartInfo(Ffmpeg.ExecutablePath)
        {
            RedirectStandardError = true,
            UseShellExecute = false,
            CreateNoWindow = true,
        };

        foreach (var argument in arguments)
        {
            startInfo.ArgumentList.Add(argument);
        }

        if (!OperatingSystem.IsWindows())
        {
            startInfo.Environment["LD_LIBRARY_PATH"] = Ffmpeg.Directory;
        }

        var caller = Process.Start(startInfo)!;

        // Drained, not merely redirected.
        var complaints = new StringBuilder();
        Stderr[caller.Id] = complaints;

        caller.ErrorDataReceived += (_, line) =>
        {
            if (line.Data is not null)
            {
                lock (complaints)
                {
                    complaints.AppendLine(line.Data);
                }
            }
        };

        caller.BeginErrorReadLine();

        return caller;
    }

    private static string Said(Process caller)
    {
        if (!Stderr.TryGetValue(caller.Id, out var text))
        {
            return string.Empty;
        }

        lock (text)
        {
            return text.ToString().Trim();
        }
    }
}
