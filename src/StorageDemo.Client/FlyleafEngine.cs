using System.IO;
using FlyleafLib;
using FlyleafLib.MediaPlayer;

namespace StorageDemo.Client;

/// <summary>Starts FlyleafLib once, pointed at the libav libraries that ship with this application.</summary>
public static class FlyleafEngine
{
    private static readonly Lock Gate = new();

    private static bool _started;

    /// <summary>Where the runtime package puts the native libraries for this platform.</summary>
    public static string FFmpegDirectory { get; } =
        Path.Combine(AppContext.BaseDirectory, "runtimes", "win-x64", "native");

    /// <summary>Throws if the engine cannot start, so the caller can fall back to a still image.</summary>
    public static void EnsureStarted()
    {
        lock (Gate)
        {
            if (_started)
            {
                return;
            }

            Engine.Start(new EngineConfig
            {
                FFmpegPath = FFmpegDirectory,
                LogLevel = LogLevel.Quiet,

                // Lets the player raise property changes for CurTime and friends on the UI thread.
                UIRefresh = true,
                UIRefreshInterval = 200,
            });

            _started = true;
        }
    }

    /// <summary>Points the player at a live stream or at a stored file, which want opposite things.</summary>
    public static void TuneFor(Player player, bool live, string? streamId = null)
    {
        var demuxer = player.Config.Demuxer;

        if (live)
        {
            demuxer.FormatOpt["analyzeduration"] = LiveProbeMicroseconds.ToString();
            demuxer.FormatOpt["probesize"] = LiveProbeBytes.ToString();

            // Hand packets on rather than holding them: this is a camera, and there is nothing to
            // be gained by being a second behind it.
            demuxer.FormatOpt["fflags"] = "nobuffer";

            if (streamId is { Length: > 0 })
            {
                demuxer.FormatOpt["streamid"] = streamId;
            }
            else
            {
                demuxer.FormatOpt.Remove("streamid");
            }

            player.Config.Decoder.LowDelay = true;

            return;
        }

        demuxer.FormatOpt.Remove("analyzeduration");
        demuxer.FormatOpt.Remove("probesize");
        demuxer.FormatOpt.Remove("fflags");
        demuxer.FormatOpt.Remove("streamid");

        player.Config.Decoder.LowDelay = false;
    }

    /// <summary>One second.</summary>
    private const long LiveProbeMicroseconds = 1_000_000;

    /// <summary>One megabyte, the other half of the same limit, which binds on a high bitrate feed.</summary>
    private const long LiveProbeBytes = 1024 * 1024;
}
