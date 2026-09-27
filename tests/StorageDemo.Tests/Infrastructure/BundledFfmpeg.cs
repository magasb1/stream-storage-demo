using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;
using StorageDemo.Infrastructure.Media;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>
/// Starts the bundled <c>ffmpeg</c> or <c>ffprobe</c>, and is the only place in the suite that does.
///
/// The library path is the first reason it exists. The fetched Linux build carries an rpath of
/// <c>$ORIGIN/../lib</c> while the overlay lays the binaries and their shared objects out flat in
/// one folder, so a launch that says nothing about the library path dies before it reaches main with
/// "error while loading shared libraries: libavdevice.so.62". The test then fails as if the code
/// under test were wrong, which is the expensive part: the machine it happens on usually has some
/// FFmpeg registered with ldconfig for the in-process libraries, so the SRT tests in the same run
/// keep passing and only these few fail. Windows needs none of it: its loader looks beside the
/// executable first.
///
/// Draining standard error is the second, and it is why every process started here is drained from
/// the moment it starts rather than when a caller remembers to ask. A redirected pipe nobody reads
/// fills up and stops the process writing into it, which for a sender held for the length of a test
/// looks like a stream that died of its own accord, with the explanation sitting unread in the pipe.
/// <see cref="Complaints"/> is how a test reads what was said, and it is worth putting in a failure
/// message: the loader error above arrives that way.
/// </summary>
internal static class BundledFfmpeg
{
    /// <summary>
    /// Which of the two bundled executables to start. Named rather than passed as a path so that a
    /// host's own ffprobe cannot be started against the bundled libraries, which is the mismatch
    /// this class exists to prevent.
    /// </summary>
    public enum Tool
    {
        Ffmpeg,
        Ffprobe,
    }

    /// <summary>
    /// Keyed on the process rather than on its id, which the operating system reuses: a buffer found
    /// by the id of a process that has exited would be handed to whatever started next, and this
    /// suite starts and kills hundreds of senders in a run.
    /// </summary>
    private static readonly ConditionalWeakTable<Process, StringBuilder> Complained = new();

    /// <param name="readOutput">
    /// Redirect standard output too, for a caller that reads what ffprobe printed. Off by default,
    /// and a caller that turns it on reads it: standard error is drained here, standard output is
    /// not, and neither may be redirected and then ignored.
    /// </param>
    public static Process Start(Tool tool, string[] arguments, bool readOutput = false)
    {
        var startInfo = new ProcessStartInfo(Executable(tool))
        {
            RedirectStandardOutput = readOutput,
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

        var process = Process.Start(startInfo)!;
        var complaints = new StringBuilder();

        Complained.Add(process, complaints);

        process.ErrorDataReceived += (_, line) =>
        {
            if (line.Data is not null)
            {
                lock (complaints)
                {
                    complaints.AppendLine(line.Data);
                }
            }
        };

        process.BeginErrorReadLine();

        return process;
    }

    /// <summary>
    /// Everything this process has complained about so far.
    ///
    /// A caller that has waited with <c>WaitForExitAsync</c> should wait again with the
    /// parameterless <c>WaitForExit</c> before reading: only that overload also waits for the last
    /// line to make its way out of the pipe.
    /// </summary>
    public static string Complaints(Process process)
    {
        if (!Complained.TryGetValue(process, out var complaints))
        {
            return string.Empty;
        }

        lock (complaints)
        {
            return complaints.ToString().Trim();
        }
    }

    private static string Executable(Tool tool)
        => tool == Tool.Ffmpeg ? Ffmpeg.ExecutablePath : Ffmpeg.ProbePath;
}
