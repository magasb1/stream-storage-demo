using System.Diagnostics;
using StorageDemo.Infrastructure.Media;

namespace StorageDemo.Tests.Infrastructure;

/// <summary>
/// Starts the bundled <c>ffmpeg</c> or <c>ffprobe</c>, and is the only place in the suite that does.
///
/// The environment is the whole reason it exists. The fetched Linux build carries an rpath of
/// <c>$ORIGIN/../lib</c> while the overlay lays the binaries and their shared objects out flat in
/// one folder, so a launch that says nothing about the library path dies before it reaches main with
/// "error while loading shared libraries: libavdevice.so.62". The test then fails as if the code
/// under test were wrong, which is the expensive part: the machine it happens on usually has some
/// FFmpeg registered with ldconfig for the in-process libraries, so the SRT tests in the same run
/// keep passing and only these few fail.
///
/// Pointing <c>LD_LIBRARY_PATH</c> at the folder the binary sits in is the whole fix, and the reason
/// it lives here rather than at each call site is that it was already copied five times and two
/// launches were still without it.
///
/// Windows needs none of this: its loader looks beside the executable first.
/// </summary>
internal static class BundledFfmpeg
{
    /// <param name="readOutput">
    /// Redirect standard output too, for a caller that reads what ffprobe printed. Off by default,
    /// because a redirected pipe nobody drains fills up and stops the process writing into it.
    /// </param>
    public static Process Start(string executable, string[] arguments, bool readOutput = false)
    {
        var startInfo = new ProcessStartInfo(executable)
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

        return Process.Start(startInfo)!;
    }
}
