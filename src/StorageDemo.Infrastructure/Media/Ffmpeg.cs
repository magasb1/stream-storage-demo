using System.Runtime.InteropServices;

namespace StorageDemo.Infrastructure.Media;

/// <summary>The FFmpeg that ships with the application.</summary>
public static class Ffmpeg
{
    /// <summary>Directory holding the native libraries and CLI tools for this platform.</summary>
    public static string Directory { get; private set; } = Path.Combine(
        AppContext.BaseDirectory,
        "ffmpeg",
        RuntimeIdentifier());

    /// <summary>Must be called before anything loads the libraries.</summary>
    public static void UseDirectory(string directory)
    {
        if (!System.IO.Directory.Exists(directory))
        {
            throw new DirectoryNotFoundException($"No FFmpeg libraries at '{directory}'.");
        }

        // avcodec-62.dll on Windows, libavcodec.so.62 elsewhere.
        var found = System.IO.Directory
            .EnumerateFiles(directory)
            .Select(Path.GetFileName)
            .Any(name => name is not null
                && name.Contains("avcodec", StringComparison.OrdinalIgnoreCase)
                && (name.EndsWith(".dll", StringComparison.OrdinalIgnoreCase)
                    || name.Contains(".so", StringComparison.OrdinalIgnoreCase)
                    || name.EndsWith(".dylib", StringComparison.OrdinalIgnoreCase)));

        if (!found)
        {
            throw new FileNotFoundException(
                $"'{directory}' holds no FFmpeg libraries. This has to be a shared build: a static "
                + "build ships only ffmpeg and ffprobe executables, which cannot be loaded in "
                + "process. Look for a build whose name says 'shared'.");
        }

        Directory = directory;
    }

    public static string ExecutablePath { get; } = Executable("ffmpeg");

    public static string ProbePath { get; } = Executable("ffprobe");

    public static bool IsPresent => File.Exists(ExecutablePath) && File.Exists(ProbePath);

    private static string Executable(string name)
        => Path.Combine(Directory, OperatingSystem.IsWindows() ? $"{name}.exe" : name);

    /// <summary>Matches the folder names in the package.</summary>
    private static string RuntimeIdentifier()
    {
        var architecture = RuntimeInformation.ProcessArchitecture == Architecture.Arm64
            ? "arm64"
            : "x64";

        if (OperatingSystem.IsWindows())
        {
            return "win-x64";
        }

        if (OperatingSystem.IsMacOS())
        {
            return $"osx-{architecture}";
        }

        // Alpine and friends need the musl build; glibc binaries simply will not start there.
        return RuntimeInformation.RuntimeIdentifier.Contains("musl", StringComparison.OrdinalIgnoreCase)
            ? "linux-musl-x64"
            : $"linux-{architecture}";
    }
}
