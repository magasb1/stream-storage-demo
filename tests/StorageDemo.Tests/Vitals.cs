using System.Diagnostics;
using System.Globalization;

namespace StorageDemo.Tests;

/// <summary>
/// What this process is spending, read straight out of <c>/proc</c>: processor time in total and per
/// thread by name, resident memory, thread count, and the kernel's UDP receive errors.
///
/// It exists because the interesting question at load is never "how fast is it" but "what ran out
/// first", and a meter cannot answer that. The per-thread breakdown is the whole point: this
/// service's measured ceiling is one libsrt receive worker per bound port
/// (<c>.scratch/scale-to-1000/baseline.md</c>), and that thread is a name in <c>/proc/self/task</c>
/// long before it is a number anywhere else. Aggregating by name rather than by thread id is what
/// makes ".NET TP Worker" one row of forty threads instead of forty rows.
///
/// Linux only, deliberately. Windows and macOS have no equivalent worth faking, and a rig that
/// reported a comforting zero for something it cannot see would be worse than one that says it has
/// nothing to say - the same reasoning <c>LiveMetrics.KernelUdpReceiveErrors</c> uses.
/// </summary>
internal sealed record Vitals(
    TimeSpan Cpu,
    IReadOnlyDictionary<string, TimeSpan> ByThread,
    long ResidentBytes,
    int Threads,
    TimeSpan Elsewhere,
    long? KernelUdpErrors)
{
    /// <summary>
    /// What <c>/proc</c> counts in, and the one number here that is not read from a file.
    /// <c>sysconf(_SC_CLK_TCK)</c> is 100 on every Linux this service is built for, and getting it
    /// honestly would mean a P/Invoke in a test helper to learn something that has not changed in
    /// twenty years.
    /// </summary>
    private const double TicksPerSecond = 100;

    public static bool Available => OperatingSystem.IsLinux();

    /// <param name="elsewhere">
    /// Other processes to add up separately - the load generator's own senders and players. Without
    /// this figure a run cannot tell a service at its knee from a rig that ran out of machine before
    /// the service did, which is the mistake the first baseline made and caught.
    /// </param>
    public static Vitals Read(IEnumerable<Process>? elsewhere = null)
    {
        if (!Available)
        {
            return new Vitals(TimeSpan.Zero, new Dictionary<string, TimeSpan>(), 0, 0, TimeSpan.Zero, null);
        }

        var threads = new Dictionary<string, TimeSpan>(StringComparer.Ordinal);

        // The whole process, including the time of threads that have already ended. Summing the live
        // threads instead makes a busy interval read as negative work whenever one of them finished
        // inside it, which at this load happens in most windows: a viewer leaving takes its thread.
        var total = Spent("/proc/self/stat") ?? TimeSpan.Zero;

        foreach (var task in Directory.EnumerateDirectories("/proc/self/task"))
        {
            if (Spent(Path.Combine(task, "stat")) is not { } cpu)
            {
                continue;
            }

            var name = Name(Path.Combine(task, "comm"));

            threads[name] = threads.GetValueOrDefault(name) + cpu;
        }

        var resident = 0L;
        var count = 0;

        foreach (var line in Lines("/proc/self/status"))
        {
            if (line.StartsWith("VmRSS:", StringComparison.Ordinal))
            {
                resident = Kilobytes(line) * 1024;
            }
            else if (line.StartsWith("Threads:", StringComparison.Ordinal))
            {
                count = (int)Kilobytes(line);
            }
        }

        var others = TimeSpan.Zero;

        foreach (var process in elsewhere ?? [])
        {
            try
            {
                if (!process.HasExited && Spent($"/proc/{process.Id}/stat") is { } cpu)
                {
                    others += cpu;
                }
            }
            catch (Exception)
            {
                // A sender that exited between the check and the read has nothing to contribute.
            }
        }

        return new Vitals(
            total,
            threads,
            resident,
            count,
            others,
            StorageDemo.Infrastructure.Streaming.LiveMetrics.KernelUdpReceiveErrors());
    }

    /// <summary>
    /// What changed between two readings, as shares of one core, which is the unit the ceiling is
    /// expressed in: a single thread cannot exceed 100 % however many cores the machine has.
    /// </summary>
    public Load Since(Vitals earlier, TimeSpan elapsed)
    {
        var seconds = Math.Max(elapsed.TotalSeconds, 0.001);

        var byThread = ByThread
            .Select(thread => (
                Thread: thread.Key,
                Share: (thread.Value - earlier.ByThread.GetValueOrDefault(thread.Key)).TotalSeconds / seconds))
            .Where(thread => thread.Share > 0.005)
            .OrderByDescending(thread => thread.Share)
            .ToArray();

        return new Load(
            (Cpu - earlier.Cpu).TotalSeconds / seconds,
            byThread,
            ResidentBytes,
            Threads,
            (Elsewhere - earlier.Elsewhere).TotalSeconds / seconds,
            KernelUdpErrors is { } now && earlier.KernelUdpErrors is { } before ? now - before : null);
    }

    /// <param name="Cores">Shares of one core: 2.5 is two and a half cores' worth of this process.</param>
    /// <param name="ByThread">Threads by name, busiest first, anything above half a percent of a core.</param>
    /// <param name="Rig">The same for the load generator's processes, so a run can say which side ran out.</param>
    internal sealed record Load(
        double Cores,
        IReadOnlyList<(string Thread, double Share)> ByThread,
        long ResidentBytes,
        int Threads,
        double Rig,
        long? KernelUdpErrors)
    {
        /// <summary>
        /// The busiest single thread whose name begins with this, which for a libsrt receive worker is
        /// the ceiling itself: one thread cannot exceed one core, and libsrt numbers them per bound
        /// port - "SRT:RcvQ:w2" is the ingest port's. The busiest rather than the sum, because two
        /// ports at half a core each are not one thread in trouble.
        /// </summary>
        public double Share(string prefix)
            => ByThread
                .Where(entry => entry.Thread.StartsWith(prefix, StringComparison.Ordinal))
                .Select(entry => entry.Share)
                .DefaultIfEmpty(0)
                .Max();

        public string Busiest(int take)
            => string.Join(", ", ByThread.Take(take).Select(entry => $"{entry.Thread} {entry.Share:P0}"));
    }

    private static TimeSpan? Spent(string stat)
    {
        try
        {
            var text = File.ReadAllText(stat);

            // The thread's name is in parentheses and may itself hold spaces and parentheses, so the
            // fields are counted from the last ')' rather than from the start of the line.
            var fields = text[(text.LastIndexOf(')') + 1)..]
                .Split(' ', StringSplitOptions.RemoveEmptyEntries);

            // Counting from "state", which is field 3 of the file: utime is 14 and stime is 15.
            return fields.Length > 12
                && long.TryParse(fields[11], CultureInfo.InvariantCulture, out var user)
                && long.TryParse(fields[12], CultureInfo.InvariantCulture, out var system)
                    ? TimeSpan.FromSeconds((user + system) / TicksPerSecond)
                    : null;
        }
        catch (Exception)
        {
            // A thread that ended between the listing and the read. Nothing to report and nothing
            // worth failing a measurement over.
            return null;
        }
    }

    private static string Name(string comm)
    {
        try
        {
            var name = File.ReadAllText(comm).Trim();

            // The pool's workers are one row rather than forty: they are numbered, and forty rows of
            // two percent hides the one row of eighty that matters.
            return name.StartsWith(".NET TP Worker", StringComparison.Ordinal) ? ".NET TP Worker" : name;
        }
        catch (Exception)
        {
            return "gone";
        }
    }

    private static long Kilobytes(string line)
        => long.TryParse(
            line.Split(':', 2)[1].Replace("kB", string.Empty, StringComparison.Ordinal).Trim(),
            CultureInfo.InvariantCulture,
            out var value)
                ? value
                : 0;

    private static IEnumerable<string> Lines(string path)
    {
        try
        {
            return File.ReadLines(path);
        }
        catch (Exception)
        {
            return [];
        }
    }
}
