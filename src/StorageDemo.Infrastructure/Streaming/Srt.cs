using System.Reflection;
using System.Runtime.InteropServices;
using System.Text;
using StorageDemo.Infrastructure.Media;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>libsrt's C API, only the parts this service uses.</summary>
public static unsafe partial class Srt
{
    private const string Library = "srt";

    /// <summary>1.5.0, as <c>srt_getversion</c> encodes it: patch + minor*0x100 + major*0x10000.</summary>
    private const uint MinimumVersion = 0x01_05_00;

    private static readonly Lock Gate = new();

    private static readonly Lazy<bool> Available = new(Probe);

    private static IntPtr _handle;

    private static bool _started;

    static Srt()
    {
        // One resolver per assembly and a second registration throws.
        NativeLibrary.SetDllImportResolver(typeof(Srt).Assembly, Resolve);
    }

    /// <summary>
    /// True when libsrt loaded and is new enough for the listen callback and the extended rejection
    /// codes.
    /// </summary>
    public static bool IsAvailable => Available.Value;

    /// <summary>Starts libsrt once per process.</summary>
    public static void EnsureStarted()
    {
        lock (Gate)
        {
            if (_started)
            {
                return;
            }

            if (srt_startup() < 0)
            {
                throw new InvalidOperationException($"srt_startup failed: {LastError()}");
            }

            _started = true;
        }
    }

    private static bool Probe()
    {
        try
        {
            // Started rather than merely loaded, so "available" means "usable": a caller that saw
            // true may create a socket without a second ceremony.
            EnsureStarted();

            return srt_getversion() >= MinimumVersion;
        }
        catch (Exception)
        {
            return false;
        }
    }

    private static IntPtr Resolve(string libraryName, Assembly assembly, DllImportSearchPath? searchPath)
    {
        if (libraryName != Library)
        {
            return IntPtr.Zero;
        }

        if (_handle != IntPtr.Zero)
        {
            return _handle;
        }

        // The soname, not the linker name: the packages ship libsrt.so.1.5 and no bare libsrt.so
        // unless the -dev package is installed, which a container has no reason to carry.
        var fileName = OperatingSystem.IsWindows() ? "srt.dll" : "libsrt.so.1.5";

        // Beside the libav libraries first, which is where scripts/fetch-libsrt.sh puts it.
        if (NativeLibrary.TryLoad(Path.Combine(Ffmpeg.Directory, fileName), out var bundled))
        {
            return _handle = bundled;
        }

        return NativeLibrary.TryLoad(fileName, assembly, searchPath, out var installed)
            ? _handle = installed
            : IntPtr.Zero;
    }

    /// <summary>The last error on this thread, as a line worth logging.</summary>
    public static string LastError()
    {
        try
        {
            var code = srt_getlasterror(null);
            var text = Marshal.PtrToStringUTF8((IntPtr)srt_getlasterror_str());

            return text is null ? $"SRT error {code}" : $"{text} ({code})";
        }
        catch (Exception)
        {
            return "libsrt is not loaded";
        }
    }

    /// <summary>The last error code on this thread, or zero when libsrt is not loaded.</summary>
    public static int LastErrorCode()
    {
        try
        {
            return srt_getlasterror(null);
        }
        catch (Exception)
        {
            return 0;
        }
    }

    public static bool SetInt32(int socket, SRT_SOCKOPT option, int value)
        => srt_setsockflag(socket, option, &value, sizeof(int)) == 0;

    /// <summary>The option's value, or -1 if it could not be read.</summary>
    public static int GetInt32(int socket, SRT_SOCKOPT option)
    {
        var value = 0;
        var length = sizeof(int);

        return srt_getsockflag(socket, option, &value, &length) == 0 ? value : -1;
    }

    /// <summary>
    /// libsrt takes a bool option as either a one-byte bool or a four-byte int, so the int is what
    /// goes over, being the one .NET can hand it without guessing at sizeof(bool).
    /// </summary>
    public static bool SetBool(int socket, SRT_SOCKOPT option, bool value)
        => SetInt32(socket, option, value ? 1 : 0);

    public static bool SetString(int socket, SRT_SOCKOPT option, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);

        fixed (byte* data = bytes)
        {
            // Length rather than a terminator: libsrt copies exactly optlen bytes.
            return srt_setsockflag(socket, option, data, bytes.Length) == 0;
        }
    }

    /// <summary>The option's value, or null if it could not be read.</summary>
    public static string? GetString(int socket, SRT_SOCKOPT option, int maxLength = StreamIdMaxLength)
    {
        var buffer = stackalloc byte[maxLength + 1];
        var length = maxLength + 1;

        if (srt_getsockflag(socket, option, buffer, &length) != 0)
        {
            return null;
        }

        return Encoding.UTF8.GetString(buffer, Math.Max(0, Math.Min(length, maxLength)));
    }

    /// <summary>SRTO_STREAMID's ceiling, and the size every read of it is given.</summary>
    public const int StreamIdMaxLength = 512;

    /// <summary>The default live payload, seven transport-stream packets.</summary>
    public const int LiveDefaultPayloadSize = 1316;

    /// <summary>MTU 1500 less the UDP and SRT headers; a live message larger than this is refused.</summary>
    public const int LiveMaxPayloadSize = 1456;

    public const int SRT_INVALID_SOCK = -1;

    public const int SRT_ERROR = -1;

    public const int SRT_ECONNREJ = 1002;

    public const int SRT_ECONNLOST = 2001;

    public const int SRT_EASYNCRCV = 6002;

    public const int SRT_ETIMEOUT = 6003;

    /// <summary>The rejection codes this service sets from the listen callback.</summary>
    public const int SRT_REJX_BAD_REQUEST = 1400;

    public const int SRT_REJX_OVERLOAD = 1402;

    public const int SRT_REJX_CONFLICT = 1409;

    [LibraryImport(Library)]
    public static partial int srt_startup();

    [LibraryImport(Library)]
    public static partial int srt_cleanup();

    [LibraryImport(Library)]
    public static partial uint srt_getversion();

    [LibraryImport(Library)]
    public static partial int srt_create_socket();

    [LibraryImport(Library)]
    public static partial int srt_bind(int u, void* name, int namelen);

    [LibraryImport(Library)]
    public static partial int srt_listen(int u, int backlog);

    [LibraryImport(Library)]
    public static partial int srt_accept(int u, void* addr, int* addrlen);

    /// <summary>
    /// The caller half of a handshake, for the one thing this service dials out to rather than
    /// accepts: a forward whose target is an SRT caller.
    /// </summary>
    [LibraryImport(Library)]
    public static partial int srt_connect(int u, void* name, int namelen);

    [LibraryImport(Library)]
    public static partial int srt_close(int u);

    /// <summary>Installs the handshake hook.</summary>
    [LibraryImport(Library)]
    public static partial int srt_listen_callback(
        int lsn,
        delegate* unmanaged[Cdecl]<void*, int, int, void*, byte*, int> hook,
        void* opaque);

    /// <summary>Only accepts values of 1000 or more; anything less is SRT_EINVPARAM.</summary>
    [LibraryImport(Library)]
    public static partial int srt_setrejectreason(int u, int value);

    [LibraryImport(Library)]
    public static partial int srt_setsockflag(int u, SRT_SOCKOPT opt, void* optval, int optlen);

    [LibraryImport(Library)]
    public static partial int srt_getsockflag(int u, SRT_SOCKOPT opt, void* optval, int* optlen);

    [LibraryImport(Library)]
    public static partial int srt_recvmsg(int u, byte* buf, int len);

    [LibraryImport(Library)]
    public static partial int srt_sendmsg(int u, byte* buf, int len, int ttl, int inorder);

    /// <summary>errno_loc may be null; it receives the platform error, which we never want.</summary>
    [LibraryImport(Library)]
    public static partial int srt_getlasterror(int* errno_loc);

    [LibraryImport(Library)]
    public static partial byte* srt_getlasterror_str();

    /// <summary>Still void*, because the destination is deliberately larger than the struct.</summary>
    [LibraryImport(Library)]
    public static partial int srt_bstats(int u, void* perf, int clear);

    /// <summary>
    /// What libsrt says about one connection, or false when it will not answer - a socket that has
    /// already gone, which is a normal thing for a caller to ask about and not an error.
    /// </summary>
    public static bool Stats(int socket, out SRT_TRACEBSTATS stats, bool clear)
    {
        // srt_bstats writes however many bytes the struct has in the libsrt that is actually
        // loaded, and takes no length to bound it.
        Span<byte> destination = stackalloc byte[sizeof(SRT_TRACEBSTATS) * 2];

        destination.Clear();

        fixed (byte* raw = destination)
        {
            if (srt_bstats(socket, raw, clear ? 1 : 0) != 0)
            {
                stats = default;

                return false;
            }

            stats = *(SRT_TRACEBSTATS*)raw;

            return true;
        }
    }
}

/// <summary>libsrt's <c>CBytePerfMon</c>, which <c>srt_bstats</c> fills in.</summary>
[StructLayout(LayoutKind.Sequential)]
public struct SRT_TRACEBSTATS
{
    public long msTimeStamp;
    public long pktSentTotal;
    public long pktRecvTotal;
    public int pktSndLossTotal;
    public int pktRcvLossTotal;
    public int pktRetransTotal;
    public int pktSentACKTotal;
    public int pktRecvACKTotal;
    public int pktSentNAKTotal;
    public int pktRecvNAKTotal;
    public long usSndDurationTotal;
    public int pktSndDropTotal;
    public int pktRcvDropTotal;
    public int pktRcvUndecryptTotal;
    public ulong byteSentTotal;
    public ulong byteRecvTotal;
    public ulong byteRcvLossTotal;
    public ulong byteRetransTotal;
    public ulong byteSndDropTotal;
    public ulong byteRcvDropTotal;
    public ulong byteRcvUndecryptTotal;
    public long pktSent;
    public long pktRecv;
    public int pktSndLoss;

    /// <summary>Packets this receiver never got and could not have retransmitted in time.</summary>
    public int pktRcvLoss;

    public int pktRetrans;
    public int pktRcvRetrans;
    public int pktSentACK;
    public int pktRecvACK;
    public int pktSentNAK;
    public int pktRecvNAK;
    public double mbpsSendRate;
    public double mbpsRecvRate;
    public long usSndDuration;
    public int pktReorderDistance;
    public double pktRcvAvgBelatedTime;
    public long pktRcvBelated;
    public int pktSndDrop;

    /// <summary>Packets that did arrive, too late for the latency window to play them.</summary>
    public int pktRcvDrop;

    public int pktRcvUndecrypt;
    public ulong byteSent;
    public ulong byteRecv;
    public ulong byteRcvLoss;
    public ulong byteRetrans;
    public ulong byteSndDrop;
    public ulong byteRcvDrop;
    public ulong byteRcvUndecrypt;
    public double usPktSndPeriod;
    public int pktFlowWindow;
    public int pktCongestionWindow;
    public int pktFlightSize;
    public double msRTT;
    public double mbpsBandwidth;
    public int byteAvailSndBuf;
    public int byteAvailRcvBuf;
    public double mbpsMaxBW;
    public int byteMSS;
    public int pktSndBuf;
    public int byteSndBuf;
    public int msSndBuf;
    public int msSndTsbPdDelay;
    public int pktRcvBuf;
    public int byteRcvBuf;
    public int msRcvBuf;
    public int msRcvTsbPdDelay;
    public int pktSndFilterExtraTotal;
    public int pktRcvFilterExtraTotal;
    public int pktRcvFilterSupplyTotal;
    public int pktRcvFilterLossTotal;
    public int pktSndFilterExtra;
    public int pktRcvFilterExtra;
    public int pktRcvFilterSupply;
    public int pktRcvFilterLoss;
    public int pktReorderTolerance;
    public long pktSentUniqueTotal;
    public long pktRecvUniqueTotal;
    public ulong byteSentUniqueTotal;
    public ulong byteRecvUniqueTotal;
    public long pktSentUnique;
    public long pktRecvUnique;
    public ulong byteSentUnique;
    public ulong byteRecvUnique;
}

/// <summary>Values from libsrt's own SRT_SOCKOPT enum.</summary>
public enum SRT_SOCKOPT
{
    SRTO_SNDSYN = 1,
    SRTO_RCVSYN = 2,
    SRTO_SNDTIMEO = 13,
    SRTO_RCVTIMEO = 14,
    SRTO_REUSEADDR = 15,
    SRTO_MAXBW = 16,

    /// <summary>
    /// Sets SRTO_RCVLATENCY and SRTO_PEERLATENCY together, which is why it is the one to set.
    /// </summary>
    SRTO_LATENCY = 23,
    SRTO_PASSPHRASE = 26,
    SRTO_RCVLATENCY = 43,
    SRTO_PEERLATENCY = 44,

    /// <summary>The one option an accepted socket does not inherit from its listener.</summary>
    SRTO_STREAMID = 46,
    SRTO_PAYLOADSIZE = 49,
    SRTO_ENFORCEDENCRYPTION = 53,
    SRTO_PEERIDLETIMEO = 55,
}
