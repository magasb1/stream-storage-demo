using System.Text;
using System.Text.RegularExpressions;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>Which way a caller means to move bytes, which the convention calls the mode.</summary>
public enum StreamIntent
{
    /// <summary>An encoder pushing into the ingest port.</summary>
    Publish,

    /// <summary>A player pulling from the consumption port.</summary>
    Subscribe,
}

/// <summary>Reads the stream's name out of the SRT stream identifier.</summary>
public static partial class StreamName
{
    /// <summary>The handshake extension caps here, in bytes of UTF-8 rather than characters.</summary>
    public const int MaxIdentifierBytes = 512;

    /// <summary>What may become a stream name, a registry key and part of a document name.</summary>
    [GeneratedRegex(@"^[A-Za-z0-9._/-]{1,128}$")]
    private static partial Regex Allowed { get; }

    /// <summary>The rollback position a viewer asked for, in seconds, or null for the live edge.</summary>
    public static double? Position(string? streamId)
    {
        if (streamId is null || !streamId.StartsWith("#!::", StringComparison.Ordinal))
        {
            return null;
        }

        return Pairs(streamId["#!::".Length..]).TryGetValue("user_from", out var value)
            && double.TryParse(value, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var seconds)
            && seconds > 0
                ? seconds
                : null;
    }

    /// <param name="streamId">The raw identifier as it came off the socket.</param>
    /// <param name="name">The stream name, when this returns true.</param>
    /// <param name="rejection">Why the connection should be dropped, when this returns false.</param>
    /// <param name="intent">Which port this arrived on.</param>
    public static bool TryParse(
        string? streamId,
        out string name,
        out string rejection,
        StreamIntent intent = StreamIntent.Publish)
    {
        name = string.Empty;

        if (streamId is null)
        {
            rejection = "no stream identifier was presented";
            return false;
        }

        if (Encoding.UTF8.GetByteCount(streamId) > MaxIdentifierBytes)
        {
            rejection = $"the stream identifier is longer than {MaxIdentifierBytes} bytes";
            return false;
        }

        // The handshake zero-pads to a four byte boundary, so a trailing NUL is padding.
        var value = streamId.TrimEnd('\0').Trim();

        if (value.Length == 0)
        {
            rejection = "the stream identifier is empty, so the stream has no name";
            return false;
        }

        // An FFmpeg older than 7.0 does not percent-decode the streamid it was given, so a sender
        // on one of those presents the escape it typed into a URL.
        if (value.StartsWith("%23!::", StringComparison.Ordinal))
        {
            value = string.Concat("#!::", value.AsSpan("%23!::".Length));
        }

        return value.StartsWith("#!::", StringComparison.Ordinal)
            ? FromEnvelope(value["#!::".Length..], intent, out name, out rejection)
            : Validate(value, out name, out rejection);
    }

    /// <summary>True when the identifier carried a session key, which is where a token will go.</summary>
    public static bool CarriesSessionKey(string? streamId)
        => streamId is not null
            && streamId.StartsWith("#!::", StringComparison.Ordinal)
            && Pairs(streamId["#!::".Length..]).ContainsKey("s");

    private static bool FromEnvelope(
        string content,
        StreamIntent intent,
        out string name,
        out string rejection)
    {
        name = string.Empty;
        var pairs = Pairs(content);

        var refused = intent == StreamIntent.Publish ? "request" : "publish";

        if (pairs.TryGetValue("m", out var mode) && mode == refused)
        {
            rejection = intent == StreamIntent.Publish
                ? "the caller asked to receive (m=request) and this is the ingest port"
                : "the caller offered to send (m=publish) and this is the consumption port";

            return false;
        }

        if (!pairs.TryGetValue("r", out var resource))
        {
            rejection = "the stream identifier carries no resource key (r), so the stream has no name";
            return false;
        }

        return Validate(resource, out name, out rejection);
    }

    /// <summary>Splits the convention's content.</summary>
    private static Dictionary<string, string> Pairs(string content)
    {
        var pairs = new Dictionary<string, string>(StringComparer.Ordinal);

        foreach (var part in content.Split(',', StringSplitOptions.RemoveEmptyEntries))
        {
            var separator = part.IndexOf('=');

            if (separator > 0)
            {
                // First '=' only.
                pairs[part[..separator]] = part[(separator + 1)..];
            }
        }

        return pairs;
    }

    private static bool Validate(string candidate, out string name, out string rejection)
    {
        name = string.Empty;

        if (!Allowed.IsMatch(candidate))
        {
            rejection = $"'{Describe(candidate)}' is not a usable stream name; "
                + "allowed are letters, digits, dot, dash, underscore and slash, up to 128 of them";
            return false;
        }

        if (candidate.StartsWith('/')
            || candidate.EndsWith('/')
            || candidate.Contains("//", StringComparison.Ordinal)
            || candidate.Split('/').Contains(".."))
        {
            rejection = $"'{Describe(candidate)}' is not a usable stream name; "
                + "a slash must separate two non-empty segments and '..' is not one of them";
            return false;
        }

        name = candidate;
        rejection = string.Empty;

        return true;
    }

    /// <summary>Keeps a hostile identifier from filling a log line with someone else's choosing.</summary>
    private static string Describe(string candidate)
        => candidate.Length <= 64 ? candidate : string.Concat(candidate.AsSpan(0, 64), "...");
}
