using System.Collections.Concurrent;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>
/// What the two media listeners are actually doing, so readiness can be about the thing that
/// matters.
/// </summary>
public sealed class LiveListeners
{
    private readonly ConcurrentDictionary<StreamIntent, int> _bound = new();
    private readonly ConcurrentDictionary<StreamIntent, int> _expected = new();

    /// <summary>False when live streaming is switched off, and then nothing here is a fault.</summary>
    public bool Enabled { get; set; }

    /// <summary>
    /// Why this replica cannot serve media at all: no libsrt, so neither port can be opened.
    /// </summary>
    public string? Fault { get; set; }

    /// <summary>How many ports this side is meant to have.</summary>
    public void Expect(StreamIntent port, int count) => _expected[port] = count;

    /// <summary>Called by the listener once <c>srt_listen</c> has succeeded.</summary>
    public void Bound(StreamIntent port) => _bound.AddOrUpdate(port, 1, (_, bound) => bound + 1);

    public void Stopped(StreamIntent port) => _bound.AddOrUpdate(port, 0, (_, bound) => bound - 1);

    public bool IsListening(StreamIntent port)
        => _bound.GetValueOrDefault(port) >= _expected.GetValueOrDefault(port, 1);

    /// <summary>Null when this replica can serve encoders and viewers; otherwise why it cannot.</summary>
    public string? NotServing()
    {
        if (!Enabled)
        {
            return null;
        }

        if (Fault is { Length: > 0 } fault)
        {
            return fault;
        }

        if (!IsListening(StreamIntent.Publish))
        {
            return "the ingest port is not accepting";
        }

        return IsListening(StreamIntent.Subscribe) ? null : "the consumption port is not accepting";
    }
}
