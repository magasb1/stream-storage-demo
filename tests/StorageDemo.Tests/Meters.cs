using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace StorageDemo.Tests;

/// <summary>
/// Collects one meter's measurements, and only that one's, for as long as it is not disposed.
/// </summary>
internal sealed class Meters : IDisposable
{
    internal readonly record struct Recording(
        string Instrument,
        double Value,
        IReadOnlyList<KeyValuePair<string, object?>> Tags);

    private readonly ConcurrentQueue<Recording> _seen = new();

    private readonly MeterListener _listener;

    /// <param name="scope">
    /// The instance whose meter to listen to, which is the object passed as
    /// <c>MeterOptions.Scope</c>.
    /// </param>
    public Meters(object scope)
    {
        _listener = new MeterListener
        {
            InstrumentPublished = (instrument, listening) =>
            {
                if (ReferenceEquals(instrument.Meter.Scope, scope))
                {
                    listening.EnableMeasurementEvents(instrument);
                }
            },
        };

        _listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Add(instrument, value, tags));
        _listener.SetMeasurementEventCallback<int>((instrument, value, tags, _) => Add(instrument, value, tags));
        _listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Add(instrument, value, tags));
        _listener.Start();
    }

    public IReadOnlyList<Recording> Read()
    {
        _listener.RecordObservableInstruments();

        return [.. _seen];
    }

    /// <summary>Every measurement of one instrument, oldest first.</summary>
    public IReadOnlyList<Recording> Of(string instrument)
        => [.. Read().Where(recording => recording.Instrument == instrument)];

    /// <summary>The newest measurement of one instrument, or null if it reported none.</summary>
    public double? Value(string instrument)
        => Of(instrument).Select(recording => (double?)recording.Value).LastOrDefault();

    /// <summary>Everything one instrument reported, added up, which is what a counter means.</summary>
    public double Total(string instrument) => Of(instrument).Sum(recording => recording.Value);

    /// <summary>The tags on the single measurement of one instrument, as a sorted, readable list.</summary>
    public IReadOnlyList<KeyValuePair<string, object?>> Tags(string instrument)
        => Of(instrument).Single().Tags;

    public void Dispose() => _listener.Dispose();

    private void Add(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        => _seen.Enqueue(new Recording(instrument.Name, value, tags.ToArray()));
}
