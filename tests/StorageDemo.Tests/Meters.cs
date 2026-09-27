using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace StorageDemo.Tests;

/// <summary>
/// Collects one meter's measurements, and only that one's, for as long as it is not disposed.
///
/// The filter is the meter's scope rather than its name: several hosts run in one test process and
/// each publishes a meter of the same name, which is exactly why both metric classes give their
/// <c>Meter</c> a scope of themselves.
///
/// It has to be started before whatever it is measuring, because a counter is an event and not a
/// value: a listener that starts afterwards sees nothing at all, however many times the counter was
/// added to. Observable instruments are the other way round and report only when asked.
///
/// Shared by the streaming meter's tests and the API meter's, because it is the only honest way to
/// read either - what a dashboard sees is measurements and tags, not the fields behind them.
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
    /// The instance whose meter to listen to, which is the object passed as <c>MeterOptions.Scope</c>.
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

    /// <summary>One tag's value on one measurement, as text, or null where it carries no such tag.</summary>
    public static string? Tag(Recording measurement, string key)
        => measurement.Tags.FirstOrDefault(tag => tag.Key == key).Value?.ToString();

    /// <summary>
    /// Every value of one tag on one instrument, counted: what a dashboard's group-by shows, and the
    /// only honest way to read an instrument whose whole meaning is in a tag - four outcomes on
    /// <c>live.recordings</c> are four different things, not one number.
    /// </summary>
    public IReadOnlyDictionary<string, int> Tally(string instrument, string key)
        => Of(instrument)
            .GroupBy(measurement => Tag(measurement, key) ?? string.Empty)
            .ToDictionary(group => group.Key, group => group.Count());

    /// <summary>How many measurements of one instrument carry one value of one tag.</summary>
    public int Count(string instrument, string key, string value)
        => Of(instrument).Count(measurement => Tag(measurement, key) == value);

    public void Dispose() => _listener.Dispose();

    private void Add(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        => _seen.Enqueue(new Recording(instrument.Name, value, tags.ToArray()));
}
