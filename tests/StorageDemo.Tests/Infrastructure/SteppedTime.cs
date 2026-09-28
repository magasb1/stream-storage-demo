namespace StorageDemo.Tests.Infrastructure;

/// <summary>
/// A clock that only moves when it is told to, so a rate taken over an interval can be asserted
/// exactly rather than waited for.
///
/// Only <see cref="TimeProvider.GetTimestamp"/> and <see cref="TimeProvider.TimestampFrequency"/>
/// are overridden, because they are the only two anything measuring an interval here uses; a test
/// that needed a wall clock or a timer would be asking this for something it does not have.
/// </summary>
public sealed class SteppedTime : TimeProvider
{
    private long _ticks;

    public override long TimestampFrequency => TimeSpan.TicksPerSecond;

    public override long GetTimestamp() => _ticks;

    public void Advance(TimeSpan by) => _ticks += by.Ticks;
}
