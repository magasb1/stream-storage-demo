using FFmpeg.AutoGen.Abstractions;
using StorageDemo.Infrastructure.Media;

namespace StorageDemo.Infrastructure.Streaming;

/// <summary>The frame subscriber that is always attached, keeping a stream's preview current.</summary>
public sealed class Harvester(int size, int quality, TimeSpan cadence)
{
    private readonly Lock _gate = new();

    private byte[]? _preview;
    private DateTimeOffset _encodedAt = DateTimeOffset.MinValue;

    /// <summary>The newest picture, or null before the first one has been decoded.</summary>
    public byte[]? Preview
    {
        get { lock (_gate) { return _preview; } }
    }

    public DateTimeOffset? UpdatedAt => _encodedAt == DateTimeOffset.MinValue ? null : _encodedAt;

    public unsafe void OnFrame(IntPtr frame)
    {
        if (DateTimeOffset.UtcNow - _encodedAt < cadence)
        {
            return;
        }

        var encoded = JpegEncoder.Encode((AVFrame*)frame, size, quality);

        if (encoded is null)
        {
            return;
        }

        lock (_gate)
        {
            _preview = encoded;
            _encodedAt = DateTimeOffset.UtcNow;
        }
    }
}
