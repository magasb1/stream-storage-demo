namespace StorageDemo.Infrastructure.Streaming;

/// <summary>One demultiplexed packet, copied out of libav into managed memory.</summary>
/// <param name="StreamIndex">Which stream inside the transport this belongs to.</param>
/// <param name="IsKeyframe">Whether a decoder can start here.</param>
public sealed record MediaPacket(
    int StreamIndex,
    byte[] Data,
    long Pts,
    long Dts,
    long Duration,
    bool IsKeyframe)
{
    public int Bytes => Data.Length;
}
