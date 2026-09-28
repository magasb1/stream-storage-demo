namespace StorageDemo.Infrastructure.Streaming;

/// <summary>
/// One demultiplexed packet, copied out of libav into managed memory.
///
/// Copied rather than reference-counted on purpose. A packet in the rolling buffer outlives the
/// read that produced it by up to thirty seconds and is handed to any number of subscribers, so
/// tying its lifetime to libav's reference counting would mean every consumer understanding that
/// contract. A byte array is also what makes the buffer's byte ceiling a real number rather than
/// an estimate.
///
/// Pooling the array is the same question with the same answer, and the answer is now measured
/// rather than argued. <c>DemuxCostBench</c> in the test project reads a camera-rate pattern through
/// the real pump and then again with the allocation removed outright and nothing correct in its
/// place, which is an upper bound on pooling rather than an attempt at it: 17.9 microseconds a
/// packet becomes 14.7, so everything a pool could ever give back is 3.2 microseconds. The same
/// packet costs the rig's per-stream thread 250 to 470 microseconds, nearly all of it in the
/// transport read - <c>.scratch/scale-to-1000/perf-ingest.md</c>'s first experiment traced 69 per
/// cent of that thread into <c>srt_recvmsg</c> and half a per cent into allocating, and swapping in
/// uninitialised arrays moved nothing it measured. At a hundred and fifty camera-rate streams the
/// whole of that 3.2 microseconds is about fourteen milliseconds of processor a second, against the
/// two point one cores the pod spends there. It is not what bounds ingest density.
///
/// What it would cost is the part worth writing down, because nothing owns this array alone. The
/// rolling buffer holds it for the window; every subscriber's channel holds it until that subscriber
/// reads; a subscriber may hold it far longer still, as <see cref="KlvExtractor"/> does by keeping
/// the same reference in the sixty-four deep ring of samples it serves; and
/// <see cref="StreamHub.NewestStartablePackets"/> hands a segment's packets to a muxer running
/// outside the hub's lock while the buffer is free to evict them underneath it. The point at which
/// an array may be reused is therefore "after the last of those", which is not a fact any one of
/// them has, so a pool needs a reference count whose decrements include every channel read, every
/// packet an overflow throws away, and every packet still queued in a subscription that was
/// completed or disposed and never drained. A missed decrement only leaks; a double one hands one
/// stream's bytes to another stream's viewer, and nothing in a video pipeline reports that.
///
/// Two smaller things would have to go with it. A rented array is oversized, so neither
/// <see cref="Bytes"/> nor <c>Data.Length</c> is the packet's size any more - and the buffer's
/// ceiling, the muxer, the frame decoder and the KLV parser each read one of those - which makes
/// this a change of contract across the consumers rather than a change to the pump. And a pool's
/// buckets are powers of two, which the same measurement shows costs 27 per cent more bytes held for
/// the same media - the wrong direction, because the rig found memory rather than processor is what
/// a pod runs out of first above about 150 streams.
/// </summary>
/// <param name="StreamIndex">Which stream inside the transport this belongs to.</param>
/// <param name="IsKeyframe">
/// Whether a decoder can start here. This is what turns a flow of packets into segments, and it
/// is the sender's keyframe interval that decides how coarse those segments are.
/// </param>
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
