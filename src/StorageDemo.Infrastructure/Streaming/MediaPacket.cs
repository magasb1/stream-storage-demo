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
/// Pooling the array is the same question with the same answer, and the answer is measured rather
/// than argued: one to two per cent of what bounds ingest density, so the pump keeps its allocation.
/// <c>DemuxCostBench</c> in the test project is the instrument and
/// <c>.scratch/scale-to-1000/demux-packet-cost.md</c> is the reading, which is where the figures and
/// their caveats live rather than here - a number copied into three files is two copies that will
/// rot. Three of its results are worth knowing before touching this type at all. A pool's whole
/// ceiling, measured against a loop that rents and returns immediately and so is an upper bound
/// rather than a pool, is three to five microseconds a packet against the 250 to 300 the
/// demultiplexer thread spends on one. Allocating the array without zeroing it - which needs none of
/// what follows - is worth two to four tenths of a microsecond, so there is no cheap version of this
/// to reach for either.
///
/// And the third, which is why: what costs is the retention rather than the allocation. Allocating
/// these arrays and dropping them immediately measures the same however hard the collector is
/// running; holding them for the buffer's thirty seconds measures two and a half to three and a half
/// microseconds a packet worse when it runs three times as often. So the cost is not in the
/// <c>new byte[]</c> that a pool would replace - it is in how long this array lives afterwards, which
/// means anything recovering that cost has to know when the last holder is done with it.
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
/// stream's bytes to another stream's viewer, and nothing in a video pipeline reports that. That no
/// holder knows when the last one is done is not a new observation about this type either - it is
/// why <see cref="StreamHub"/> retires layouts instead of freeing them.
///
/// Two smaller things would have to go with it. A rented array is oversized, so
/// <see cref="Bytes"/> - which is <c>Data.Length</c> - stops being the packet's size, and the
/// buffer's ceiling, <see cref="PacketMuxer"/>, <see cref="VideoDecoder"/> and the KLV parser each
/// read it: the muxer would write the padding into every viewer's transport stream. That makes this
/// a change of contract across the consumers, <see cref="Data"/> becoming a
/// <see cref="ReadOnlyMemory{T}"/>, rather than a change to the pump. And the rented array is the
/// next size up the pool keeps, which measures as 23.8 per cent more memory held for the same
/// window - the wrong direction, because the rig found memory rather than processor is what a pod
/// runs out of first above about 150 streams.
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
