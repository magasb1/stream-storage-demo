using System.ComponentModel.DataAnnotations;

namespace StorageDemo.Infrastructure.Streaming;

public sealed class LiveOptions
{
    public const string SectionName = "Live";

    /// <summary>
    /// The named <see cref="HttpClient"/> one replica reaches another on, for a control call and
    /// for a relayed viewer's media alike. Named here rather than in the API project because both
    /// ends of that hop resolve it and only one of them is an API.
    /// </summary>
    public const string PeerClient = "LivePeer";

    /// <summary>
    /// Off by default. Switching this on opens a port that anybody who can reach it may push a
    /// stream into, so it should be a decision rather than an inheritance.
    /// </summary>
    public bool Enabled { get; init; }

    /// <summary>Required in the X-Storage-Token header when set. Set it if the API is reachable.</summary>
    public string? Token { get; init; }

    /// <summary>
    /// Where encoders push. Reaching it lets you push a stream and nothing else, which is the
    /// whole reason it is not the API port.
    /// </summary>
    [Range(1, 65535)]
    public int IngestPort { get; init; } = 9000;

    /// <summary>
    /// How many consecutive ports ingest binds, starting at <see cref="IngestPort"/>. One by
    /// default, which is what this service has always done.
    ///
    /// It exists because libsrt runs one receive worker thread per bound UDP port per process, and
    /// every socket accepted on a port shares its listener's. That single thread is what a replica
    /// runs out of first: measured here at roughly sixty camera-rate streams, well before processor
    /// or memory bind. Binding four ports gives four of those threads and a replica that holds
    /// about four times as much, at the cost of a deployment that has to publish the whole range
    /// and senders that have to be spread across it - no single port is any bigger than it was.
    ///
    /// A count rather than a list, because a Service publishing a contiguous range is one manifest
    /// entry and nobody has asked for holes in it.
    ///
    /// Fewer, larger replicas is also a worse failure: when one dies it takes four times as many
    /// streams into a reconnect with it.
    /// </summary>
    [Range(1, 16)]
    public int IngestPortCount { get; init; } = 1;

    /// <summary>
    /// Where viewers pull live streams, over SRT, symmetric with ingest. Separate from ingest so a
    /// deployment can expose one to the internet and keep the other on a private network, each
    /// with its own rules.
    /// </summary>
    [Range(1, 65535)]
    public int ConsumptionPort { get; init; } = 9010;

    /// <summary>
    /// The address to listen on. Every interface by default, which is what a container wants.
    /// </summary>
    public string IngestAddress { get; init; } = "0.0.0.0";

    /// <summary>
    /// How long a sender may fall silent before the transport gives up on it. The stream then
    /// becomes interrupted rather than gone, and the grace period below decides the rest.
    /// </summary>
    [Range(1, 300)]
    public int FeedTimeoutSeconds { get; init; } = 5;

    /// <summary>
    /// How long an interrupted stream is kept before it is considered gone: still claimed, still
    /// listed, hub and buffer and any recording all still alive. A tile that vanishes and returns
    /// is worse than one showing a state, and after a resume it would be the same stream on both
    /// sides of the gap.
    /// </summary>
    [Range(1, 3600)]
    public int GracePeriodSeconds { get; init; } = 30;

    /// <summary>
    /// The receiver's buffering delay on both media ports, in milliseconds. It is the floor on
    /// end-to-end latency and the budget out of which a lost packet is retransmitted, so it is the
    /// one number to turn down on a link that does not need it.
    ///
    /// Haivision's deployment guide sizes it as a multiple of the round trip: about four times the
    /// RTT as a rule of thumb, three on a link losing under one percent, and never below 60 ms.
    /// A LAN is therefore 60. The internet is whatever the internet is that day.
    ///
    /// libsrt's own live default, kept so that a deployment which sets nothing behaves exactly as
    /// it did before this option existed.
    /// </summary>
    [Range(0, 8000)]
    public int SrtLatencyMs { get; init; } = 120;

    /// <summary>
    /// How many streams this replica holds before it starts refusing new names at the handshake,
    /// with <c>SRT_REJX_OVERLOAD</c>. Zero, the default, is unlimited, so a deployment that sets
    /// nothing behaves exactly as it did before this existed. Only publishers are counted and only
    /// publishers are refused: a viewer is cheap and turning one away helps nobody.
    ///
    /// A name this replica already holds is always admitted, whatever the count. A pod at its limit
    /// that refused its own encoders reconnecting after a blip would abandon the streams it is
    /// already responsible for, and they would have nowhere to come back to.
    ///
    /// There is no formula that turns a pod into a number here, because the ceiling is a joint
    /// budget of open sockets and packet rate rather than either one:
    ///
    ///     streams/175 + pps/45000 &lt; 1
    ///
    /// That budget, and the per-bitrate figures below, were measured on one rig and do not travel:
    /// on a container over loopback the receive worker it describes never passed a quarter of its
    /// core at any load, including through a collapse, so the formula over-predicts its cost by
    /// roughly an order of magnitude and would size a Linux node far below what it can carry. Size
    /// from what a pod is actually delivering instead - the per-stream figures in GET /api/live, and
    /// the kernel's UDP receive errors - and treat the numbers here as the shape rather than the
    /// scale. See .scratch/scale-to-1000/ingest-and-readers.md.
    ///
    /// Twenty streams carrying 300 Mbit/s sit at a third of libsrt's receive thread; seventy-five
    /// streams carrying the same 300 collapse it. So this is set per deployment from the bitrate
    /// the encoders pointed at it actually send. Measured knees, one ingest port, in
    /// .scratch/scale-to-1000/baseline.md: about 150 streams at half a megabit, about 60 at a
    /// camera-like four, fewer than 20 at fifteen. Memory and thread count bind separately and the
    /// deployment manifest carries that arithmetic.
    /// </summary>
    [Range(0, 100_000)]
    public int MaxStreams { get; init; }

    /// <summary>
    /// Identifies this replica. Defaults to POD_NAME, which Kubernetes supplies from the downward
    /// API, and to the machine name elsewhere.
    /// </summary>
    public string? NodeName { get; init; }

    /// <summary>
    /// The address other replicas reach this one on, recorded with the name claim, for example
    /// "http://10.42.0.9:8080". A replica holding no stream of its own forwards a control call here
    /// and pulls a relayed viewer's media through here, because only the owner has the bytes.
    /// </summary>
    public string? PeerBaseUrl { get; init; }

    /// <summary>
    /// The SRT address to hand to a player, when this instance knows its own, for example
    /// "srt://live.example.com:9010". Empty means the client builds one from the host it is
    /// already talking to.
    /// </summary>
    public string? PublicConsumptionUrl { get; init; }

    /// <summary>
    /// How often the preview is refreshed. Short, because a live tile that updates every few
    /// seconds reads as live and one that does not reads as a stuck image.
    /// </summary>
    [Range(1, 300)]
    public int PreviewIntervalSeconds { get; init; } = 2;

    /// <summary>
    /// How much recent stream every hub keeps, as a promise of at least this many seconds. The
    /// actual window moves in whole segments, so a sender with a coarse keyframe interval gives
    /// more than this and in coarser steps.
    /// </summary>
    [Range(5, 300)]
    public double BufferWindowSeconds { get; init; } = 30;

    /// <summary>
    /// The hard memory bound per stream, which is what stops one careless encoder evicting the
    /// service. Thirty seconds is roughly six megabytes for a modest feed and seventy-five for a
    /// contribution one, so this has to be forgiving enough to hold a couple of segments from a
    /// coarse sender or it will bind before the window does and quietly shorten every pre-roll.
    /// </summary>
    [Range(1024 * 1024, 2L * 1024 * 1024 * 1024)]
    public long BufferByteCeiling { get; init; } = 96L * 1024 * 1024;

    /// <summary>
    /// How far either side of a trigger a recording reaches, so an event already under way when
    /// it was noticed is still caught. A floor rather than an exact figure: a recording can only
    /// begin at a position a decoder can start from.
    /// </summary>
    [Range(0, 60)]
    public double PrerollSeconds { get; init; } = 5;

    /// <summary>
    /// How long a recording runs past its trigger when no duration was given, and how much longer
    /// each further trigger extends it. Continuous detection then leaves one clip covering the
    /// whole event rather than a drift of overlapping near-duplicates.
    /// </summary>
    [Range(1, 3600)]
    public double DefaultRecordingSeconds { get; init; } = 30;

    /// <summary>
    /// How much of a recording is muxed to a local file before it is stored and the file deleted.
    ///
    /// A part, not a segment: a segment in this codebase is the buffer's unit, one keyframe to the
    /// next, and is the sender's to decide. This is ours, and it is minutes rather than seconds.
    ///
    /// It bounds disk and memory for a recording of any length, since a camera running for six
    /// hours costs a handful of parts at a time - <see cref="RecorderPendingParts"/> says how many.
    /// It also decides how much is lost if the pod goes:
    /// everything up to the last completed part is already in storage. Short enough to keep both
    /// small, long enough that a day of recording is not a hundred thousand objects.
    /// </summary>
    [Range(0.1, 60)]
    public double RecordingPartMinutes { get; init; } = 5;

    /// <summary>
    /// The ceiling on one recording, so a stream nobody stops still ends somewhere. Disk no longer
    /// needs a ceiling, because segments are stored and deleted as they complete; this bounds the
    /// document instead. On reaching it the recording closes and a still-firing trigger starts the
    /// next one.
    /// </summary>
    [Range(1, 7 * 24 * 60)]
    public int MaxRecordingMinutes { get; init; } = 12 * 60;

    /// <summary>
    /// Queue depth for a viewer, in seconds of media. Overflowing costs a viewer a skip forward to
    /// live, which is the right answer for something that must never accumulate delay - and this is
    /// how much delay it may accumulate first, which is the only unit that statement can be made
    /// in. A viewer's queue is therefore sized per stream, from what the sender is actually
    /// sending, plus whatever rollback that viewer asked to start from.
    ///
    /// It is as honest as the sender's own declaration, which is what the rate is read from: a
    /// sender that overstates its frame rate gets a queue shallower in real seconds than this asks
    /// for, and one that understates it a deeper one. <see cref="ViewerQueuePackets"/> is what
    /// bounds the cost of the second case, and a sanity clamp on the declared rate the first.
    ///
    /// This used to be stated in packets, and 2000 of them read as a generous queue right up until
    /// somebody worked out what it was worth: a packet here is one demultiplexed frame, so at
    /// twenty-five a second that queue was eighty seconds deep. Skip-to-live never engaged, because
    /// a viewer would have had to fall more than a minute behind to reach it, and a viewer that far
    /// behind live is not a viewer any more.
    ///
    /// Four seconds: two keyframe intervals at a common two-second setting, so an ordinary
    /// scheduling or network hiccup costs nothing, and short enough that the skip when one does not
    /// recover is a correction rather than a jump out of the recent past. Overflowing costs the
    /// wait for the next keyframe on top of this, which is why it is not shorter.
    ///
    /// Four is what measured clean rather than what was argued for: at this depth five hundred and
    /// fifty healthy readers against two hundred streams skipped nothing at all, while two hundred
    /// deliberately slow ones skipped about once every six seconds each, which is this protection
    /// working. The case for a larger figure - that four seconds is two keyframe intervals, and thin
    /// against a collection pause, a retransmit burst or a handover - is untested, because the rig
    /// behind those numbers has no way to make a reader stall in bursts rather than steadily. See
    /// .scratch/scale-to-1000/ingest-and-readers.md.
    /// </summary>
    [Range(0.25, 60)]
    public double ViewerQueueSeconds { get; init; } = 4;

    /// <summary>
    /// How long a direct viewer's socket may accept nothing at all before the viewer is dropped.
    ///
    /// It bounds one condition and only one: a peer that is connected, acknowledging, and taking no
    /// bytes. That peer exists because <c>SRTO_TLPKTDROP</c> is the player's to set - one query
    /// parameter on its URL clears it - and with it clear libsrt's <c>sndDropTooLate</c> returns
    /// immediately, so nothing ever frees the send buffer and <c>srt_sendmsg</c> waits. The two cases
    /// were measured against each other on the sending socket, in <c>SrtSendPressureTests</c>, and
    /// they are two orders of magnitude apart: a peer advertising drop stalls about a second and
    /// recovers - that second being max(latency + SRTO_SNDDROPDELAY, 1000) + 20 ms, so 1020 ms at
    /// <see cref="SrtLatencyMs"/> of 120 - while a peer with the flag cleared never recovers at all:
    /// probing libsrt 1.5.3 directly for #20 gave sixteen unbroken seconds of timeouts in a
    /// sixteen-second window, ended only by closing the socket. Neither figure moves with <c>SRTO_SNDTIMEO</c>, which is why that is a const in
    /// <c>SrtListener</c> and not an option: it is the polling interval, and this is the policy.
    ///
    /// An ordinary viewer on the worst network cannot reach this, and that is the property being
    /// protected rather than the drop. A peer reading literally nothing, with the sender paced at the
    /// stream's own bitrate and <c>SRTO_SNDTIMEO</c> set, produced zero timeouts: the full
    /// 12.29 Mbit/s went onto the socket while libsrt discarded from its own buffer whatever the peer
    /// had not taken, holding 1.7 MB. Such a viewer loses picture instead, which is #11's settled
    /// answer and not a fault. Neither can a viewer that asked for a rollback: seeding one hands the
    /// socket up to <see cref="ViewerQueuePackets"/> packets - about twenty-three seconds of media,
    /// some thirty-six megabytes, well past the twelve-megabyte send buffer - before a live packet
    /// arrives, and that measures as two isolated timeouts, never two in a row. The budget is
    /// wall-clock and resets on
    /// every accepted chunk precisely so those do not add up; a rule counting timeouts would drop
    /// every viewer that asked to start from the past.
    ///
    /// This is also what bounds per-viewer send-buffer memory, which is the non-obvious half.
    /// Nothing else does. A blocked viewer's sending socket was measured holding about twelve
    /// megabytes - libsrt's default <c>SRTO_SNDBUF</c>, filled because the peer's drop flag is clear
    /// and so nothing ages out of it - and three hundred of those is 3.6 GB against a 4Gi pod. The
    /// only thing that returns that memory is dropping the viewer, so shortening this shortens how
    /// much of a pod a fan-out of such peers can hold, and lengthening it is what has to be justified.
    ///
    /// <c>SRTO_SNDBUF</c> is deliberately not set anywhere, and deliberately not in
    /// <c>SRT_SOCKOPT</c> either, so nobody reaches for it as the memory bound instead. It is
    /// pre-bind only: setting it on an accepted socket is refused with "Cannot do this operation on a
    /// BOUND socket", so a per-viewer size does not exist. It could only go on the listener, one
    /// figure for every viewer of every stream on the pod, and this service does not learn a stream's
    /// bitrate until an encoder arrives. Being wrong low is not a smaller buffer but a manufactured
    /// stall: at 262 KB, against a well-behaved peer, a probe never completed in 240 s, because
    /// <c>sndDropTooLate</c> runs on entry to <c>srt_sendmsg</c> and never while a send is already
    /// waiting, so an undersized buffer is never freed by its own head-of-line packet ageing out.
    /// Being wrong high costs nothing, and the 12 MB default already covers any stream to about
    /// 94 Mbit/s.
    ///
    /// Five seconds: far enough above the rollback seed, and anything else that measures as an
    /// isolated timeout, to leave them alone; short enough that a viewer holding twelve megabytes for
    /// no reason holds them for five seconds rather than for as long as it likes. The floor of one
    /// second is deliberately above the 1020 ms drop threshold, so no setting in range can drop an
    /// ordinary viewer on the strength of libsrt's own drop delay.
    ///
    /// Direct viewers only. A forward's far end can clear its drop flag too, but its dialled socket
    /// has no <c>SRTO_SNDTIMEO</c> set and so never reaches this; that is filed separately rather
    /// than widening this change.
    /// </summary>
    [Range(1, 60)]
    public int ViewerSendStallSeconds { get; init; } = 5;

    /// <summary>
    /// The ceiling on a viewer's queue, in packets, whatever <see cref="ViewerQueueSeconds"/> and a
    /// rollback work out to. It is what stops a sender claiming an absurd frame rate from sizing a
    /// queue per viewer that this replica cannot afford, and it is why the depth above can be
    /// expressed in seconds at all.
    ///
    /// Kept under its old name and its old value, so a deployment that tuned it keeps a bound it
    /// recognises. What changed is that it is a ceiling rather than the depth itself: a deployment
    /// that lowered it still gets no more than it asked for, and one that left it alone no longer
    /// gets a queue measured in minutes.
    ///
    /// It is not a formality. At twenty-five frames a second beside AAC audio it is about
    /// twenty-eight seconds of media, which is less than the rolling buffer's own window, so it is
    /// this rather than <see cref="ViewerQueueSeconds"/> that decides how far back the deepest
    /// rollback a viewer can be given actually reaches - see the pre-roll clamp in
    /// <c>LiveStreamCoordinator</c>, which is where the two meet.
    ///
    /// A forward still takes this figure as its depth outright rather than as a ceiling. A forward
    /// is not a viewer - it is one configured far end rather than one of a thousand arriving
    /// players - and sizing it is not what this option was changed for.
    /// </summary>
    [Range(64, 100_000)]
    public int ViewerQueuePackets { get; init; } = 2_000;

    /// <summary>
    /// Queue depth for a recorder. Larger, because overflowing here is not a skip: it ends the
    /// recording and marks the document truncated, and that must be genuinely rare.
    ///
    /// Still in packets, and now deliberately rather than provisionally. What this option used to say
    /// could not safely be seconds has been fixed: the queue no longer has to cover a part upload,
    /// because <c>StreamRecorder.StorePartsAsync</c> took that off the read path. What settled it
    /// afterwards was working out what seconds would actually buy, with the same rate arithmetic
    /// <see cref="StreamLayout"/> uses for a viewer. Nothing, for every stream this service carries:
    /// at or below about sixty-seven packets a second the floor such a change would need rules, and a
    /// 25 fps camera lands back on exactly this number; a camera with AAC gains a tenth; KLV beside
    /// video gains a third; only a transport above about two hundred packets a second sees three
    /// times it, and nothing here sends one. Against the ten minutes of slack
    /// <see cref="RecorderPendingParts"/> now provides, a third more queue is a rounding error, and
    /// it is not worth two more options and the prose to explain them. If a fast transport ever
    /// arrives, that is when to do it.
    ///
    /// What this figure is not is a memory bound, and the packet is why. Memory is bytes, and at a
    /// fixed bitrate a packet gets larger as the packet rate falls, so this costs most exactly where
    /// it looks most generous: twenty thousand packets of a 25 Mbps feed at 25 fps is a frame of
    /// 125 KB apiece and about 2.5 GB, against a pod limit of 4Gi, while the same twenty thousand of
    /// a thousand-packet transport is a tenth of that. Only a recording whose storage has stalled for
    /// minutes ever fills it, and taking the upload off the read path made such a stall far less
    /// likely - but the ceiling is the ceiling, it is unchanged by any of this, and what it wants is
    /// to be stated in bytes as <see cref="BufferByteCeiling"/> already is. Its own change.
    /// </summary>
    [Range(64, 1_000_000)]
    public int RecorderQueuePackets { get; init; } = 20_000;

    /// <summary>
    /// How many finished parts may be waiting for storage before a recorder has to wait for it.
    ///
    /// This is the slack that took the part upload off the read path. A recorder muxes a part to a
    /// local file, hands it over and opens the next one, so an upload that finishes inside a part's
    /// duration costs the packet queue nothing whatsoever. With one part allowed to wait behind the
    /// one being uploaded, storage can stall for two whole parts - ten minutes at the defaults -
    /// before the queue is touched at all.
    ///
    /// It is a disk figure as much as a slack one, and it loosens a promise this service used to make.
    /// Disk was bounded by one part per recording; it is now bounded by this plus two, the one being
    /// written and the one being uploaded. At a 25 Mbps contribution feed a part is about 940 MB, so
    /// the default is three of those rather than one, and <c>k8s/live/deployment.yaml</c> carries what
    /// that does to the recordings-per-pod ceiling. Raise it where storage is slow and disk is cheap.
    /// One is the least it can be, because a recorder that could hand nothing over would be back to
    /// uploading between two reads.
    ///
    /// It is also a shutdown figure, which is the one place raising it costs something that is not
    /// disk. On SIGTERM the muxing loop cancels, hands over its last part and waits for the storer,
    /// which must then upload this many parts plus the one in flight, serially and without
    /// cancellation, because a muxed part is recorded video and dropping it would lose bytes the
    /// recorder has already counted. <c>LiveStreamEntry</c> gives a recording sixty seconds to do
    /// that, against a termination grace period of the same sixty, and that budget did not move when
    /// this option arrived: two parts at camera rate is 300 MB and comfortable, two parts of a
    /// contribution feed is about 1.9 GB and would need 250 Mbit/s sustained to land, failing which
    /// the recording is abandoned mid-upload. Before this option it was always one part. Raising it
    /// raises what a pod has to flush in that same minute.
    /// </summary>
    [Range(1, 16)]
    public int RecorderPendingParts { get; init; } = 1;

    /// <summary>
    /// How long libav may spend working out what an arriving stream contains, in seconds.
    ///
    /// This is dead time between a camera connecting and its stream being on air, and libav's own
    /// default is five seconds. MPEG-TS repeats its tables every hundred milliseconds or so, so a
    /// second is generous for a camera that presents everything at once.
    ///
    /// Raise it for a source that starts its audio late: a probe that ends before the audio track
    /// appears produces a stream, and recordings from it, with no sound.
    /// </summary>
    [Range(0.1, 30)]
    public double ProbeSeconds { get; init; } = 1;

    /// <summary>
    /// How many bytes libav may read while working that out. The other half of the same limit,
    /// and the one that binds on a high bitrate source.
    /// </summary>
    [Range(32 * 1024, 64 * 1024 * 1024)]
    public long ProbeBytes { get; init; } = 1024 * 1024;

    /// <summary>Where recordings are written before they are uploaded as documents.</summary>
    public string? RecordingDirectory { get; init; }

    /// <summary>
    /// Where the file-backed source store keeps its JSON, for the deployments that have no Redis.
    /// Unset puts it beside the recordings, because that is already the directory an operator gives
    /// this service when they want its state somewhere they chose. The default cannot be written
    /// here: it reads <see cref="RecordingDirectory"/>, so it is resolved where the store is built.
    /// </summary>
    public string? SourceFile { get; init; }

    /// <summary>
    /// How long a forward that failed waits before it is tried again. Short, because the usual
    /// reason is a far end that restarted and an operator watching a disconnected row wants it back
    /// without touching anything; long enough that an unreachable host is not dialled in a tight
    /// loop for as many hours as it stays down.
    /// </summary>
    [Range(1, 600)]
    public int ForwardRetrySeconds { get; init; } = 5;

    /// <summary>
    /// Transports a manual stream may name. Anything outside this list is refused, which is what
    /// stops "create a stream" from turning into "read this local file". Automatic ingest needs no
    /// such list: it is one port and one protocol, and nobody chooses a URL.
    ///
    /// Adding <c>http</c> or <c>https</c> here is a further decision beyond the one this list was
    /// first drawn up to guard, worth stating plainly: a source URL this service dials is not only
    /// "read this local file" any more, it is "reach anything on the network this pod can reach",
    /// cloud metadata endpoints included. The default leaves both out for exactly that reason -
    /// pulling HTTP or HLS is something a deployment opts into, not something it inherits.
    /// </summary>
    public string[] AllowedSchemes { get; init; } = ["udp", "rtp", "srt"];

    /// <summary>
    /// Demuxer options for a manual input over udp, rtp or srt. The timeout gives up on a source
    /// nothing arrives from, rather than holding a socket until the process restarts, and the
    /// buffer absorbs a burst on a busy link.
    ///
    /// Not used for http or https: <see cref="HttpInputOptions"/> is its own dictionary, because
    /// <c>fifo_size</c> means nothing to a TCP connection and the one option HTTP genuinely needs -
    /// a reconnect on a dropped connection - means nothing to UDP.
    /// </summary>
    public Dictionary<string, string> ManualInputOptions { get; init; } = new()
    {
        ["timeout"] = "30000000",
        ["fifo_size"] = "1000000",
    };

    /// <summary>
    /// Demuxer options for a manual input pulled over http or https, HLS included: libav auto-
    /// detects the HLS demuxer from the playlist it fetches over this same protocol, so one set of
    /// options covers a raw HTTP container and a live HLS playlist alike.
    ///
    /// The reconnect options are the reason this exists as its own dictionary rather than a branch
    /// in <see cref="ManualInputOptions"/>: an HTTP source is ordinarily a long-lived TCP
    /// connection to a server this deployment does not run, and a connection that drops for a
    /// moment is the common case rather than the stream ending. Without them a dropped socket ends
    /// the whole pull and leaves the reconcile pass to notice and redial from cold, seconds later
    /// at best; with them libav's own http protocol reconnects the one request that failed.
    /// </summary>
    public Dictionary<string, string> HttpInputOptions { get; init; } = new()
    {
        ["timeout"] = "10000000",
        ["reconnect"] = "1",
        ["reconnect_streamed"] = "1",
        ["reconnect_delay_max"] = "2",
    };

    /// <summary>
    /// How long an http or https pull may spend working out what it is, and how much it may read
    /// doing it - the same pair <see cref="ProbeSeconds"/> and <see cref="ProbeBytes"/> are for
    /// every other manual input, sized differently because the two are not comparable sources. A
    /// live UDP or SRT feed is mid-stream the moment its socket opens and MPEG-TS repeats its
    /// tables every hundred milliseconds, so a second is generous; an HTTP pull pays a TCP and
    /// often a TLS handshake before its first byte, and HLS pays a playlist fetch on top of that
    /// before a single frame of video has even been requested. Kept separate rather than raising
    /// the shared figures, which would slow every other pull's join time to suit a source most
    /// deployments never use.
    /// </summary>
    [Range(0.1, 30)]
    public double HttpProbeSeconds { get; init; } = 5;

    /// <inheritdoc cref="HttpProbeSeconds"/>
    [Range(32 * 1024, 64 * 1024 * 1024)]
    public long HttpProbeBytes { get; init; } = 4 * 1024 * 1024;
}
