# Two hundred streams and five hundred readers, measured

Measured 2026-09-27 with `tests/StorageDemo.Tests/Integration/LiveScaleTests.cs`, which is the rig
that produced every table here. `LIVE_SCALE=1 dotnet test --filter LiveScaleTests` repeats the first
one; the invocation above each of the others says what was changed.

**The machine was four cores and 15 GiB, in a cloud container, with the service in the test process
and the load generator beside it on the same four cores.** The senders and the service therefore
compete, which matters only at the last row of the bitrate table and is called out there. Treat
absolutes as this machine's and ratios between rows as sound - the same rule `baseline.md` sets, for
the same reason.

Three things about the rig before any number is read.

- **Senders push ten streams per process** and readers take ten connections per process, because a
  process per stream is what runs out first: two hundred senders is ten gigabytes of resident ffmpeg
  and several cores of overhead before the service has done anything. The cost is shared fate - one
  output blocking holds up its siblings - so every row also carries the rig's own processor share.
- **The senders pace about seven percent under the file's nominal rate**, consistently, at every
  load. So each row is compared against what the same senders achieved at the first and smallest step
  (`of rig`) as well as against the file (`of src`). The first column is what scale did; the second
  is what the rig always does.
- **The `RcvQ:w` column is the busiest libsrt receive worker of the two ports**, not their sum. Each
  bound port has one and it cannot exceed one core, which is what makes it a ceiling; ingest's is
  usually the busy one, and a reader-heavy row is where the consumption port's takes over.
- **Relayed readers are in-process HTTP reads** of `api/live/peer/view/{name}` against the test
  server, drained and discarded. That measures the fan-out, the per-viewer subscription and the
  per-viewer transport-stream muxer exactly, and it measures no socket at all: there is no Kestrel
  write path and no kernel in it. The direct readers are real players on the consumption port, and
  they are what says the two routes agree. Their drain loop also runs in the service's own process,
  so `pod` in a reader row is a few percent generous.

## The question as asked: 200 streams, 500 readers

`LIVE_SCALE=1`, the default sweep. Pattern `testsrc2=size=640x360:rate=25`, 0.80 Mbit/s of payload
per stream (0.87 on the wire). Readers are 500 relayed plus 50 real players.

| streams | readers | Mbit/s in | of rig | Mbit/s out | RcvQ:w | pod | rig | RSS | threads | kernel drops | lost | verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 50 | 0 | 0.73 | 100 % | - | 4 % | 0.4 | 0.2 | 0.41 GiB | 130 | 0 | 0 | clean |
| 100 | 0 | 0.74 | 101 % | - | 8 % | 0.6 | 0.4 | 0.78 GiB | 231 | 0 | 0 | clean |
| 150 | 0 | 0.74 | 101 % | - | 11 % | 0.9 | 0.5 | 1.24 GiB | 329 | 0 | 0 | clean |
| 200 | 0 | 0.74 | 101 % | - | 16 % | 1.2 | 0.8 | 1.52 GiB | 429 | 0 | 0 | clean |
| 200 | 100 | 0.75 | 102 % | 0.87 | 16 % | 1.3 | 0.9 | 1.82 GiB | 428 | 0 | 0 | clean |
| 200 | 200 | 0.75 | 102 % | 0.87 | 16 % | 1.4 | 1.0 | 2.00 GiB | 427 | 0 | 0 | clean |
| 200 | 350 | 0.74 | 100 % | 0.87 | 15 % | 1.4 | 0.9 | 1.75 GiB | 428 | 0 | 0 | clean |
| **200** | **550** | **0.75** | **102 %** | **0.87** | **15 %** | **1.4** | **0.9** | **1.85 GiB** | **432** | **0** | **0** | **clean** |

**Two hundred streams and five hundred readers is not where this breaks.** 148 Mbit/s in and
437 Mbit/s out, on 1.4 of four cores, with the libsrt receive worker at fifteen percent of the one
core it is confined to, under two gigabytes resident, and not a packet lost anywhere. A second run of
the same sweep agreed row for row: delivered rates within a percent, processor within a tenth of a
core, memory within four percent.

So the rest of this page is about finding the edge, which took two more sweeps: one raising the
bitrate, one raising the readers.

## Where ingest actually breaks: bitrate, not stream count

```
LIVE_SCALE=1 LIVE_SCALE_PICTURE="testsrc2=size=1280x720:rate=30" LIVE_SCALE_BITRATE=4000k \
  LIVE_SCALE_STREAMS=25,50,100,150,200 LIVE_SCALE_READERS=0 LIVE_SCALE_PATTERN=300
```

4.00 Mbit/s of payload per stream, which is a real camera rather than a test pattern.

| streams | Mbit/s in | of rig | offered | RcvQ:w | pod | rig | RSS | threads | kernel drops | lost | verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 25 | 3.70 | 100 % | 100 Mbit/s | 4 % | 0.3 | 0.2 | 0.65 GiB | 82 | 0 | 0 | clean |
| 50 | 3.74 | 101 % | 200 | 8 % | 0.7 | 0.3 | 1.37 GiB | 133 | 0 | 0 | clean |
| **100** | **3.73** | **101 %** | **400** | **15 %** | **1.4** | **0.6** | **2.50 GiB** | **229** | **0** | **0** | **clean** |
| 150 | 4.31 | 116 % | 600 | 22 % | 2.1 | 0.9 | 4.06 GiB | 329 | 12,876 | 41,264 (6.8 %) | lossy |
| 200 | 2.35 | 63 % | 800 | 24 % | 2.8 | 2.0 | 5.26 GiB | 430 | 3,571,865 | 1,667,558 (77 %) | collapsed |

**The knee is between 100 and 150 camera-rate streams, which is 400 to 600 Mbit/s arriving.** In
stream counts it is not the same answer as the table above at all: the 200 light streams that were
clean carried 148 Mbit/s, and 150 heavy ones carrying 600 were already losing.

**The 150-stream row is the interesting one and it is the one a dashboard would call healthy.** Every
stream is listed live. The delivered rate is *higher* than the step before it - 116 % of what the same
senders managed at 25 streams - because a receiver catching up on retransmissions delivers more than
the source rate, not less. The only figures telling the truth are 41,264 packets lost in fifteen
seconds and 12,876 kernel UDP receive errors. That is exactly the order `docs/observability.md` puts
its alerts in, and this row is the case that page was written for: the rate is reassuring, the loss is
not, and the kernel counter is the one that cannot be argued with.

**The row after it is the collapse `baseline.md` described**: 77 % of the transport's packets lost,
3.6 million kernel drops in a fifteen-second window, and each stream delivering 2.35 of 4.00 Mbit/s
while the registry still lists all two hundred as live.

## What ran out, by name

The rig reads `/proc/self/task` and reports threads by name, summed per name, shares of one core.

At the collapsed row - 200 camera-rate streams:

```
.NET Long Runni 179 %,  .NET TP Worker 66 %,  SRT:RcvQ:w2 24 %,  SRT:TsbPd 6 %
```

At the clean 200-stream, 550-reader row:

```
.NET TP Worker 72 %,  .NET Long Runni 32 %,  SRT:RcvQ:w2 15 %,  SRT:TsbPd 13 %,  SRT:SndQ:w1 5 %
```

- **The per-stream demultiplexer threads are the cost, not the receive worker.** `.NET Long Runni` is
  the `LongRunning` task per feed - `LiveStreamCoordinator.Feed`, which is synchronous and so really
  does hold a thread - and at the collapse it is 1.8 cores against the receive worker's 0.24. Ingest
  scale on a Linux node is therefore bounded by per-packet cost on that thread rather than by the
  accept path. Note what that does *not* say - the socket reader is not exonerated, it is starved;
  the bullet below is the careful version.

  **Corrected**: this bullet used to say the same measurement made pooling `StreamDemuxer.Pump`'s
  per-packet `byte[]` worth doing, and it does not. `perf-ingest.md`'s Experiment 1 had already
  bounded that at a hundred streams and dropped it, and `demux-packet-cost.md` has since measured it
  at camera rate off a file, where the effect is an order of magnitude above the noise instead of an
  order below it. Pooling's whole ceiling - the pump's loop against the same loop with the allocation
  removed outright, differing in one line - is **3.1 to 5.4 µs a packet** depending on how hard the
  collector happens to be running, against the **250 to 300 µs** this thread spends on a packet on a
  rig. That is 0.014 to 0.024 of a core at 150 streams, out of the 2.1 the pod spends there.
  Allocating without zeroing, which needs no reference count and no change of contract, is worth
  **0.2 to 0.4 µs** - measured in isolation, because in the pump it never showed at all in twelve
  replicates. And a pool holds **23.8 %** more memory for the same window, where memory binds first.

  The same measurement says where that cost actually is, which is the part this bullet got most
  wrong: **the retention, not the allocation.** Allocating a packet's array and dropping it at once
  costs the same however often the collector runs; holding it for the buffer's thirty seconds costs
  2.5 to 3.6 µs/pkt more when the collector runs three times as often. A pool cannot have that back
  without knowing when the last holder is finished, which is the reference count - so there is no
  cheap end of this to pick up.

  Naming the thread was right; naming the allocation inside it was a guess. What the figures do
  support is the transport read: reading the same pattern at libsrt's 1,316-byte message granularity
  rather than 64 KB costs **3.4 to 4.9 µs a packet** on this side of the P/Invoke alone, about what
  the whole allocation costs, and `perf-ingest.md`'s Experiment 3 puts 68.9 % of the demultiplexer
  thread's *samples* inside `SrtSocketStream.Read`. Two cautions on that last figure, which this
  branch has already once cited wrongly: those are wall-clock samples, blocked or running, so they
  include time asleep waiting for a message; and the "69 %" in that note is `SRT:RcvQ:w2`'s
  system-time share, a different thread group. The processor split for this thread group is 46 %
  system against 54 % user.
- **The demultiplexer threads ate the cores the receive worker needed** - which is a weaker claim
  than "the receive path is fine", and the weaker one is what the rig supports. At the collapsed row
  the pod wanted 2.8 cores and the rig 2.0 on a four-core box, so every thread there is
  scheduler-bound and a thread's *share* is a floor under its appetite, not a measure of it: a
  starved thread reads as an idle one. And 3.6 million kernel UDP receive errors means the packets
  died in the socket buffer **before** libsrt read them, so the receive path was failing to drain
  whatever its processor share says. The honest reading is that demultiplexing spent the cores and
  the receive path starved as a consequence. The 150-stream row is the cleaner evidence for it: pod
  2.1, rig 0.9, three of four cores, loss already starting.
- **`SRT:RcvQ:w` never exceeded 24 % of its core, at any load, including the collapse.** That
  contradicts `baseline.md`'s central finding - a knee at about 60 % of that thread, and the budget
  `streams/175 + pps/45000 < 1` - and the difference is the rig underneath, not the code: those
  figures crossed a WSL2 virtual NIC and a Docker bridge, where a packet costs the receive path
  several times what it costs on this container's loopback. By that formula the clean 200-stream row
  should have been impossible at 1.48 of budget. **The formula is rig-specific and should not be used
  to size a Linux node**; the loss and kernel-drop signals should.
- **Four cores was the real limit at the last row.** The pod took 2.8 and the rig 2.0 of 4, so the
  senders and the service were fighting, and part of that collapse is the box rather than the design.
  The 150-stream row is cleaner evidence: pod 2.1, rig 0.9, three of four cores, and loss already
  starting. The honest reading is "400 to 600 Mbit/s of ingest per four cores, and the demux threads
  are what spends it".
- **Memory is arithmetic on the buffer, as `baseline.md` said.** These figures are the whole test
  host's resident set, the rig's senders excepted but its in-process readers and xunit included, so
  read them as an upper bound on the service's own - it matters least for the ingest-only rows, which
  is where the OOM conclusion below is drawn from. 7 MB per stream at 0.8 Mbit/s
  (1.52 GiB at 200), 26 MB at 4 Mbit/s (5.26 GiB at 200). `k8s/live/deployment.yaml`'s 4 GiB limit
  therefore OOM-kills at about 150 camera-rate streams - comfortably after the 60 that manifest's own
  arithmetic sizes it for, which is the right way round, but worth knowing that memory binds before
  the processor does above about 150.

## Readers: no ceiling found

```
LIVE_SCALE=1 LIVE_SCALE_STREAMS=100 LIVE_SCALE_READERS=500,1000,1500,2000 LIVE_SCALE_PATTERN=420
```

100 streams of 0.8 Mbit/s, then relayed readers ramped to two thousand. "Readers" includes the 50
real players; the rest are relayed.

| readers | Mbit/s in | Mbit/s out each | total out | RcvQ:w | pod | rig | RSS | threads | skips | lost |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 0 | 0.73 | - | - | 8 % | 0.7 | 0.4 | 0.66 GiB | 234 | 0 | 0 |
| 550 | 0.75 | 0.88 | 484 Mbit/s | 8 % | 1.0 | 0.6 | 1.14 GiB | 229 | 0 | 0 |
| 1050 | 0.75 | 0.87 | 914 | 8 % | 1.1 | 0.6 | 1.11 GiB | 228 | 0 | 0 |
| 1550 | 0.75 | 0.86 | 1333 | 8 % | 1.2 | 0.5 | 1.26 GiB | 228 | 0 | 0 |
| **2050** | **0.75** | **0.87** | **1791** | **8 %** | **1.3** | **0.5** | **1.32 GiB** | **228** | **0** | **0** |

**Two thousand readers cost about six tenths of a core and seven hundred megabytes, and ingest did not
notice**: 0.75 Mbit/s per stream and the receive worker at 8 % in every row. Nobody fell behind - no
subscriber overflowed, so no viewer skipped to live - and every reader was getting the whole stream,
0.87 of the 0.87 Mbit/s on the wire. 1.79 Gbit/s of fan-out.

Three caveats, in the order they matter:

1. **There is no socket in it.** The relayed readers are in-memory test-server reads. What is measured
   is the subscription, the muxer and the copy; what is not is Kestrel's write path, a real TCP
   connection, or an SRT egress socket per viewer. Whatever the ceiling for readers is, it is in the
   part this rig replaces, and finding it needs a rig outside the process.
2. **Their drain runs in the same process**, so the `pod` column in those rows is a few percent
   generous to the rig's side of the ledger.
3. **The direct route was sampled, not swept.** 300 real players on the consumption port
   (`LIVE_SCALE_READERS=0 LIVE_SCALE_DIRECT_READERS=300`) cost the pod 1.1 cores against 0.8 with
   none, took the receive worker from 8 % to 17 % - the consumption port has a receive worker of its
   own, for the handshakes and the ACKs coming back - and the rig 1.4 cores for the thirty player
   processes. Ingest stayed at 0.75 Mbit/s per stream, nothing lost.

## A finding that was not the question

**A viewer does not hold a thread, although the code says it should.** 300 concurrent direct viewers
moved the process's thread count by nothing at all: 231 with none, 230 with three hundred.

`LiveConsumptionService.OnAccepted` starts serving with
`Task.Factory.StartNew(() => ServeAsync(...), TaskCreationOptions.LongRunning)`, and `ServeAsync` is
`async`. The dedicated thread that `LongRunning` asks for therefore lives until the first `await` -
the registry read on the first line of the loop - and then exits; everything after it runs on the
thread pool, which is what `.NET TP Worker` at 72 % in the reader rows is. The ingest side is the
other way round: `Feed` is a synchronous method, so those threads are real, and two per stream is
what the 429-threads-at-200-streams row is made of.

Which way this should be fixed is a decision rather than a defect:

- **Leave it, and say so.** It is why five hundred viewers cost no threads, and a pool continuation
  per idle viewer is the better shape at this scale. The flag is then misleading and the comment
  above it should say what actually happens.
- **Or make it deliberate**: `Task.Run` for the viewer path, and keep `LongRunning` where the delegate
  is synchronous. One OS thread is currently created and thrown away per accepted viewer, which is
  wasted work on a reconnect storm - the exact moment a pod is already busy.

## Sizing, for this machine and this shape

| per-stream bitrate | streams per replica, four cores | bound by |
| --- | --- | --- |
| 0.8 Mbit/s | 200 measured clean, more untested | nothing yet at 200 |
| 4 Mbit/s | about 100 | processor, in the demux threads |
| 4 Mbit/s, 4 GiB limit | about 150 | memory, before the processor |

Readers do not enter that table: at the fan-out they are nearly free, and at two thousand of them
against a hundred streams the constraint is still ingest.

## Not measured

- **The socket path for readers**, as above. It is the one thing between this rig's numbers and a
  real replica's, and it needs a load generator in another process or another pod.
- **More than 200 streams of anything.** The rig's senders become the limit before the service does
  at the light bitrate, and the box runs out of cores at the heavy one.
- **Anything across two replicas.** Every figure here is one pod holding every stream and every
  viewer, so nothing here touches the relayed route as it is actually used, which is a viewer that
  reached the wrong pod.
- **Multi-port ingest.** `Live__IngestPortCount` is the answer to a saturated receive worker, and the
  receive worker was never the limit here, so there was nothing for it to fix. The open TODO to
  re-measure it on a real node still stands.

# Part two: the work the first rig was not doing

Everything above measures ingest and fan-out, which are the two cheapest things this process does:
they move bytes and never decode. The first version of this page therefore reported a replica coasting
at a third of its cores, and the reaction it deserved was the one it got - that cannot be the whole
story. It was not. A preview is a decode per keyframe and a JPEG per interval, per stream, always on.
A snapshot is a container muxed, spilled to a file, opened, decoded and encoded, per request. A
recording is a muxer, parts on disk, objects in storage and a document write, per stream. None of that
was in the measurement.

`LIVE_SCALE_WORK` is on by default now, and the rig's second half audits previews and then asks for a
snapshot and a recording of every stream at once, at whatever load the ramps ended on.

## What is healthy, measured rather than assumed

At 200 streams with 50 real players attached:

| | measured |
| --- | --- |
| previews | 200 of 200 streams hold a picture; 40 of 40 sampled changed within 5 s |
| wall of 200 tiles, fetched at once | 0.04 s median, 0.04 s slowest |
| 200 snapshots at once | 0.38 s median, 0.69 s p95, 0.77 s slowest, 0.8 s for all of them |
| snapshot outcomes | 200 stored - no fallback to the harvester's older picture, none empty |
| 200 recordings started at once | 0.13 s median, 0.20 s slowest |
| 200 recordings running | all 200 on the gauge, ingest unchanged |
| documents | 200 of 200 stored, 1042 MiB, the last arriving 18.4 s after they were measured running |
| ingest throughout | 0.74 Mbit/s per stream, no loss, no kernel drops |

**Previews are working, and they are inside a 1.4-core total.** Every one of the 40 sampled previews
changed within five seconds, which is worth having because they fail quietly by design: the decoder's
own subscription is skip-to-live and `JpegEncoder.Encode` returning null leaves the old picture in
place, counting nothing, so a stale preview is invisible in every figure the service publishes. The
check catches a frozen preview; a harvester running at half its configured cadence passes it, so
"working" here means "not frozen" rather than "on time".

What this does **not** establish is the comparison it is tempting to draw. `baseline.md`'s third defect
called the always-on preview the dominant cost in the whole design; this rig never turns previews off,
and their decode and JPEG work lands in the `.NET TP Worker` bucket with everything else, so nothing
here attributes any particular share to them. "Part of a 1.4-core total" is all that was measured. A
preview-off run against a preview-on run at the same load is the missing measurement, and it is cheap:
`Live__PreviewIntervalSeconds` is already an option.

**Capture work does not disturb ingest.** Two hundred simultaneous decodes and two hundred
simultaneous recordings left the delivered rate and the loss counters untouched, because each feed has
its own thread and its own buffer. That is the design working.

## The 76-second snapshot, and why it was mostly the rig

One configuration produced something much worse. With **500 relayed readers** attached, the same 200
snapshots took **23.5 s median, 50.1 s p95, 76.4 s for all of them**, the process's thread count went
from 428 to 926, the readers' throughput collapsed from 0.87 to 0.02 Mbit/s with 2,638 subscribers
skipping to live, and the last recording's document arrived 128 s late instead of 18 s.

It is worth writing down what that was, because the obvious reading of it is wrong:

| readers during the 200-snapshot storm | median | slowest | threads | document tail |
| --- | --- | --- | --- | --- |
| 50 real players, no relayed | 0.38 s | 0.77 s | 428 | 18.4 s |
| 300 real players, no relayed | 0.60 s | 1.17 s | 430 | 18.7 s |
| 500 relayed + 50 real players | 23.51 s | 76.35 s | 926 | 127.9 s |

**The relayed readers in this rig drain inside the service's own process**, as thread-pool work, so
when the pool filled with snapshot decodes their drains were delayed, their response pipes filled, the
synchronous writes feeding them blocked, and the pool injected about one thread per stalled reader -
926 threads is 428 plus the 500 readers. In a real deployment those readers are other pods behind
kernel sockets and that particular feedback loop does not exist. **So the 76 seconds is substantially
an artifact of the measuring apparatus, and this page is not claiming a 76-second snapshot latency.**

What it did do is point at a mechanism, which then reproduced on purpose and without the artifact.

## The real one: a viewer that does not drain holds a thread

```
LIVE_SCALE=1 LIVE_SCALE_STREAMS=100 LIVE_SCALE_READERS=100,200 \
  LIVE_SCALE_SLOW_READERS=200 LIVE_SCALE_SLOW_DELAY_MS=3000
```

A slow client is not a hypothesis; it is a phone on a bad network. These readers pause three seconds
between reads, which is what that looks like from the pod.

| relayed readers, all slow | their rate | pool threads | process threads | skips | ingest |
| --- | --- | --- | --- | --- | --- |
| 0 | - | 12 | 234 | 0 | 0.73 Mbit/s |
| 100 | 0.17 of 0.87 Mbit/s | 118 | 341 | 0 | 0.75 |
| 200 | 0.17 of 0.87 Mbit/s | 219 | 441 | 0 | 0.75 |

**One thread-pool worker per slow viewer, and nothing bounds it.** The pool went from twelve workers
to two hundred and nineteen, tracking the slow readers one for one - **an in-process figure**, since
these readers are test-server reads rather than sockets, and with a real socket the kernel buffer and
Kestrel's pipe decide where the write blocks and therefore how many threads are held at a given client
speed. The mechanism is route-independent and the count is this apparatus's. It is one for one because
`LiveStreamCoordinator.Serve` writes to a viewer through `PacketMuxer` synchronously - libav's muxer
has no asynchronous write callback, which is exactly why the relay route sets
`AllowSynchronousIO = true` - so a consumer that will not take the bytes does not make the viewer fall
behind, it holds the thread that was writing to it.

**And the protection that exists did not engage.** `skip` is zero in every row: not one subscriber
overflowed. `Live:ViewerQueuePackets` is 2,000 **packets**, and a packet here is a video frame, so at
25 fps that queue is **eighty seconds deep**. A viewer eighty seconds behind live is not a viewer any
more, and until it gets there the skip-to-live policy has nothing to say while a thread stays held.
The depth is in the wrong unit for the job: what a live viewer's queue wants to be measured in is
seconds of media, and `RecorderQueuePackets` at 20,000 has the same shape for the same reason (800
seconds at this frame rate, where the intent was "enough that truncation is genuinely rare").

Ingest never noticed any of it, again: 0.75 Mbit/s per stream, the receive worker at nine percent.

**The direct route is not the same and was not measured.** A player on the consumption port is written
to by `SrtSocketStream.Write`, which is `srt_sendmsg` in libsrt's blocking mode - but SRT is a live
protocol and drops packets it can no longer deliver in time rather than blocking its sender
indefinitely, so a slow SRT viewer should lose picture where a slow relayed viewer holds a thread.
That is code reading rather than measurement, and it is worth measuring: `ffmpeg -readrate 0.3` is a
slow player, and this rig can hold three hundred of them.

## Bottlenecks, ranked by what to do about them

1. **A slow relayed viewer costs a thread, unbounded.** Measured: 219 pool workers for 200 slow
   viewers. It bounds viewers per pod by threads rather than by bandwidth, and it is the relay route,
   which in a cluster is how every viewer that reached the wrong pod is served. Wanted: a bounded set
   of writers with a queue that drops, or an asynchronous write path with the muxer writing into a
   buffer, or a cap that disconnects a viewer that cannot keep up. Any of the three beats a thread.
2. **The viewer and recorder queues are sized in packets, so their real depth is a frame rate away
   from whatever was intended.** Eighty seconds of lag before a live viewer is skipped forward.
   Seconds of media is the unit that matches the intent.
3. **Nothing bounds concurrent snapshot decodes.** `LibavMediaAnalyzer.OnAFileAsync` spills each
   request to a temp file and runs libav in `Task.Run`, with no valve: 200 at once is 200 temp files
   and 200 decodes competing with everything else on the pool. It is cheap per snapshot here - about
   15 ms - so it passes at this scale and would not at a thousand streams with a detector triggering
   captures. A semaphore the width of the processor count would make the same work take the same time
   without the queue.
4. ~~**The document tail is about eleven a second.**~~ **Withdrawn: the figure was measured from the
   wrong anchor.** The 18 s came from a stopwatch started after two full measurement windows - thirty
   seconds into a forty-five-second recording - so at least fifteen of those eighteen seconds were the
   recordings still running, and the actual tail after the last recorder stopped was about **three and
   a half seconds for two hundred documents**, or sixty a second rather than eleven. There is no queue
   here worth reporting at this scale, and the attribution to LiteDB serializing writers was inference
   stacked on a number that was mostly `Task.Delay`. The rig now anchors on the active-recordings gauge
   reaching zero; the 127.9 s figure in the artifact table above carries the same offset and the same
   correction.
5. **Ingest at camera rate**, from part one: the per-stream demultiplexer threads, 100 to 150 streams
   per four cores.

## What is still not measured

- A slow **direct** player, per above.
- Detection, KLV and forwards, none of which the rig touches. Detection in particular raises the
  decode rate, which is the one thing that would make the frame tier the dominant cost rather than a
  third of it.
- The socket path for relayed readers, still: they are in-memory, which is the same caveat as part one
  and the reason the 76-second row is presented as an artifact rather than a result.

# Part three: after the fix

Measured on the branch that became #19, same machine, same rig, same invocations as the runs above.

## The slow-viewer signature is gone

```
LIVE_SCALE=1 LIVE_SCALE_STREAMS=100 LIVE_SCALE_READERS=100,200 \
  LIVE_SCALE_SLOW_READERS=200 LIVE_SCALE_SLOW_DELAY_MS=3000 LIVE_SCALE_DIRECT_READERS=0
```

| slow relayed readers | pool workers before | pool workers after | process threads after | skips per window after |
| --- | --- | --- | --- | --- |
| 0 | 12 | 11 | 233 | 0 |
| 100 | 118 | **7** | 230 | 200 |
| 200 | **219** | **7** | 229 | ~500 |

One worker per slow viewer, gone. Ingest never noticed either run: 0.75 Mbit/s per stream, the receive
worker at 8–9 %, nothing lost, no kernel drops.

**And the skips arrived, which is the other half of the claim.** They were zero before only because the
queue was eighty seconds deep; at four seconds a reader taking a fifth of the stream exhausts it and
skips forward. The rate is about 2.5 skips per reader per fifteen-second window — one refill cycle
every six seconds or so, which is what a 4 s queue draining at a 0.7 Mbit/s deficit arithmetically
gives. The review predicted "once per GOP", roughly ten times more; the mechanism it described was
right and its repeat rate was not. Every reader stayed attached and kept receiving throughout, so
these are viewers skipping repeatedly rather than viewers being dropped.

## The healthy majority is untouched

The default sweep, 200 streams then readers to 550, all of them draining promptly:

| streams | readers | Mbit/s in | of rig | Mbit/s out | RcvQ:w | pod | RSS | pool | skips |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 200 | 0 | 0.74 | 100 % | – | 14 % | 1.0 | 1.53 GiB | 6 | 0 |
| 200 | 200 | 0.75 | 101 % | 0.87 | 14 % | 1.2 | 2.07 GiB | 6 | 0 |
| 200 | 550 | 0.74 | 101 % | 0.87 | 14 % | 1.3 | 1.95 GiB | 6 | **0** |

Every reader at the full wire rate, **not one skip**, ingest and processor unchanged against part one
(1.3–1.4 cores either way). Resident memory is up by roughly 100–200 MB at 550 readers, which is the
per-viewer buffer at its 64 KB starting size plus whatever a burst grew it to — the one cost the write
path added.

Capture work at that load, with #17's decode valve now in place: **200 snapshots in 0.5 s** (0.27 s
median, against 0.38 s median and 0.77 s slowest before the valve existed), 200 recordings all
stored, 1047 MiB, **no truncations** — which is the recorder revert verified at scale rather than
argued. The thread pool queued up to 1,025 items during the snapshot storm and stayed at twelve
workers, so the queue drains rather than starving.

## What this run does and does not settle about `ViewerQueueSeconds`

It stays at **4**. At that depth 550 healthy readers skipped nothing, so there is no measured reason
to raise it.

The review's argument for 6–8 s — that four seconds is two GOPs at a common keyframe interval, and
thin against a GC pause, a retransmit burst or a mobile handover — is **not refuted by this run,
because this rig cannot produce jitter**. Its readers are in-process and drain promptly or not at all.
A real network's badly-timed four-second stall is exactly the case the rig has no way to generate, and
the honest position is that the default is the value measured clean here, the argument for a larger
one is untested, and the option is the dial. Testing it properly needs a reader that stalls in bursts
rather than steadily, which is a rig change, not a code change.

## Corrected earlier, and worth restating here

The document tail now reads **0.0–0.1 s after the final recorder stopped** in both runs, measured from
the corrected anchor. That is the independent confirmation that the withdrawn finding in part two was
an artefact of where the stopwatch started: there was never a queue of document writes.
