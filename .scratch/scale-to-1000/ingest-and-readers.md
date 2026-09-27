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
  does hold a thread - and at the collapse it is 1.8 cores against the receive worker's 0.24. The open
  TODO about pooling `StreamDemuxer.Pump`'s per-packet `byte[]` is aimed at exactly this thread, and
  this is the measurement that says it is worth doing: ingest scale on a Linux node is bounded by
  demultiplexing cost per packet, not by the accept path or the socket reader.
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
- **Memory is arithmetic on the buffer, as `baseline.md` said.** 7 MB per stream at 0.8 Mbit/s
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
