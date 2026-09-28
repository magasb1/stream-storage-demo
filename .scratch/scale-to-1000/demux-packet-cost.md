# What one demultiplexed packet costs, off a file

Measured 2026-09-28 on this branch by `DemuxCostBench` (`tests/StorageDemo.Tests/Infrastructure/`,
gated on `DEMUX_COST=1`), which is an instrument rather than a rig. It answers the question
`perf-ingest.md` bounded at 100 streams and `ingest-and-readers.md` then mis-attributed: of the
per-packet cost on the demultiplexer thread, **which part is it**.

Run it with:

```
dotnet build tests/StorageDemo.Tests/StorageDemo.Tests.csproj -c Release
DEMUX_COST=1 DOTNET_ROOT=/home/user/.dotnet \
  ./tests/StorageDemo.Tests/bin/Release/net10.0/StorageDemo.Tests \
  -class StorageDemo.Tests.Infrastructure.DemuxCostBench -showLiveOutput
```

Release matters: in Debug the managed side is unoptimised and a comparison between configurations
that differ only in managed code is not worth having.

## Answer first

| | µs/pkt | at 150 streams (4,500 pkt/s) | share of the pod's 2.1 cores |
| --- | --- | --- | --- |
| **pooling, absolute upper bound** | 3.1 to 5.4 | 0.014 to 0.024 cores | 0.7 to 1.2 % |
| **uninitialised arrays** (no refcount needed) | 0.2 to 0.4 | ~0.002 cores | 0.1 % |
| **read granularity**, 1,316 B against 64 KB | 3.4 to 4.9 | 0.015 to 0.022 cores | 0.7 to 1.0 % |
| the demultiplexer thread's own cost per packet, on a rig | 250 to 300 | — | — |

So **the allocation is about one to two per cent of what bounds ingest density**, and the version of
it that needs no reference count is a tenth of that. Neither is worth a change to the pump. Two
orders of magnitude of margin, which is why this closes rather than defers.

## Why off a file

The rig cannot resolve this. At the collapsed row every thread is scheduler-bound on four contended
cores, and the rig's own repeatability is about ±0.1 core against an effect worth 0.014 - the thing
being measured is an order of magnitude below the instrument's noise. Reading a rendered pattern from
the page cache as fast as the processor allows removes the transport, the pacing and the contention,
and leaves libav's demuxer, the copy and the hub. **These figures are a cost per packet, not a
throughput, and they are not comparable to a rig row.** They are comparable to each other, which is
the whole point.

The pattern is what the rig pushes for a camera-rate stream: `testsrc2=size=1280x720:rate=30` at
4 Mbit/s, 45 s, 22 MiB, read 60 times per replicate. One PES packet per frame: 1,350 packets a pass,
81,000 a replicate, **mean 16,706 B, largest 60,978 B**. Nothing reaches the large object heap in any
configuration, so the 85 KiB threshold is structurally absent from every row and a pool would not
have had to answer for it.

## What the run reports about itself

- **`open+probe` 0.95 to 1.31 ms per pass = 0.70 to 0.97 µs/pkt**, present inside every row. Every
  configuration opens identically for this reason - the same probe budget from `LiveOptions`
  (`analyzeduration` 1 s, `probesize` 1 MiB), container probed rather than declared, which is what
  `StreamDemuxer.Run(string, …)` does. An earlier version of this bench let `pool-max` force `mpegts`
  with libav's 5 s / 5 MB defaults while `full` went through the production open, and charged the
  difference to pooling.
- **`resolution` 1 µs steps.** `Process.TotalProcessorTime` advances in microseconds on this kernel,
  not in 10 ms clock ticks, so over 81,000 packets the clock contributes ±0.00 µs/pkt. **Clock
  resolution is not the limit; run-to-run spread is**, at about ±0.5 µs/pkt within a run and rather
  more between runs - see the two regimes below. Nothing under ~1 µs/pkt is claimed from the rows.
- **`zeroing` 0.19, 0.21 and 0.36 µs** over three runs. See `uninit`.

Processor time is taken for the whole process, not the one thread, because retention is what makes an
allocation expensive and the threads that pay for retention are the collector's. What that tolerates
is other processes on the box; it does not tolerate other threads in this process, which is why each
configuration runs alone on a thread of its own with the heap emptied first.

## The rows, in two collector regimes

Four runs on the same tree and the same box (4 cores, workstation GC, two other agents working).
They fell into two groups, which is itself the most useful thing in the measurement: **runs A and B
saw 10 gen0 and 9 gen1 collections per replicate, runs C and D saw 35 and 34** - the runtime's gen0
budget responding to the machine's memory pressure, not to anything in the test. Every replicate,
µs/pkt of whole-process processor time:

| config | regime 1 (A, B): 10/9 coll. | median | regime 2 (C, D): 35/34 coll. | median |
| --- | --- | --- | --- | --- |
| `floor` | 16.20, 14.98, 14.85, 15.67, 13.97, 14.48 | **14.9** | 14.35, 14.73, 14.71 | **14.7** |
| `copy` | 17.39, 17.40, 17.36, 16.62, 15.78, 16.39 | **16.9** | 16.91, 18.10, 15.53 | **16.9** |
| `full` | 20.90, 17.45, 17.96, 19.04, 17.47, 16.67 | **17.7** | 20.82, 21.40, 21.30 | **21.3** |
| `mirror` | 17.09, 17.69, 18.25, 17.41, 17.35, 17.32 | **17.4** | 21.80, 19.57, 19.50, 20.14, 20.32, 21.24 | **20.2** |
| `uninit` | 17.64, 18.13, 17.54, 17.64, 17.52, 18.07 | **17.6** | 20.74, 21.08, 20.55, 21.00, 19.24, 19.73 | **20.6** |
| `pool-max` | 14.28, 13.97, 14.03, 15.05, 14.32, 14.64 | **14.3** | 14.95, 14.99, 14.26, 14.74, 14.74, 15.00 | **14.8** |
| `avio-64k` | 18.30, 16.93, 16.74, 18.12, 16.98, 16.29 | **17.0** | 19.26, 17.92, 17.55, 23.71, 20.10, 18.70 | **19.7** |
| `avio-1316` | 22.12, 21.44, 21.79, 20.43, 22.04, 23.69 | **21.9** | 23.03, 23.10, 22.50, 23.83, 22.93, 23.10 | **23.1** |

GC pause per replicate went from ~47 ms (3.2 % of processor) in regime 1 to ~170 ms (10.3 %) in
regime 2. Run C's `floor`, `copy` and `full` rows were not captured; everything else is all four runs.

Allocation per packet is identical across regimes: `floor` 0.1 B, `copy` 16,734 B, `full`/`mirror`/
`uninit` 16,817 B, `pool-max` 84 B, and the two `avio` rows 16,817 and 16,820 B.

What the configurations are: `floor` is `av_read_frame` and nothing else; `copy` adds the `byte[]` and
the copy, retained by nobody; `full` is the real `StreamDemuxer` into a real `StreamHub` with the
production 30 s buffer and no subscribers, which is the ingest-only shape the rig's collapsed row was
measured in. `mirror`, `uninit` and `pool-max` are **one duplicated copy of the pump's loop run three
times, differing in the single line that obtains the array and in nothing else** - which is what
makes the allocation the only variable. `full` and `mirror` do the same work by different routes and
agree within the spread in regime 1 (17.7 against 17.4) and in regime 2 (21.3 against 20.2): that is
the control saying the duplicate is faithful, and every claim below is a comparison among the three
variants rather than against `full`.

## What the two regimes show

**The cost is the retention, not the allocation.** `copy` allocates exactly the same 16.7 KB per
packet as `mirror` does and is **identical across the two regimes** (16.9 against 16.9), because it
retains nothing and every array dies in gen0. `floor` is likewise unmoved (14.9 against 14.7), and so
is `pool-max` (14.3 against 14.8), which allocates nothing at all. Every row that *keeps* its packets
for the 30 s window got 2.5 to 3.6 µs/pkt worse when the collector started running 3.5× more often.

That is the mechanism the whole question turns on, and it is why there is no cheap version of this
change: what costs is that the buffer holds the array, so anything that recovers the cost has to know
when the last holder is done with it - which is the reference count, with everything that follows from
it. It is not the `new byte[]` itself.

**Pooling's ceiling is 3.1 µs/pkt in regime 1 and 5.4 in regime 2** (`mirror` − `pool-max`). Within
each regime the two configurations' ranges do not overlap across six replicates, so the effect is
real in both. `pool-max` rents and returns the array *immediately*, while the rolling buffer and
every subscriber still hold it - the corruption this whole question is about - so it never starves,
never counts a reference and never waits for the last holder. Whatever it gives back is therefore
strictly more than a correct pool could. It also removes GC entirely at this live set: 35 gen0 + 34
gen1 and 170 ms of pause become zero.

**Skipping the zeroing is worth nothing in the pump, and 0.2 to 0.4 µs really.** `uninit` -
`GC.AllocateUninitializedArray<byte>(packet->size)`, which needs no reference count, no oversize and
no change of contract - is **never faster than `mirror` in any of twelve replicates** across both
regimes (17.6 against 17.4, then 20.6 against 20.2). That null result is not the instrument failing to
see a real effect: measured in isolation, with nothing in the loop but the allocation and a copy of
the same length, a 16,734-byte array costs 1.31 to 3.01 µs zeroed against 1.10 to 2.82 µs
uninitialised - **a difference of 0.19 to 0.36 µs**. The zeroing is nearly free *here specifically*
because the copy overwrites the same cache lines microseconds later, so the memset dirties lines the
copy was going to dirty anyway. `perf-ingest.md`'s Experiment 1 reported "Nothing moved" for this
change on a rig that could not have resolved it; it turns out to have been right, and this is the
instrument that can say so. 0.3 µs/pkt at 150 streams is 1.4 ms of processor a second, 0.0014 of a
core. **Not worth a commit.**

**The read granularity costs 3.4 to 4.9 µs/pkt**, about what the entire allocation costs. `avio-1316`
against `avio-64k` is the cleanest pair here - one argument differs - though `avio-64k` is the
noisiest row in the set (16.29 to 23.71 across all runs). 1,316 bytes is libsrt's default payload
size, and `SrtSocketStream` passes one message up per read, so a real ingest reads a 16.7 KB frame in
about 13 hops where this reads it in one. That figure is only what the hops cost *on this side of the
P/Invoke*, over a `FileStream`; a real ingest pays it into a locked libsrt receive buffer instead,
which is where `perf-ingest.md`'s trace puts most of the thread. The interest is the mechanism, not
this number.

**The bucket rounding costs 23.8 % more memory.** Identical in all four runs, from the run rather
than from a rounded column: **1,353,200,160 B of media held in 1,675,100,160 B of rented array,
×1.2379.** An earlier version of this note said 27 %, which was an estimate read off two figures
rounded to 0.1 GiB - wrong, and the reason the bench now prints both totals. The sign is what
matters: the rig found memory rather than processor is what a pod runs out of first above ~150
streams, and pooling makes memory worse.

## What the rig figure is

The demultiplexer thread spends **250 to 300 µs on one packet** on the rig. That bracket is
`perf-ingest.md`'s 0.7 % of a core per stream at 25 pkt/s (≈280 µs) and `ingest-and-readers.md`'s 1.79
cores over the collapsed row's packet rate (≈300 µs). The 470 µs quoted earlier in this branch was
pod-wide rather than per-thread and should not be compared with a per-thread cost. Note also that the
rig's packets were 2,540 B where this bench's are 16,706 B - 6.6× larger - so the two are the same
order but not the same measurement.

## Where the time actually goes, cited properly

`perf-ingest.md` Experiment 3 splits the demultiplexer thread group by user against system time from
`/proc/1/task/*/stat`: **`.NET Long Runni` is 37.4 % user against 31.5 % system, a 46 % system
share**. Within it, the thread-time trace puts `StreamDemuxer.Pump` exclusive (libav's TS parsing) at
7.4 % of samples, **`SrtSocketStream.Read` at 68.9 %**, `StreamHub.Publish` at 0.31 %, the allocation
at 0.49 % and the copy at 0.12 %.

Three cautions, because this branch has already once cited these wrongly:

- The often-quoted "69 %" in that note is **`SRT:RcvQ:w2`'s system-time share** - a different thread
  group - not anything about the demultiplexer thread.
- The 68.9 % is a share of **wall-clock samples, blocked or running**, so it includes time asleep in
  `srt_recvmsg` waiting for the next message. It is not 68.9 % of processor time.
- 0.49 % is the allocation's share of samples on a rig whose packets were 6.6× smaller than this
  bench's. Right order of magnitude, not a figure to subtract from anything.

## Caveats

- **A file is not a socket.** 17.4 µs/pkt here against 250–300 on the rig. The gap is attributed to
  the transport read on the strength of the trace above and of `avio-1316`, not measured directly.
- **The live set is one hub, ~15 MB.** A pod at 150 streams holds ~4 GB. The two regimes above are
  the closest this bench comes to bracketing that, and they say the ceiling moves with how hard the
  collector runs: 3.1 µs when it ran 10 times a replicate, 5.4 when it ran 35. A pod's live set is
  larger again, so a rig run is the only way to settle the top of that range - it would have to rise
  **50×** from the worse of the two to reach a tenth of what the demux thread spends.
- **No subscribers.** `Publish` does more work with viewers, recorders and KLV attached, which lowers
  the allocation's share further. Conservative.
- **The zeroing result is specific to this shape** - an array allocated and then immediately
  overwritten in full. It is not a general claim about `AllocateUninitializedArray`.
