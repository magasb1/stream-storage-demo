# A slow player on the consumption port, measured

Measured 2026-09-28 on the same four-core cloud container as `ingest-and-readers.md`, with the
service in the test process and the players beside it. Two tests produced everything here and both
are in the suite:

```
dotnet test --filter "FullyQualifiedName~SrtSendPressureTests"   # the transport, on the sending socket
dotnet test --filter "FullyQualifiedName~LiveSlowPlayerTests"    # what it costs the replica
```

The question was #11's: a slow **relayed** viewer used to hold a thread each (219 pool workers for
200 of them, `ingest-and-readers.md` part three), and #19 fixed that route by making the write
asynchronous. The **direct** route was deliberately left blocking inline in `srt_sendmsg`, and #20
followed from reading the code: no `SRTO_SNDTIMEO` is set anywhere, so a slow-but-alive player looked
able to hold its writer for as long as it liked. Nobody had measured either half.

Both halves are below. The short answer is that the ordinary case is safe and the pathological case
is real, and which one connects is the **player's** choice rather than this service's.

## The mechanism, on the sending socket

Two sockets over loopback, no service: one `SrtSocketStream` writing 12.29 Mbit/s exactly as the
consumption port writes a viewer, a peer that never reads a byte, and libsrt's own sending figures
read off the accepted socket. The socket is configured the way `SrtListener` configures one, which
here means 120 ms of negotiated latency and nothing else.

**Peer advertising too-late-packet drop** (libsrt's default, so what an ordinary player does):

| at | written KB | send buffer KB | held ms | dropped | send outstanding | free KB |
| --- | --- | --- | --- | --- | --- | --- |
| 6 s | 9 005 | 8 | 5 | 0 | 0.00 s | 11 989 |
| 8 s | 12 005 | 556 | 359 | 0 | 0.00 s | 11 386 |
| 10 s | 15 006 | 1 439 | 928 | 899 | 0.00 s | 10 412 |
| 14 s | 21 012 | 1 579 | 1 018 | 2 712 | 0.00 s | 10 258 |
| 26 s | 39 017 | 1 581 | 1 020 | 8 274 | 0.00 s | 10 255 |

The buffer settles at **1 020 ms and 1 581 KB** — the drop threshold itself, `max(negotiated
latency + SRTO_SNDDROPDELAY, 1000) + 20` ms — of the **12 000 KB** libsrt says the socket could hold,
and **no send is ever outstanding**. The dropped column is cumulative, so the rate is its slope:
7 375 packets between the tenth second and the twenty-sixth, **461 a second**, against about 1 170 a
second written. The rest went onto the wire and was discarded at the far end instead, which is the
next table but one. That is the whole
reason a slow direct player costs no thread, and it is measured rather than inferred: every figure in
this table is `SRT_TRACEBSTATS` off the sending socket.

**The same peer with its drop flag cleared** (`srt://…?tlpktdrop=0`, one query parameter):

| at | written KB | send buffer KB | held ms | dropped | send outstanding | free KB |
| --- | --- | --- | --- | --- | --- | --- |
| 10 s | 15 004 | 3 143 | 2 027 | 0 | 0.00 s | 8 532 |
| 14 s | 21 007 | 9 273 | 5 982 | 0 | 0.00 s | 1 770 |
| 16 s | 21 054 | 10 880 | 7 023 | 0 | **1.96 s** | **0** |
| 26 s | 21 054 | 10 880 | 7 023 | 0 | **11.97 s** | **0** |

Nothing frees the buffer, it fills in fourteen seconds, and **one send is still inside libsrt twelve
seconds later** — it returned only when the socket was closed under it. `srt_sendmsg` waits on a
condition variable with no timeout because `SRTO_SNDTIMEO` is -1 unless somebody sets it, and a peer
that keeps acknowledging never trips the connection-lost exit either. Since #18 the thread that waits
is a **pool worker**, and the consumption port's `Admit` says yes to everyone by design.

So #20 is not hypothetical and does not close. A caller reaches it with one query parameter, or by
asking for a longer latency, which raises the drop threshold with it because the handshake settles at
the larger of the two sides.

## What ten ordinary slow players cost the replica

One stream at 12.29 Mbit/s, one healthy player as a control, then five and five slow ones
(`ffmpeg -readrate 0.3`), each step watched for thirty seconds and sampled every five.

| at | players | viewers | pool | threads | RSS MB | pod | rig | skips | udp | alive |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| baseline | 0 | 1 | 6 | 31 | 303 | - | - | 0 | 0 | - |
| 30 s | 5 | 6 | 7 | 32 | 323 | 0.13 | 0.07 | 0 | 0 | 5 |
| 30 s | 10 | 11 | 6 | 31 | 349 | 0.31 | 0.15 | 0 | 0 | 10 |
| peak | 10 | 11 | **8** | **33** | - | - | - | 0 | 0 | 10 |

- **Skip-to-live stayed at zero**, which is the sharpest figure here and the one that does not depend
  on thread-count noise: a direct viewer is written to inline, so a send that waited four seconds
  would stall that viewer's drain, fill its four-second subscription and score a skip. Zero skips
  bounds every single write below about four seconds without counting a thread at all.
- **Threads say less than they appear to.** Peak 8 workers and 33 threads against a baseline of 6 and
  31, and attaching players is itself most of that: the peak lands during the attach and decays while
  they are held. The honest claim is *fewer than about half a thread each*, which excludes the
  one-per-viewer shape #4 measured on the relayed route and does **not** establish none. This rig
  cannot tell two threads of growth from zero.
- **Ingest never noticed**: the stream stayed live and arriving at 12.40 Mbit/s, 0 lost, 0 dropped
  and 0 retransmitted on the publisher's own link, 0 kernel UDP receive errors, all eleven viewers
  attached at the end, and the healthy player held throughout and complained about nothing. The
  replica spent 0.31 of a core against the rig's 0.15.

## What the player gets

A reader of our own, taking three tenths and reporting `SrtSocketStream.Health()` on its own socket,
because a viewer socket's figures belong to the service and are published nowhere — `GET /api/live`
carries `SrtLinkStats` for the *publisher's* socket only, which is the other direction on a different
connection.

Each row is the interval since the previous one, cleared on every read the way the heartbeat reads a
publisher's.

| at | took MB | arriving Mbit/s | lost | dropped | resent |
| --- | --- | --- | --- | --- | --- |
| 5 s | 3 | 12.78 | 0 | 0 | 0 |
| 10 s | 5 | 8.60 | 438 | 0 | 0 |
| 15 s | 7 | 1.90 | 4 931 | 0 | 0 |
| 20 s | 9 | 1.90 | 4 894 | 0 | 0 |
| 30 s | 14 | 1.92 | 4 898 | 0 | 0 |

Its receive buffer fills in about ten seconds and then it is handed **1.9 Mbit/s of a 12.29 Mbit/s
stream**, losing 19 959 packets over the window — 665 a second averaged across it, about a thousand a
second once the buffer is full. That is #11's prediction, measured: a slow direct player loses picture
where a slow relayed one used to hold a thread.

Pacing strictly by bytes, such a reader is then disconnected by **its own** library — `SEQUENCE
DISCREPANCY. BREAKING CONNECTION … Reception no longer possible. REQUESTING TO CLOSE.` — which
happened at about thirty seconds in the runs with a longer window, and just after this one ended. The
ffmpeg players pace by timestamps instead and stayed connected throughout, losing picture rather than
the link.

## What this does not settle

- **What a slow viewer costs in memory.** Resident memory climbs while they are served — 4.6 MB per
  player in this run, and between 5.8 and 20.7 MB per player in earlier ones — but ten slow players
  are also ten muxers, ten subscriptions and ten four-second queues, and nothing here separates those
  from the 1.5 MB of send buffer the socket actually holds. The spread across runs is itself the
  argument for not quoting a per-viewer figure from this rig. Treat the resident figure as a slope to be measured
  at scale, not as a per-viewer cost. `LIVE_SCALE_SLOW_DIRECT` exists for that.
- **Anything below this bitrate.** What has to fill is bytes, not seconds: about twelve megabytes at
  each end. At the 600 kbit/s the rest of the suite sends, a slow player is absorbed for minutes
  before the transport is asked anything at all, which is why this pushes 12 Mbit/s and why a test at
  the lower rate would have reported a clean bill of health.
- **Bursty slowness**, still, for the same reason `ingest-and-readers.md` gives: these readers stall
  steadily.

## Where it leaves #11, #20 and #4

**#11 closes.** The code reading was right about the ordinary case and the measurement is in the
suite rather than in a report.

**#20 stays open, re-scoped**: set `SRTO_SNDTIMEO` on accepted viewer sockets and decide what a
timeout means — skip forward, or drop a viewer that cannot take the bytes at all — together with
`SRTO_SNDBUF` and `ViewerQueueSeconds`, because the three are one policy. The coupling is the trap:
any per-viewer send buffer below `bitrate × 1.02 s` (1.5 MB at 12.3 Mbit/s) guarantees the buffer
reaches its ceiling before the drop threshold can free it, and so manufactures this block for every
viewer rather than only for the one that asked for it.

**#4's direct half** is not a thread ceiling for ordinary players. The thread is back only in the
case above, which is #20's.
