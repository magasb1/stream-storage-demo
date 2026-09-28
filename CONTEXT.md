# Context

The domain language of this repository. Terms here are used precisely, in code and in conversation.

## Live streaming

**Stream.** A named live feed. The name is the identity, not an incidental label: a feed that drops
and reconnects under the same name is the same stream resuming, not a second one. For an automatic
stream the name arrives in the SRT streamid.

**Automatic stream.** One that nobody announced. An encoder connects to the ingest port, names itself
in the streamid, and the stream exists from that moment.

**Manual stream.** One created by a request before anything connects, for protocols that cannot name
themselves. It is named at creation. Once a demultiplexer exists it is indistinguishable from an
automatic stream.

**Hub.** The per-stream fan-out point. It owns the rolling buffer and the set of subscribers, and
packets flow one way through it. One demultiplexer feeds one hub.

**Packet subscriber.** Something attached to the hub receiving demultiplexed packets, filtered by
stream index, with no decoding. The recorder and the viewer are packet subscribers, and so is the
KLV extractor, on the metadata stream index alone.

**KLV extractor.** The packet subscriber that is always attached, the metadata twin of the
harvester. It keeps a short ring of the newest MISB KLV packets, each with its presentation time
on the reference clock when the carriage gave it one (ST 1402 synchronous) and flagged as
timestamp-only when it did not, and decodes the ST 0902 minimum set out of ST 0601, including the
ST 0102 classification marking. Everything else in a packet stays raw. It touches no decoder,
which is what keeps it close to free at a thousand streams. The recorder already captures KLV
because it subscribes to every index, so recordings were complete before this existed; the
extractor is for live access.

**Synthetic track.** A track this replica produces rather than receives, appended to a stream's
layout after everything its sender declared, so every index the sender declared keeps its number.
Its packets go in through the hub like any other, which is what makes them reach viewers, forwards,
recordings, snapshots and the KLV extractor through one path. It declares the rate it will actually
be published at, because an undeclared one falls through to the assumption and silently resizes
every viewer's queue on that stream. It carries a role, so the metadata index can only ever point at
the platform-metadata track.

**Static sensor.** Where a fixed camera is and where it looks, configured on its source rather than
reported by it: position, altitude, true bearing, depression and field of view, with an optional
marking. A mast or perimeter camera has no platform and no INS, so all of that is configuration and
none of it changes, and the service synthesises a valid ST 0601 Local Set from it onto a synthetic
track. A stream that sends KLV of its own gets none: the camera reporting for itself always wins,
and the transition is a layout change a reconnect makes visible rather than a quiet swap.

**Synthesised metadata.** A KLV packet this service built from a static sensor rather than received.
Marked in band as ST 0601 tag 10, Platform Designation, so it survives to any conforming consumer
downstream, and out of band on the KLV sample and the stream, so a client can say "configured
position" rather than "reported position" without parsing a string.

**Detection.** What a detector found in one frame: an identifier, a box in pixels, and what it
thinks the thing is. Detections leave as a MISB ST 0903.4 VMTI local set, one VTarget pack each, on
the same timestamp as the ST 0601 metadata for that frame, so a STANAG 4609 consumer reads them
without being told anything. Detection is per stream and on demand: a toggle set through the
owner, with a rate in detections per second.

**Detection worker.** A separate deployment that produces detections. It lists the streams, claims
one whose toggle is set and which no worker holds by writing its name through the owner, subscribes
to it over the peer view route, decodes only at the detection rate, detects, tracks, and posts each
VMTI frame back to the owner. The claim is a lease, renewed every beat and lost when the worker
stops renewing it; nothing pushes work to a worker. The owner keeps a short ring of the posted
frames beside the KLV ring and serves the newest on both surfaces. It never decodes.

**Frame subscriber.** Something receiving decoded pictures. Exactly one decoder is itself a packet
subscriber and republishes frames, so a stream is decoded once however many things want pictures, and
only at the rate frame subscribers ask for.

**Harvester.** The frame subscriber that is always attached, keeping a stream's preview current.

**Preview.** The picture right now. Distinct from a **poster frame**, which is the picture a fixed
number of seconds into a piece of media. They are different questions, and one interface answering
both is what let a live preview silently serve a fixed frame. See
`.scratch/server-side-ingest/issues/03-sampler-freeze.md`.

**Rolling buffer.** Ten to thirty seconds of recent packets held by the hub. It is the position a
joining viewer starts from, the window a viewer rolls back through, and what a recording reaches back
into when someone presses record.

**Segment.** The unit the buffer holds. It begins at a position a decoder can start from and runs to
the next one, so choosing a segment is how a viewer joins, how a viewer rolls back, and where a
pre-roll is cut. Its length is the sender's keyframe interval, not a setting, which is why the buffer
window moves in coarse steps for some encoders.

**Rollback.** Moving a viewer back inside the buffer window. Bounded by that window, available
whether or not anything is recording, and never served from a recording. Deliberate rollback is a
chosen position; a viewer falling behind by accident is a fault, corrected by skipping forward to
live.

**Viewer.** A packet subscriber serving someone watching. Always at the live edge unless the user has
deliberately rolled back. It never accumulates delay.

**Recording.** A durable capture of a stream, taken because something asked, continuing whether or
not any client is connected. It is written in **segments** of a few minutes, each stored as it
completes so that no pod ever holds the whole thing, and read back as one document: one name, one
size, one download, seekable end to end. The segmenting is the recorder's business and nobody
else's. Not a scrub timeline. Every
recording begins in the buffer, so it starts before the moment it was requested. One runs at a time
per stream; a trigger arriving while one is running extends it rather than starting another. A
recording with a stop time set at the start is what would otherwise be called a clip, and is not a
separate thing.

**Part.** The unit a recording is stored in: a few minutes, muxed to a local file and uploaded as an
ordinary object as it completes, so that no pod ever holds a six hour recording and losing one costs
at most the part in progress. Not a **segment**: a segment is the buffer's unit and the sender's to
decide, a part is ours and is minutes long, and a part holds many segments. Parts are joined on the
way out, so a document made of them has one name, one size, one download and seeks end to end. Where
the boundaries fell is the recorder's business and nobody else's.

**Pre-roll.** The stretch of buffered stream a recording reaches back for, so an event already under
way when it was noticed is still caught. A floor rather than an exact figure, since a recording can
only begin at a position a decoder can start from.

**Trigger.** Whatever asks for a recording. A person pressing record and a detector firing are the
same thing, which is why an automatic detector needs no separate path.

**Snapshot.** A single picture of a stream, taken because something asked, stored as a document. It is
decoded fresh from the newest part of the buffer at full source resolution, not taken from the
preview, which is both smaller and older. Named by stream and wall-clock capture time. Available
while a stream is interrupted, refused once it is gone.

**Ingest port.** Where encoders push. One address, never changing, any replica behind it. Reaching it
lets you push a stream and nothing else.

**Consumption port.** Where viewers pull live streams, over SRT, symmetric with ingest: a player
calls it and names the stream it wants in the streamid, exactly as an encoder names the stream it
is sending. Live only, since finished recordings and snapshots are documents and stay on the API.
Reaching it lets you watch and nothing else. SRT reaches the player and goes no further: the hop
behind it, when this replica is not the owner, is HTTP.

**Owner.** The replica holding a stream's connection. Only the owner has the bytes, so requests
reaching another replica are forwarded to it, and a viewer that landed elsewhere is served by
fetching the stream from it over HTTP and writing it into the viewer's SRT socket. Media never
reaches a player over the API port; between two pods it does, and that is one handshake and one
latency window cheaper than a second SRT hop was. A viewer's own connection is held open across a
change of owner, so what a moving stream costs somebody watching is the gap in the feed rather than
a reconnect.

**Claim.** How a replica becomes the owner of a name, taken on a distributed lock and renewed by the
heartbeat. Names are one namespace shared by automatic and manual streams. A live name is locked:
while `demo` is live and its owner is heartbeating, a second publisher of `demo` is refused at the
handshake rather than taking it over. The name is free again when the feed is interrupted, when the
owner has stopped heartbeating for three beats, or when nobody owns it, and whoever claims it then
resumes the same stream; the replica losing it learns so on its next heartbeat.

**Interrupted.** A stream whose feed has stopped arriving but whose grace period has not expired. It
is still claimed, still shown, and its hub, buffer and any recording are all still alive. A reconnect
inside this window resumes the same stream.

**Grace period.** The thirty seconds an interrupted stream is kept before it is considered gone. Once
it expires the stream leaves no trace, since the documents it produced are its trace.

## Storage

**Document.** A stored file with its metadata. Snapshots and recordings become documents; a stream by
itself never does.

**Storage provider** and **database provider** are chosen independently by configuration, and neither
is visible to domain or application code. That independence is the point of the demo.
