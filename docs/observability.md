# Observability

What this service measures about itself, why those figures and not others, and how to look at them.

Run it:

```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.observability.yml up --build
```

Grafana is on <http://127.0.0.1:3000>, already signed in, with two dashboards in a **Storage Demo**
folder: **live streaming** and **API and gRPC**. Prometheus is on `:9090` if you would rather type
queries, and Tempo holds the traces.

## The shape of it

```
   StorageDemo.Api                OTLP :4317        collector           Prometheus        Grafana
   ┌──────────────────┐            ┌──────────────────┐            ┌────────────┐     ┌──────────┐
   │ StorageDemo.Live │ ─ metrics ─▶ otel-collector   │ ─ scraped ─▶ prometheus  │ ◀── │ grafana  │
   │ StorageDemo.Api  │            │  :8889 /metrics  │            └────────────┘     │          │
   │ framework meters │ ─ traces  ─▶                  │ ─  OTLP   ─▶ tempo       │ ◀── │          │
   └──────────────────┘            └──────────────────┘            └────────────┘     └──────────┘
```

The service speaks OTLP and knows nothing else. Replace the collector with Grafana Alloy, point
`OTEL_EXPORTER_OTLP_ENDPOINT` at a collector the cluster already runs, or send it to a hosted
backend, and nothing in the application changes.

**There is no `/metrics` endpoint on the pod, on purpose.** The .NET Prometheus exporter has never
had a stable release — still beta, three years on — and a media service with publicly reachable
ports is the wrong place to take a preview dependency whose job is to open another one. The
collector exposes the Prometheus shape instead, which is where it belongs: a pull target that
Prometheus understands, scraped from somewhere that is not holding a thousand sockets.

**Export is off by default.** `Telemetry:Enabled` is `false` in `appsettings.json`, because switching
it on makes the process dial out every few seconds and a service that does that uninvited is a
surprise in somebody's egress rules. The meters exist either way, and

```bash
dotnet-counters monitor -n StorageDemo.Api --counters StorageDemo.Live,StorageDemo.Api
```

reads every instrument on this page with no package, no collector and no configuration at all. That
is also the fastest way to check a figure while writing code.

Configuration lives in the `Telemetry` section (`src/StorageDemo.Api/Observability/TelemetryOptions.cs`),
and the standard `OTEL_*` environment variables work as they do in any other OpenTelemetry SDK —
`Telemetry:Endpoint` is there for the cases where a configuration file is easier to reach than the
environment.

## Metric names are the OpenTelemetry names

The collector's Prometheus exporter runs with `add_metric_suffixes: false`, so `live.streams.owned`
arrives in Grafana as `live_streams_owned` and nothing else happens to it. With suffixes on — the
Prometheus convention, and a perfectly reasonable default — a counter would gain `_total` and an
instrument declaring bytes or seconds would gain `_bytes` or `_seconds`, so the name in the code, the
name on this page and the name in a panel would be three different strings. The failure that
produces is an empty panel with no error on it, which is the most tedious thing in this stack to
diagnose. Turn the suffixes on if a house convention requires it, and rename the queries in
`docker/grafana/dashboards/` to match.

Every series carries `service_name`, `service_instance_id` and `deployment_environment_name` from the
resource the service declares. **`service_instance_id` is the same name the stream registry records
as a stream's owner** — the configured node name, then `POD_NAME`, then the machine — so "which pod
is this" is one word in both a dashboard query and `GET /api/live`.

## Two rules the instruments follow

**Aggregates only; nothing is tagged with a stream name.** A thousand streams times twenty
instruments is twenty thousand time series, every one retained long after the stream ended, to answer
a question `GET /api/live` already answers per stream for free: it lists every stream with its own
loss, drop, buffer and viewer figures. Every tag on every instrument here has a small fixed set of
values, and **no tag value is ever derived from something a caller sent** — a reject reason taken
from a stream identifier would let whoever is connecting choose this service's cardinality.

**Nothing is measured on the demultiplexer's thread.** Packets and bytes are counted by the hub as
part of publishing them, because it is holding the lock and incrementing two fields anyway, and the
heartbeat reports the interval since it last looked. A counter call per packet per stream would be a
million instrument operations a second at the scale this service is sized for, to produce a number
that is exact either way. The one exception is a subscriber overflowing, which is a fault and
therefore rare, and is counted where it happens.

## Live streaming — `StorageDemo.Live`

What the pod is holding, republished by every heartbeat as one picture:

| Metric | What it says |
| --- | --- |
| `live_streams_owned` | Streams this replica holds the connection for, interrupted ones included. What an autoscaler reads. |
| `live_streams_interrupted` | Of those, feeds that have stopped inside their grace period. A whole census interrupted is senders going away, not streams. |
| `live_streams_unstartable` | No decodable position inside the buffer window: every pre-roll empty, every snapshot second-hand. The fix is at the encoder. |
| `live_viewers` | Players pulling from this replica, direct and relayed alike. |
| `live_recordings_active` | Recordings running here, each writing a document. |
| `live_forwards_active` | Configured copies being pushed to a far end. |

What arrived, sampled per stream per beat and summed:

| Metric | What it says |
| --- | --- |
| `live_packets`, `live_bytes` | What was demultiplexed. As a rate, `live_bytes` is the pod's ingest bitrate. |
| `live_packets_lost` | Never arrived and could not be retransmitted in time: the network, or a receive path past its knee. |
| `live_packets_dropped` | Arrived too late for the latency window, which usually means the window is too small for the link. A different fault from loss, and adding the two together loses what decides the fix. |
| `live_udp_receive_errors` | The kernel's `Udp: InErrors` for the pod's whole network namespace. Linux only. |
| `live_klv_packets`, `live_klv_rejected` | MISB metadata decoded, and packets that were ST 0601 and failed their checksum. |

What happened:

| Metric | Tags | What it says |
| --- | --- | --- |
| `live_accepts` | `port` | Callers accepted, by the port they arrived on. |
| `live_rejects` | `port`, `reason` | Refused at the handshake. `conflict` is the name lock working; `overload` is `Live:MaxStreams` working. |
| `live_claims` | `outcome` | `taken`, `resumed`, `refused`. A rolling update should read as resumed, which is what a move looks like when nothing is lost. |
| `live_streams_ended` | `reason` | `stopped`, `expired`, `source-off`, `displaced`, `shutdown`. |
| `live_viewer_sessions` | `route` | How a player reached the replica that owns its stream: `direct` on that replica's own consumption port, `relayed` when it reached another one and is fetched from there. Counted by the owner, once the viewer is actually subscribed, so one viewer is one session however many pods it passed through and a retrying attach is not a session at all. |
| `live_overflows` | `policy` | A subscriber fell behind. `skip-to-live` is a viewer losing a moment; `fail` is a recorder stopping. |
| `live_snapshots` | `outcome` | `stored`, `preview` — served from the harvester's older, smaller picture — or `none`. |
| `live_recordings`, `live_recorded_bytes` | `outcome` | `stored`, `truncated`, `empty`, `failed`. Bytes are counted as each part is stored, so a six-hour recording reports while it runs. |
| `live_detections` | | VMTI frames the detection workers posted back. |
| `live_heartbeat_duration` | | How long one pass took, in seconds, with buckets straddling the two-second beat. |
| `live_heartbeat_failures` | `stage` | `stream`, `registry`, `sources`. Work the beat swallowed so a Redis blip could not take the ingest port down — which is exactly why it is counted. |

## API and gRPC

Most of this question is already answered by the framework, so the service adds little. ASP.NET
Core's `http.server.request.duration` times and counts **both** surfaces, because a gRPC call is an
HTTP/2 request and appears there with its method as the route; `Grpc.AspNetCore.Server` adds the gRPC
outcomes that a trailer hides from the HTTP view — a gRPC error is an HTTP 200 to everything else
here — under its own pre-conventions instrument names, which is why the series are called
`total_calls`, `current_calls`, `calls_failed`, `calls_deadline_exceeded` and `calls_unimplemented`
rather than anything beginning `rpc_server_`. Kestrel answers for connections and queues, `System.Net.Http`
for every call out of the process, and `System.Runtime` for the working set, the collector and the
thread pool. All of it is registered in `Observability/Telemetry.cs` and none of it is re-implemented.

What is left is what a request-shaped meter cannot see — `StorageDemo.Api`:

| Metric | Tags | What it says |
| --- | --- | --- |
| `api_grpc_streams_active` | `rpc` | Subscriptions open right now. A server stream is one request for as long as a client runs, so a request rate says nothing about it and a duration histogram only reports the ones that already ended. |
| `api_grpc_stream_messages` | `rpc` | Messages written to those streams. Only changes are written, so an idle wall of tiles sends nothing and a rate climbing with the stream count is what the wall costs. |
| `api_documents_uploads`, `api_documents_upload_bytes` | `surface`, `outcome` | Uploads, by `grpc` or `rest`. The byte figure is the stored document's own size. |
| `api_documents_downloads`, `api_documents_download_bytes` | `surface`, `kind`, `outcome` | `kind` is `document`, `thumbnail` or `preview`: a wall of a thousand live tiles and a thousand file downloads are nothing like the same load. Bytes are counted only where this service copies them itself — every gRPC download and a preview; a REST download hands a stream to Kestrel and may serve a range of it, so Kestrel's meter is the honest place for that. |
| `api_peer_calls` | `kind`, `outcome` | The in-cluster hop, which both replicas serve on the same route and which is therefore invisible in a histogram keyed by route. `unreachable` is usually a registry entry naming a pod that has already gone; `no-address` is a missing `Live:PeerBaseUrl`, a configuration fault rather than a network one. |

## Traces

Server spans for every request on both surfaces, client spans for every call out — which makes the
trace worth having the **cross-replica** one: a snapshot asked of the wrong replica, or a viewer
relayed to the owner, is two spans on two pods under one trace id, and nothing else in this service
draws that picture.

Two kinds of request are left out. The health probes, because Kubernetes asks twice a second forever
and the answer is always the same. And the four `Watch` RPCs, because a span describes something that
finished: a client holds one open for as long as it runs, so what would reach Tempo is an hour-wide
span with nothing in it, and it would arrive only once the client had gone. They are measured
instead, by `api_grpc_streams_active` and `api_grpc_stream_messages`, which is the shape that question
actually has. `Telemetry:TraceSubscriptions` turns them on for debugging one client.

`Telemetry:TraceSampleRatio` is 1 in Compose and 0.1 in `k8s/configmap.yaml`. Metrics are unsampled
and answer "how often"; traces answer "what happened to this one", and a tenth of them still answers
it.

## What to alert on

In rough order of how much it means:

1. **`live_udp_receive_errors` rising.** This is the collapse itself rather than a proxy for it:
   every one of those errors is the shared UDP receive buffer overflowing. Measured at three loads on
   one pod: zero over a whole run at 50 streams, 227,135 over a marginal run at 100, and 13,000 a
   second at 250 — where the service reported all 250 streams live and healthy while delivering a
   fifth of the media sent to it. Decide over a window of at least fifteen beats
   (`increase(live_udp_receive_errors[30s])`), because near the knee the figures are noisy and a
   single-beat rule flaps on a pod that is merely marginal. Sustained non-zero means
   `Live:MaxStreams` is too high for the bitrate arriving, and the answer is a lower limit and more
   pods rather than a bigger one.
2. **`live_overflows{policy="fail"}` above zero.** A recording stopped and its document is
   truncated. It is a real failure with a real document behind it.
3. **`live_heartbeat_duration` p99 near or above two seconds.** The beat is what keeps the cluster
   honest; a pass slower than the beat means every other replica is reading a stale registry, which
   is how two pods come to admit one name.
4. **`live_heartbeat_failures{stage="registry"}`.** The handshake is answering from a stale copy of
   what is claimed.
5. **`api_peer_calls{outcome="unreachable"}`.** The cluster is carrying an owner nothing can reach,
   and every caller that lands on the wrong replica is failing.
6. **`live_rejects{reason="overload"}`.** Encoders are being turned away. Correct behaviour, and a
   capacity decision somebody has to make.

Which streams are hurt is not on this page and should not be: that is `GET /api/live`, which names
every stream with its own loss and drop figures over the last beat.

## What is not measured yet

- **The detection worker's meter is not exported.** It publishes one instrument already -
  `live.detection.duration`, the wall time of a detection call by execution provider and batch size,
  which is how a pod that silently fell back from the GPU to the processor becomes visible - and it
  publishes it on this same `StorageDemo.Live` meter. But the worker is a separate deployment
  (`src/StorageDemo.Worker`) with no exporter wired, so today that histogram is readable only with
  `dotnet-counters` against the worker process. Wiring it is the same call and the same two
  environment variables, plus deciding whether a GPU worker's own telemetry belongs on these
  dashboards or its own.
- **Logs** stay on stdout as structured Serilog, for whatever the cluster already collects. Sending
  them through OTLP as well would let Grafana pivot from a span to the log lines inside it; it is one
  sink and one line of configuration, and it is not done here.
- **Exemplars** are enabled in Prometheus but nothing attaches them. It needs trace-metric
  correlation on the .NET side, which is a per-instrument decision rather than a switch.
- **No alert rules ship in this repository.** The list above is the content of them; where they live
  depends on whether a cluster runs Alertmanager, Grafana alerting or something else.
