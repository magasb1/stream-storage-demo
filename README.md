# Storage Demo

A test of one idea: **can the same code, unmodified, run on a bare laptop with no infrastructure
and scale out across a multi-node Kubernetes cluster** — with nothing but configuration telling it
which world it's in?

Nothing in the application or domain code knows whether a file lands on disk or in a bucket,
whether metadata lives in LiteDB or PostgreSQL, whether a live stream's registry is a dictionary in
this process or a Redis hash every replica reads. Every one of those decisions is behind an
interface, chosen by configuration, and proven identical on both sides by running the same tests
against every implementation.

Four abstractions carry that test, two of them for documents and two for live streaming - added to
see whether the same idea survives a much harder problem: a stream that arrives over a socket, not
a request, and that other replicas have to know about within a couple of seconds of one pod
claiming it.

- **Documents** — upload, store, list, download, delete, with pluggable file storage and database
  providers.
- **Live video** — SRT ingest and low-latency playback, a gateway for pulling and forwarding
  streams, and object detection, sized to run as one process on a Windows desktop with no GPU and
  to scale to hundreds of concurrent streams across many Kubernetes replicas with GPU workers.

```
                                       ASP.NET Core API
                    gRPC :5080          REST :8080          SRT ingest :9000 / play :9010
                        |                   |                          |
                        +-------------------+                          |
                                  |                                    |
                                  v                                    v
                          DocumentService                   LiveStreamCoordinator
                          /              \                    /                 \
                         v                v                  v                   v
                 IFileStorage   IDocumentRepository  ILiveStreamRegistry   ILiveSourceStore
                      |                   |                   |                   |
                +-----+-----+       +-----+-----+       +-----+-----+       +-----+-----+
                v           v       v           v       v           v      v           v
             Filesystem     S3   LiteDB   PostgreSQL  InMemory     Redis  File        Redis
```

Same shape, harder reason. A document read needs no coordination between replicas, so an in-memory
dictionary and a Redis hash behave identically and the choice is only about where the bytes should
live. A live stream's registry entry is how every *other* replica finds out a name is claimed at
all - in the clustered shape that has to be shared state, where the standalone shape gets away with
a dictionary because there is only ever one replica to ask. `ILiveStreamRegistry` is what lets
`LiveStreamCoordinator` not know which of those two worlds it's running in.
`ILiveSourceStore` answers the same question for configuration instead of a live reading - the pull
sources and forwards an operator set up, which a JSON file remembers alone and Redis remembers for
the whole cluster.

## The four runtime combinations

| Scenario | Runtime | File storage | Database | Persistence needed |
| --- | --- | --- | --- | --- |
| A | Developer Windows | Filesystem | LiteDB | Local disk |
| B | Developer Linux | Filesystem | LiteDB | Local disk |
| C | Docker Compose | SeaweedFS (S3-compatible) | PostgreSQL | Container volumes |
| D | Kubernetes | AWS S3 | PostgreSQL | None in the pod |

The two providers are chosen independently, so `Filesystem + PostgreSQL` and `S3 + LiteDB` work
too. Only the configuration changes between scenarios, never the source.

## Run it

```bash
# A and B: no infrastructure required
dotnet run --project src/StorageDemo.Api
# gRPC on :5080, REST and Swagger on :8080

# C: containers, SeaweedFS and PostgreSQL, api and the detection worker both included
docker compose -f docker/docker-compose.yml up --build
# gRPC on :5081, REST on :8081, SeaweedFS S3 on :9000, admin UI on :23646
# The first build also exports RF-DETR Nano to ONNX inside the worker image (~5-10 min, one-time
# per image); nothing to fetch on the host first.

# D: Kubernetes
kubectl apply -f k8s/           # documents
kubectl apply -f k8s/live/      # and live streaming, layered over the same Deployment
kubectl -n storage-demo port-forward svc/storage-demo 5082:5080
```

Read `k8s/deployment.yaml` before applying it: the image name, the IRSA role annotation and the
`CHANGE_ME` secrets are placeholders, and its demo Redis and PostgreSQL exist so the manifests
apply on a laptop cluster — both should be managed services in a real one.

A desktop client (`dotnet run --project src/StorageDemo.Client`) is included as a thin demo of the
gRPC surface: file browsing and playback, live view with a MISB telemetry overlay, and a **Server**
box that switches between the four scenarios above without restarting.

## Live streaming

An encoder connects over SRT, names itself, and is on air from that moment — nothing is requested
first, and nothing is written to disk unless something asks for it. Full detail, including the
SRT internals, the gateway page for pulling and forwarding streams, and the detection pipeline, is
in [`CONTEXT.md`](CONTEXT.md) and the design notes under [`.scratch/scale-to-1000/`](.scratch/scale-to-1000);
the four pictures in [`docs/video-server.drawio`](docs/video-server.drawio) show the shape of it.
It is off by default (`Live__Enabled`) — switching it on opens a port anyone who can reach it may
push a stream into, so a deployment opts in rather than inheriting it.

## Observability

OpenTelemetry out, Grafana in front, and neither of them load-bearing.

```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.observability.yml up --build
# Grafana on :3000, already signed in, with a dashboard each for live streaming and the API
```

Everything this service measures is a plain `System.Diagnostics.Metrics` instrument, so
`dotnet-counters monitor -n StorageDemo.Api --counters StorageDemo.Live,StorageDemo.Api`
reads all of it with no package and nothing configured. `Telemetry:Enabled` adds an OTLP exporter
to a collector, which is what the Compose overlay above starts, along with Prometheus, Tempo and a
provisioned Grafana. Export is off by default: the meters are how the service measures itself, and
dialling out to a collector should be a decision rather than an inheritance.

Two rules keep it honest at a thousand streams. Nothing is tagged with a stream name — that
question is `GET /api/live`, which lists every stream with its own loss, drop and buffer figures,
where a metric per stream would be thousands of time series retained long after the stream ended.
And nothing is measured on the demultiplexer's thread: the hub counts packets and bytes as part of
publishing them, and the heartbeat reports the interval since it last looked.

What is measured, what each figure actually means, and what to alert on - starting with the kernel
UDP error counter, which is the only signal that saw the 250-stream collapse while every stream
still reported itself healthy - is in [`docs/observability.md`](docs/observability.md).

## Security

- Storage keys are generated, never taken from the client; the filesystem provider refuses any key
  that escapes its configured root.
- Uploaded bytes are served with `nosniff` and a sandboxing `Content-Security-Policy`.
- No credentials in the repository: S3 uses the AWS provider chain (IRSA in a cluster), database
  credentials come from environment or secret references, and a live source URL is held to an
  explicit scheme allowlist, `http`/`https` excluded by default.
- Provider exceptions are translated at the infrastructure boundary, so `AmazonS3Exception` and
  `NpgsqlException` never reach the application layer.

## Tests

```bash
dotnet test
```

One specification per abstraction, run against every implementation, so LiteDB and PostgreSQL — and
the filesystem and S3 providers — are held to identical behaviour. Live streaming is tested the
same way it runs: a real SRT transport, a real demultiplexer, a real viewer. PostgreSQL's contract
tests skip unless `POSTGRES_TEST_CONNECTION` points at a real database.

One of them is a load test rather than a specification. `LiveLoadTests` puts fifty encoders on one
ingest port, opens and closes viewers, snapshots and records every stream at the same time, and
kills ten feeds — five of them with somebody watching — then holds every figure the replica
publishes about itself against what it was actually doing. It runs alone, takes about a minute and
a half, and reports its measurements in the shape `.scratch/scale-to-1000/baseline.md` uses, so a
run is a row in that table rather than a pass:

```
load             50 streams x 0.80 Mbit/s, one ingest port
on air           1.9 s for 50 streams (26.4 accepts/s)
delivered        0.79 of 0.80 Mbit/s per stream (98 % of source, median), 39.3 Mbit/s over 50 streams
meter agreement  39.3 Mbit/s counted by the hub, 39.3 reported per stream
heartbeat        1 ms median, 15 ms slowest, over 38 passes
transport        0 lost and 0 dropped of 84042 packets
kernel udp       0 receive errors over the run
```

That run was four cores. `LIVE_LOAD_STREAMS=250 dotnet test --filter LiveLoadTests` uses the same
test as the rig on a machine with more of them.

Beside it, `LiveScaleTests` is a rig rather than a test, skipped unless `LIVE_SCALE=1`: it ramps
ingest to two hundred streams and then readers to five hundred on top of them, and reports at every
step what each side delivered, what the transport and the kernel lost, and — out of
`/proc/self/task` — which threads by name were spending the machine. What it found on four cores is
in [`.scratch/scale-to-1000/ingest-and-readers.md`](.scratch/scale-to-1000/ingest-and-readers.md):
two hundred streams and five hundred readers is not where this breaks (1.4 cores, 148 Mbit/s in and
437 Mbit/s out), camera-rate ingest starts losing packets between 100 and 150 streams, two thousand
readers cost six tenths of a core, and the thread that runs out first is the per-stream
demultiplexer rather than the SRT receive worker every earlier measurement pointed at.

## Migrations

```bash
dotnet ef migrations add <Name> --project src/StorageDemo.Infrastructure
```

Applied at startup in Compose; run as the Kubernetes Job in `k8s/deployment.yaml` instead, so
replicas never migrate concurrently.
