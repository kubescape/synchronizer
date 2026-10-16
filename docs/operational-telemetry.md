# Synchronizer operational telemetry

This covers the cluster client and backend server. Read the shared
[current implementation guidance](https://github.com/armosec/shared-designs-and-docs/blob/docs/otel-implementation-reality/projects/agent-telemetry-system/research/high-level-requirements.md#current-implementation-october-16-2026)
([update PR](https://github.com/armosec/shared-designs-and-docs/pull/273)). Its May
proposal is historical. **5–15 KB/agent/hour is an unvalidated planning target,
not a measured rate or an enforced upload budget.** Nothing here imposes a byte
quota. Server traffic aggregates many cluster clients; node-agent sizing does
not describe this workload.

## Support questions

- Is synchronization moving? Compare operation rates, transferred bytes and active
  connections across releases/replicas. Bytes are counted at each instrumented
  transport stage, not unique logical object bytes.
- Are clients waiting on the network writer? `dispatch.pending` counts callers
  waiting for the existing unbuffered outgoing channel; dispatch duration shows
  backpressure. `send` includes network writes, retries and reconnect waits.
- What is failing? Operation outcomes distinguish connect, transport, adapter and
  queue failures without creating a series per customer, Kubernetes kind or URL.
- Is work stuck? `operations.stalled` counts in-progress operations older than
  30 seconds, including broker acknowledgements and adapter calls. Normal idle
  socket reads do not count as stalled work.
- Is a reconnect storm or slow backend responsible? Compare connection attempts,
  slow-path spans, send latency, queue acknowledgement latency and suppressed
  diagnostic counts. W3C trace context crosses WebSocket establishment and broker
  message properties, enabling correlation with cooperating services.

## Initialization and configuration

Both binaries use the existing `go-logger/otelsetup v0.0.35` once, before adapters,
loggers and instruments. Discovery finishes before providers are initialized.
There is no second exporter stack and no Prometheus-to-OTLP bridge. The old
client `logger.InitOtel`/Uptrace initialization has been removed.

| Setting | Effective component default / precedence |
| --- | --- |
| Discovery source | Existing backend v3 client URL/parser; `metrics` field. Client and server: discovery only with explicit nonempty `API_URL`; no cloud fallback. |
| Endpoint precedence | Base `disable` (case insensitive, surrounding whitespace accepted) disables all operational signals and skips telemetry discovery. Otherwise an explicitly present base wins, **including empty**. If base unset, nonempty legacy `OTEL_COLLECTOR_SVC` wins; otherwise discover only with `API_URL`. Per-signal OTLP endpoints override an ordinary base. |
| Discovered URL | Scheme-less values get `https://`; explicit schemes remain unchanged, as in the reference normalization contract. Shared setup exports OTLP/gRPC; configure HTTP(S) gRPC collector URLs. |
| Service identity | `synchronizer-client` or `synchronizer-server`; version `RELEASE` from image build. Client cluster/namespace from existing config, overridden by cluster data and `CLUSTER_NAME`; node/pod from `NODE_NAME`/`POD_NAME`. Server namespace from `POD_NAMESPACE`. |
| Authentication | Existing account/access key passed through `ProviderConfig`; when the access key is nonempty, shared setup attaches `X-API-ACCOUNT` and `X-API-KEY` to each signal's RPC. Missing credentials are nonfatal; an explicit collector works unauthenticated. Never instrument incoming server request credentials. |
| Metric interval | `OTEL_METRIC_EXPORT_INTERVAL=60000` ms; 60 s gives operational capacity/troubleshooting visibility without event-rate export. No runtime instrumentation is started implicitly. |
| Trace sampler | `OTEL_TRACES_SAMPLER=parentbased_traceidratio`, `OTEL_TRACES_SAMPLER_ARG=0.1`. Sampled remote parents are honored; local creation gates still apply. |
| Exported log level | Fixed warn-and-above **plus** component scope/body allowlist. Existing local logger name/level still follow `KS_LOGGER_NAME` / `KS_LOGGER_LEVEL`; default remains pretty/info. Selecting debug does not export existing debug/info records. |
| Compression | `OTEL_EXPORTER_OTLP_COMPRESSION=gzip`, supported by the pinned gRPC exporters. Per-signal compressor overrides remain effective. |
| Span creation | Ordinary operations: no span below 1 s. Retrospective slow spans and connection-establishment spans share one process-wide gate: at most one creation per 10 s, before sampling. No connection-lifetime or per-object root spans. |
| Diagnostic dedup | Three fixed record bodies; key = body, window = 60 s across the entire process (all customers/operations). At most three records/minute, independent of workload cardinality. Cache has at most three keys; suppression is counted. |
| Export queue bounds | Trace and log queues: 128 records, batch size 32, nonblocking SDK processors. `OTEL_BSP_*` / `OTEL_BLRP_*` overrides remain effective. |
| Export/retry bounds | OTLP, metric reader, trace batch and log batch export timeouts default to 3,000 ms. SDK transport retries end with the export context; no application retry queue. |
| Discovery bound | One request, at most 3 s (or parent deadline), response limited to 1 MiB, no redirects, no retries. Errors/missing endpoint are nonfatal. |
| Shutdown | Shared combined flush, bounded by 5 s with a fresh background context. Client handles SIGINT/SIGTERM and cancels reconnect backoff; server retains its existing shutdown path. Abrupt termination / existing Fatal exits cannot guarantee flushing. |

Standard timing, sampler, compressor and queue defaults are set only when the
variable is **unset**. Explicit values (even empty/invalid) are left for the SDK
to interpret; invalid SDK settings may fall back to SDK defaults. Inspect the
actual environment when diagnosing effective behavior. Increasing queue sizes,
changing sampling or reducing metric intervals increases cost. Batching changes
request grouping; it is not an ingest quota.

`disable` clears per-signal operational endpoints and the legacy alias as well
as the base. Explicit empty base suppresses discovery/legacy fallback but still
allows explicitly configured per-signal endpoints. Telemetry opt-out does not
skip the client's separate synchronizer-server URL lookup when `API_URL` is supplied. The independent
`OTEL_EVENTS_EXPORTER_ENDPOINT` is never read or changed here.

With no `API_URL` and no base, legacy or per-signal OTLP endpoint, all three
signals remain no-op: no discovery or telemetry exports, and no missing
telemetry-credential warning. Console logging stays enabled. The client retains
its configured synchronizer WebSocket URL when `API_URL` is absent. OSS users
can set `OTEL_EXPORTER_OTLP_ENDPOINT=http://collector:4317` without credentials,
or configure only a per-signal endpoint; other signals remain no-op. Explicit
empty base still suppresses discovery and the legacy alias while allowing
per-signal overrides. These activation rules apply to both client and server.

## Instrumentation inventory

All instruments use scope `github.com/kubescape/synchronizer/telemetry`.

| Instrument | Type / dimensions |
| --- | --- |
| `synchronizer.operations` | Counter: `operation`, `outcome=success,error` |
| `synchronizer.transfer.bytes` | Counter (bytes): same dimensions; positive payload sizes only |
| `synchronizer.operation.duration` | Histogram (seconds): `operation` |
| `synchronizer.operations.active` | Up/down counter: `operation` |
| `synchronizer.operations.stalled` | Up/down counter: `operation`; active for >=30 s |
| `synchronizer.dispatch.pending` | Up/down counter: awaiting writer; no attributes |
| `synchronizer.connections.active` | Up/down counter: running synchronizer instances; no attributes |
| `synchronizer.drops` | Counter: `reason=invalid_message,depth,canceled,send_failed,other` |
| `synchronizer.spans.suppressed` | Counter: creation gate suppressions; no attributes |
| `synchronizer.logs.suppressed` | Observable counter: duplicate diagnostics; no attributes |
| `synchronizer.telemetry.errors` | Observable counter: SDK error-handler invocations; no attributes; not a count of every dropped SDK record |

Operation vocabulary: `http_ingest,connect,receive,send,dispatch,batch,get,verify,delete,patch,
put,mq_produce,mq_receive,other`. All caller strings normalize to this vocabulary.
`receive` measures delivery to the incoming worker pool after the socket read,
excluding idle read time. Adapter handlers measure application processing.
Broker production ends on the asynchronous acknowledgement callback, including
failures; broker consumption measures handling, including reconciliation waits.

Histogram boundaries: **0.001, 0.01, 0.1, 1, 5, 30 seconds** (seven buckets including
+Inf). These cover small patches through network stalls without copying node-agent
rule evaluation buckets. The theoretical inventory bound is 108 OTLP data
points per resource/export if every allowed attribute combination occurs; 14
are histograms. Classic Prometheus histogram expansion would make that 220
series; the observed SigNoz min/max expansion would instead make the theoretical
limit 248. Both exclude resource labels, exemplars, and other instrumentation. Actual
combinations are a subset. Resource cardinality still grows with replicas and
rollouts; bounded dimensions do not bound fleet size.

Spans are named `synchronizer.<normalized operation>` and record fixed error
status text, never raw errors or payloads. Slow spans are created on completion
with the original start timestamp; their context cannot retroactively parent
work already performed. Connection spans cover establishment only. WebSocket
handshake headers and Kafka/Pulsar properties propagate W3C trace context without
baggage. The WebSocket message schema is unchanged: there is no new per-object
trace carrier. Existing asynchronous contexts and cooperating remote services
are required for continuous cross-service traces.

Exported diagnostics have fixed bodies (`synchronizer operation failed`,
`synchronizer operation slow`, `synchronizer work dropped`), one normalized
`operation` attribute, severity and timestamps. OTEL derives trace/span IDs from
the emission context; failures inside an emitted connection/slow span correlate
with that span. Fast failures without a valid parent have no trace ID. Dedup
can retain an earlier uncorrelated record rather than a later correlated one.

A component-local provider facade rejects other log scopes and reconstructs
accepted records before they reach shared SDK processing. Existing logger output
is preserved locally, but its unrestricted fields/errors are not exported.
The shared debug ring flush listener is deliberately suppressed during provider
initialization (the local debug level is restored immediately): direct ring
replay would bypass these gates. The shared bounded 7,500-record ring still
retains accepted, sanitized records; there is no component flush endpoint.
SDK errors print fixed text to stderr at most once/minute and increment the
error counter, without logging endpoints or credentials.

## Live staging verification

The cluster client was temporarily deployed and restored on October 16, 2026.
[Live stage validation](telemetry-stage-validation.md) records same-cluster SigNoz
ingestion, default cadence, controlled failures, cardinality and shutdown evidence.
Those live observations are separate from the local synthetic byte measurements below.

## Deployment evidence and limitations

Checked in this repository:

- `build/Dockerfile` builds client/server and sets `RELEASE`; it does not inject
  API URLs, telemetry endpoints, credentials, namespace/node/pod environment or
  signal overrides. No Helm/Kubernetes deployment manifests are present here.
- `configuration/client/config.json` supplies example account/access-key values,
  cluster/namespace and localhost WebSocket URL. The client retains existing
  `/etc/credentials` overrides and `/etc/config/clusterData.json` loading
  (`CLUSTER_CONFIG` overrides the path). Production credentials must replace
  placeholders. `API_URL` was already the client's discovery input.
- `configuration/server/config.json` (Pulsar) and `server-kafka/config.json` lack
  tenant telemetry credentials and discovery URL. Backend authentication config
  validates **clients**; it is not a server telemetry identity. Server startup
  can load dedicated `/etc/credentials` in the same format as the client, or use
  an explicitly configured internal unauthenticated collector. No customer
  identity is inferred from a connection. Without explicit endpoint or `API_URL`,
  operational telemetry stays disabled on the multi-tenant server.
- `configuration/ingester` config belongs to integration fixtures; this change
  does not initialize another component's providers. Existing Prometheus
  producer metrics and their compatibility aliases remain available unchanged,
  including their historical account/cluster/kind labels. They are **not**
  registered or exported in the OTLP meter provider.

Follow-ups outside this repository: provision a dedicated backend telemetry
identity or internal collector route; verify chart mounts/env, discovery URL,
signal endpoint overrides and rollout; expose processor/sampler/reader options
in shared setup so its ring and debug listener need no startup env adaptation;
review existing Prometheus cardinality; deploy and verify collector admission
limits/quotas. Authentication, retention and container memory limits do not
protect ingest volume.

The linked live report verifies the staging client deployment and collector path.
Production deployment, the backend-server identity, root-sampling statistics and
hourly/fleet bytes remain unverified. Broker queue depths, Kubernetes watch queue
loss, reconciliation scheduling delays and process CPU/memory are not covered by
this inventory. Backend outages lose telemetry when bounded SDK queues fill;
core processing does not wait for exporter RPCs. A stalled operation is visible
while active; a crash can remove its last state before the next export. There is
no byte quota, no fleet quota, and no promise that every error has a span/log.

## Reproducible validation

`go test ./telemetry -count=1 -v` exercises mocked/local discovery precedence,
opt-out, explicit empty values, TLS normalization, errors and cancellation; log
filtering, privacy, dedup, cardinality normalization, histogram boundaries, span
gates, stalled/pending work, shared-provider authentication, trace propagation,
and unavailable collector behavior. It contacts no production services.

`TestLocalOTLPVolume` uses the real shared providers and a localhost OTLP/gRPC
collector. It drives **instrumentation calls**, not a representative customer
traffic replay: 100 and 10,000 completed put operations, 10% failures, one failed
connection. It forces `always_on` sampling to make span-gate verification
deterministic, enables gzip, and flushes once at shutdown. Results include OTLP
protobuf bytes and compressed gRPC payload wire bytes (including gRPC message
framing; excluding HTTP/2 headers, TLS and network overhead). It counts OTLP data
points, spans and logs. Exemplar selection, timestamps and resources cause byte
variation. Test output is the measurement record; these small synthetic batches
cannot establish hourly or fleet ingest rates.

One local run (Go 1.27, gzip, forced `always_on`, one shutdown flush):

| Synthetic operations | OTLP data points / spans / logs | Protobuf bytes (metrics / traces / logs) | Compressed gRPC bytes (metrics / traces / logs) |
| --- | --- | --- | --- |
| 100 | 11 / 1 / 1 | 6,010 / 460 / 472 | 1,243 / 349 / 355 |
| 10,000 | 11 / 1 / 1 | 7,354 / 460 / 472 | 1,462 / 353 / 362 |

These are measured single-batch results, not estimates of hourly ingestion.
Repository short tests, race checks for telemetry/core/messaging/entrypoints,
`go vet ./...`, and `go build ./cmd/...` passed. The full broker integration run
failed at container initialization, including Pulsar reporting “No space left on
device”; those broker checks remain unverified for this change.

Before capacity approval, replay representative object sizes, workload counts,
reconciliations, reconnect storms and backend outages across multiple export
intervals. Measure per-signal bytes at both exporter and collector, active
series, histogram/exemplar overhead, resource churn and collector-generated
span metrics. Record effective runtime configuration and deployed versions.
