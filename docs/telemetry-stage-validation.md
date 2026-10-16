# Synchronizer live stage validation — October 16, 2026

PR [#186](https://github.com/kubescape/synchronizer/pull/186) was temporarily deployed
to the **existing cluster client** in `kubescape/synchronizer`, in `armo-dev-stage`.
Operational telemetry reached **SigNoz in that same cluster**, using the endpoint
advertised by backend discovery. No workstation-local telemetry receiver was used.
The original deployment specification and configuration were restored and verified.

This validates one cluster client, not the multi-tenant server deployment, a fleet
budget, or live outage recovery. **5–15 KB/agent/hour is still an unvalidated
planning target, not an enforced or measured rate.** Shared context is in the
[Current implementation document](https://github.com/armosec/shared-designs-and-docs/blob/docs/otel-implementation-reality/projects/agent-telemetry-system/research/high-level-requirements.md#current-implementation-october-16-2026).

## Tested artifact and effective configuration

| Area | Evidence / value |
| --- | --- |
| Tested commit | `c0bcee309df6c2f5a0fc13c6fb1474175678f64d` |
| Unique image | `quay.io/matthiasb_1/synchronizer:otel-stage-c0bcee3-20261016104528` |
| Multi-architecture digest | `sha256:c227e10f55b7a26337d6902ca2853c64b9e5208921ed02df5488e8705d3eb73b` |
| Platforms | Built/published linux/amd64 and linux/arm64; anonymous pull verified. Test pod ran on arm64. Cluster had 24 nodes: 23 arm64, one amd64. |
| Pod / lifecycle | `synchronizer-848dc9d966-fm5hg`; container started 10:48:27 UTC; ready, zero restarts throughout observation. |
| Service identity | `synchronizer-client`, version `otel-stage-c0bcee3-20261016104528` verified in ingested resource attributes |
| Discovery | Existing `API_URL=https://api-dev.armosec.io`; v3 response read **from inside the cluster** advertised `otel.dev-stage.armosec.io:443`, normalized to HTTPS |
| Credentials | Existing read-only `cloud-secret` mount at `/etc/credentials`, passed through shared ProviderConfig. No values extracted or logged. |
| Authenticated routing | Existing collector `armotenant` processor; ingested resources contain `tenant.id`; zero SDK telemetry-error counter. Service/version absent from the pre-test baseline, present afterward in the cluster's ClickHouse. |
| Existing local logging | `zap` / `info`, unchanged |
| Default metric interval | 60,000 ms, verified by observed collection times |
| Default sampler | Parent-based 10% roots; no sampling override. Sampled incoming parents used for controlled HTTP failures. This short run does not measure the probabilistic root sampling ratio. |
| Exported logs | Fixed warn+ scope/body/attribute allowlist; per-fixed-body 60-second dedup, at most three keys |
| Compression / queues | Gzip, 128-record trace/log queues, batch size 32, 3-second export contexts, 5-second shutdown bound. Code/image defaults checked; gzip was not inspected on the encrypted live connection. |
| Histogram / span gates | Seconds: .001, .01, .1, 1, 5, 30; slow threshold 1 second; shared process-wide span creation gate 10 seconds |
| Collector | Existing two-replica `armo-signoz-otel-collector:v0.4.0`; no changes |

No base/legacy/per-signal operational endpoint override was present in the deployment;
discovery populated the base for all three signals. No signal, sampler, interval,
queue or log-level override was introduced. Existing account/cluster configuration,
API URL, resources, replicas and limits remained unchanged. Endpoint precedence,
explicit opt-out, unset versus empty variables, per-signal overrides, bounded
nonfatal discovery, filtering, retries, and separation from
`OTEL_EVENTS_EXPORTER_ENDPOINT` retain the code-tested evidence documented in
[operational telemetry](operational-telemetry.md); they were not each toggled live.

Only the client's image and a unique pod-template validation annotation changed.
The backend server, ingester, node agent, operator, shared libraries, discovery
contracts, chart sources and collector configuration were not modified. The release
registry denied publishing; a unique anonymously pullable personal-namespace tag
was used instead, without overwriting any release tag.

## Preparation and bounded rollback

All Kubernetes commands used explicit context
`arn:aws:eks:us-east-1:015253967648:cluster/armo-dev-stage`; global context was not
changed. Read-only SigNoz baseline at **10:47:51 UTC** had zero recent
`synchronizer-client` metric series; recent trace/log counts were also zero.
Queries used the cluster's `signoz/chi-cluster-clickhouse-cluster-0-0-0` directly.

Before mutation, save the original deployment and inspected configuration privately.
A separate 15-minute watchdog was armed before patching. Rollback required the
original deployment UID, expected unique test image and validation annotation,
and used a resource-version-tested JSON patch to restore only the changed fields.
It also watched readiness and would restore after 150 seconds without a ready replica.
This avoids overwriting unrelated concurrent changes. Normal cleanup used the same
rollback path. No watchdog expiration or emergency rollback was needed.

## Component-local correction and safe failure boundaries

Inspection found the client's HTTP validation path was absent from operational
telemetry. Commit `c0bcee3` adds aggregate `http_ingest` metrics, safe diagnostics,
and W3C context extraction; existing slow-span and dedup gates remain unchanged.
Health/readiness paths remain uninstrumented. No request path, payload, workload
identity or error string becomes a metric dimension or exported diagnostic field.

The regression `TestInvalidPathStopsBeforeBodyAndCallbacks` proves the malformed
path returns HTTP 400 **before body reading, admission or any callback**. The second
controlled path sends only `{` to the existing network-stream HTTP route: JSON
unmarshal fails with HTTP 500 **before object identity extraction and callbacks**.
No valid network stream or target mutation is submitted. Neither path calls the
WebSocket writer or downstream storage. Existing oversized-body tests also verify
that status observation preserves MaxBytesReader's connection-close behavior.

## Normal and burst observations

All timestamps below are UTC. Samples are cumulative, isolated by test service
name/version; operation categories count processing stages rather than unique objects.

| Collection | 10:49:28.074 | 10:50:28.074 | Pre-burst 10:51:28.074 | Post-burst 10:52:28.073 |
| --- | ---: | ---: | ---: | ---: |
| All completed operation stages | 6,714 | 6,997 | 7,096 | 8,257 |
| Successful HTTP ingest | 20 | 57 | 76 | 109 |
| Failed HTTP ingest | no series | no series | no series | 1,000 |
| Invalid-message drops | no series | no series | no series | 998 |
| Duplicate logs suppressed | 1 | 6 | 13 | 2,018 |
| Span creation attempts suppressed | 1 | 4 | 8 | 13 |
| SDK telemetry errors | 0 | 0 | 0 | 0 |
| Writer callers pending | 0 | 0 | 0 | 0 |
| Stored application series | 85 | 85 | 85 | 87 |

The first two collections were **60.000 seconds apart**. Subsequent periodic
intervals were 60.000, 59.999, 60.050 and 59.950 seconds. Metric transport-stage bytes
rose from 6,096,008 to 14,312,399 between the first two collections; these are
**application payload bytes**, not telemetry ingest bytes. Normal activity included
thousands of successful sends/dispatches and continued receives, gets and HTTP ingest.
Between the pre/post-burst collections, successful sends and dispatches each rose
3,332→3,391; gets 177→182; receives 178→183. Core work continued during the test.

`operations.active` was two at periodic snapshots and the writer queue was idle at
those snapshots. `operations.stalled` had no stored data point: no >=30-second stall
was observed, which is not equivalent to a continuously measured zero. Successful
get/receive latency included operations above one second; three sampled get spans
in the complete experiment lasted about 13.1–13.8 seconds. Histogram bounds and
these slower paths provided useful troubleshooting evidence rather than per-event spans.

### Exactly 1,000 controlled failures

From **10:51:32.801007** to **10:51:36.645141**, submit 998 malformed-path requests
and two one-byte invalid-JSON requests, using 34 bounded client threads and a sampled
incoming W3C parent. All **1,000 completed within 3.844 seconds**, inside one
10-second creation window and one 60-second dedup window:

- 998 HTTP 400; two HTTP 500; zero 2xx acceptances; zero transport exceptions.
- Completion is independently confirmed by the next cumulative `http_ingest/error`
  value of 1,000. Drops rose by 998: only the malformed-path rejections qualify for
  this wrapper's invalid-message drop classification.
- Exactly two burst-attributed logs were stored: `synchronizer work dropped` and
  `synchronizer operation failed`. Both were severity 13 (warn), had only the
  normalized `operation` attribute, and preserved the input trace ID and parent span ID.
- HTTP histograms showed all burst handlers completed within one server-side second
  (maximum 0.867 s), despite delaying two clients' body sends by 1.25 s. Client delay
  is not a measurement of handler duration. **No burst HTTP span was eligible**;
  a sampled parent did not bypass the slow threshold. Do not attribute this result
  to the ten-second creation cap.
- The next log-suppression counter rose **13→2,018**, a delta of 2,005. The 1,998
  burst diagnostic attempts explain 1,996 suppressions plus two exports. Histogram
  deltas show ten additional normal slow completions (five get, five receive);
  nine were suppressed and one exported, explaining the remaining nine suppressions.
- Span suppressions rose **8→13** while normal slow work continued. This aggregate
  counter is not exclusively attributable to the burst.
- Stored application series rose **85→87**, exactly the new bounded error-outcome
  and invalid-message-drop series; no series per injected request or trace ID appeared.

### Separate trace-parent and creation-cap calibration

After the burst, two additional one-byte invalid-JSON requests started at
**10:53:38.054828** and **10:53:42.512695**; both returned HTTP 500 before callbacks.
They were about 4.46 s apart and shared a sampled parent. The interval's cumulative
histogram count minus the <=1 s bucket increased by two across 48 HTTP completions
(46 normal successes and two injected failures). This does not identify both slow
requests. One recorded `synchronizer.http_ingest` span directly confirms a slow
calibration request, preserving the exact supplied trace ID and parent span ID;
its duration was **1.060001212 s**. Its failed-operation log correlated with that
recorded child span. One stored span is consistent with the shared ten-second gate,
but does not independently isolate suppression of the second calibration request.
The aggregate suppression counter includes normal work. These two requests are
separate from the 1,000-request burst; the creation cap retains code-test evidence,
while this run directly verifies the slow threshold and parent preservation.

Across the whole temporary deployment, SigNoz stored **four spans**: three
`synchronizer.get` and one `synchronizer.http_ingest`. Nine operational logs were
stored: six fixed slow records, two failed records and one dropped record, all warn
with only `operation`. Three carried trace context; unparented ordinary slow logs
had empty trace IDs. No raw URLs, error strings, payloads or credentials were present
in the inspected operational records. This is not a full cross-component object trace.

## Cardinality and resource accounting

Eleven instruments are defined. Nine had data during normal observation; ten after
first drops. The stalled-work instrument remained absent. These are instrument counts,
not stored series counts.

Six operation-attribute combinations of the explicit histogram expanded to **66
stored series**: 42 bucket series (seven each), six counts, six sums, six minima and
six maxima. SigNoz stores min/max in addition to classic Prometheus bucket/count/sum
expansion. Normal collections had **85 stored scalar sample rows**, representing
25 original OTLP attribute-set data points reconstructed from that expansion; post-burst
87 rows correspond to 27 reconstructed OTLP points. OTLP wire data points were not
captured directly. Shutdown added two bounded outcome series, ending at **89
application series / 29 reconstructed points**.

Observed application dimensions were `operation`, `outcome`, `reason`, with collector/
SDK `le` and `__temporality__`. Values were operations `connect,dispatch,get,http_ingest,
receive,send`, outcomes `success,error`, reason `invalid_message`, and temporality
`Cumulative,Unspecified`. Shutdown introduced receive-error/transfer-byte outcome
series. Tenant stamping stayed in resource attributes; no workload, request, URL or
error-string metric dimension was observed.

Collector-generated span metrics added **63 service-attributed stored series**:
`signoz_calls_total` (3), latency bucket (54), count (3), sum (3), for **152 related
series including the 89 application series**. Two collector identities split get
span metrics; the HTTP span added another group. These generated metrics lack the
test version label and are attributed by service name and the zero-service baseline;
they must not be presented as strictly version-filtered data. The post-burst checkpoint
had 42 generated plus 87 application series (129 related).

Point samples showed **18m CPU / 48 MiB** around the first collection and **151m /
183 MiB** just after the burst. Readiness remained true, zero test-pod restarts, and
health/readiness endpoints returned 200. These are snapshots, not peak measurements,
an A/B comparison or proof of telemetry overhead. No live exporter outage or queue
saturation was deliberately induced.

## Bytes, restoration and limitations

The inspected `signozmeter` connector flushes every **1 hour** and labels its data
by service name, without service version. No `synchronizer-client` meter rows were
available during this bounded experiment. **Attributed live bytes by signal and an
hourly rate were not measured.** Compressed storage, unrelated fleet traffic,
application payload-byte counters and the local synthetic OTLP sizes are not substitutes.
Local synthetic results remain separate in [operational telemetry](operational-telemetry.md).

Targeted restoration was submitted at **10:54:45.463676 UTC**. A final off-cadence
collection at **10:54:45.523 UTC** reached SigNoz, with active connections zero and
SDK telemetry errors zero, supporting successful shutdown export. One operation
remained active in that snapshot: provider flushing does not prove all worker work
drained. Restoration completed at **10:54:50.066885 UTC**, with one original-image
replica ready and **exact equality of the restored deployment's complete spec**.
Original image: `quay.io/kubescape/synchronizer:v0.0.167`. No unrelated concurrent spec
changes were found. The failed/terminated historical pod visible before the experiment
was not modified or removed.

All **23 SigNoz ConfigMaps' data**, the collector deployment spec, and the component
configuration data compared equal before/after. The watchdog and local port-forward
were stopped; temporary registry credential copies and private raw configuration
snapshots were removed after comparison. The unique test image remains available for
reproduction. No test process or temporary deployment configuration remains active.

Checks passed: repository `go test -short ./...`, targeted race checks including
HTTP adapter/telemetry/core/messaging/entrypoints, `go vet ./...`, `go build ./cmd/...`,
and `git diff --check`. Independent read-only review approved the HTTP correction.
The earlier full broker integration storage failure remains a separate local gap.

Remaining unverified requirements: backend-server deployment/authentication, live
opt-out/endpoint-override matrix, sustained exporter outage recovery, queue saturation,
isolated ten-second-cap suppression, statistical root-sampling ratio, complete
downstream trace continuity, peak/attributed
resource overhead, and live hourly/fleet byte budgets. Shared-provider option APIs and
collector admission controls remain cross-repository follow-ups, not changes in this run.


## Subsequent review corrections

The live artifact remains the commit/digest listed above. Subsequent Copilot review
found a race between incoming-pool initialization and shutdown. Pool publication and
shutdown cancellation now share the existing connection-state lock; startup releases
an unpublished pool when shutdown wins. Deterministic early-stop and repeated concurrent
Start/Stop race regressions reproduce the original failures and pass with the fix.
This follow-up was verified locally, not redeployed in the recorded experiment.

The separate integration-test module also now declares the existing gRPC exporter
versions and missing content checksums. Its readonly telemetry tests pass. An initial compilation attempt hit Go's
500 MiB source-archive limit for the pinned private ingester. Retrying with the CI
setting `GIT_LFS_SKIP_SMUDGE=1` passed readonly integration-test compilation:
`go test -mod=readonly -tags=integration -run '^$' ./...` from `tests/`. This compiles
the test package without running its broker/Kubernetes scenarios. No dependency
version or cross-repository change was needed.


A second review found that the existing HTTP proxy dialer discarded cancellation,
which could prevent the new client SIGTERM path from returning during a silent CONNECT
handshake. Direct/proxy TCP establishment now uses context-aware dialers; cancellation
closes in-progress CONNECT I/O. A successful tunnel detaches the cancellation callback
and clears the handshake deadline so its lifetime is not limited by the dial context.
Silent-proxy, canceled TCP-forward, and post-deadline tunnel regressions passed 20
repeated race-detector runs. This transport correction was locally verified after the
recorded staging deployment; it was not part of that tested image.

Copilot review `5547794310` also identified the reconnect callback capturing
`run`'s lifecycle error. Connection retries now execute in `connectWebsocket`,
with an error local to each dial attempt, so the writer cannot overwrite the
error returned by `Start`. A cancellation-during-reconnect regression runs 100
iterations per test; 20 race-detector repetitions pass. Root short tests, vet and
command builds pass. This follow-up was locally verified, not deployed live.
