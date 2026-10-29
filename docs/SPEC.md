# Specification

## Problem

A support engineer gets a Prometheus TSDB dump from a customer's OpenShift cluster: the blocks and the write-ahead
log (WAL), usually as one `.tar.gz`, captured the way Red Hat's
[solution 5482971](https://access.redhat.com/solutions/5482971) describes. The engineer wants to ask it what the
cluster was doing around an incident.

The only way today is to run the cluster's own Prometheus image under a container runtime, with retention set
far enough back that startup doesn't delete the old blocks and with an empty config so it scrapes nothing, then
query in the web UI. That needs a container runtime, the image, a pull secret and a network, and some of the
places engineers work have none of them. It also fails quietly in two ways: the web UI's default time range is
now, and a dump is old, so the first query shows nothing; and a damaged block or WAL stops Prometheus from
starting, so engineers delete the damaged files by hand and lose that data without a record.

prometheus-tsdb-reader is one static binary that opens a dump and answers PromQL the way Prometheus does, says
what the dump covers and what it is missing, and bounds its own output. A second caller is an automated system
that runs it as a sandboxed command; its constraints are requirements too, in [Running it](#running-it).

Each requirement has an ID the [roadmap](ROADMAP.md) cites. A deferred requirement stays here and gets a later
slice there.

## Running it

- **RUN-1: One static binary.** The tool runs on a host with no container runtime, no image, no pull secret and
  no network. It needs nothing installed beside it and never dials out.
- **RUN-2: Runs under a sandboxed caller.** The second caller runs the tool with argv only and no shell. Its
  input is on a read-only mount, one scratch working directory is the only writable place, there is no network,
  and it keeps about 1 MiB of output, of which a model reads the first few KiB. So the tool takes every input as
  a whole argument value, writes only under [`--scratch`](#dumps) (default: the working directory), and puts the
  record that says what happened first on stdout ([OUTPUT-2](#output)). The default [limits](#limits) keep a
  result under 1 MiB.
- **RUN-3: Stdout is data, stderr is progress.** Stdout carries only the JSON lines of the result. Progress and
  diagnostics (extracting, replaying the WAL, skipping a block) go to stderr. The exit code is 0 when the tool
  answered, including an empty or truncated answer; 1 when it couldn't (no dump, no TSDB root, a PromQL error);
  2 for bad arguments.

## Commands

The CLI is modeled on `promtool query`, so what engineers know transfers. One deliberate difference: an
expression or selector is never a positional argument. It enters only through the `--promql` flag, so a caller
that gates on argv can deny free-form PromQL by denying that one flag ([QUERY-6](#queries)).

```
prometheus-tsdb-reader [global flags] <command> [command flags]
```

| Command | Does |
| --- | --- |
| `info` | The dump's time range, blocks, WAL state, gaps and the PromQL version |
| `query instant --promql EXPR` | One expression at one time |
| `query range --promql EXPR` | One expression over a range at a step |
| `query series --promql SELECTOR ...` | Series matching any of the selectors |
| `query labels [NAME]` | Label names, or the values of one label, optionally under `--promql SELECTOR` |
| `query metadata [--metric NAME]` | Metric metadata (type, help, unit) when the dump carries any |
| `named NAME [flags]` | A [named query](#named-queries) built from typed flags |

Global flags:

| Flag | Meaning | Default |
| --- | --- | --- |
| `--dump PATH` | An extracted directory or a `.tar.gz` | required |
| `--scratch DIR` | The only place the tool writes | the working directory |
| `--no-wal` | Don't replay the WAL | off |
| `--max-extract-bytes N` | Largest archive the tool will extract | 32 GiB |
| `--max-series N` | Series per result before truncation | 100 |
| `--max-points N` | Points per result before truncation | 20000 |
| `--max-samples N` | Samples one query may load, as Prometheus's `--query.max-samples` | 50000000 |
| `--memory-limit N` | Soft limit on the process's heap | 4 GiB |

Time flags, on every query:

| Flag | Meaning | Default |
| --- | --- | --- |
| `--time T` | Instant query evaluation time | the dump's end |
| `--start T`, `--end T` | Range query bounds | `--end` is the dump's end, `--start` is 2h before it |
| `--step D` | Range query step | see [STEP-1](#steps) |
| `--window D` | The `[W]` range in a named query's `rate` | 5m |

`T` is an RFC 3339 time or a duration such as `6h`, which counts back from the dump's end. The defaults are
relative to the dump, never to now, because a dump is old and a range ending now is empty
([QUERY-8](#queries)).

## Dumps

- **DUMP-1: A directory or an archive.** `--dump` names an extracted directory or a `.tar.gz`. An archive is
  extracted under `--scratch` first.
- **DUMP-2: Safe extraction.** Extraction refuses an entry whose path would land outside the extraction
  directory, a symbolic or hard link whose target would, and a device or other special file. It stops at
  `--max-extract-bytes` and reports how far it got. An archive the tool refuses is reported, with the entry that
  caused it.
- **DUMP-3: Find the TSDB root.** Dumps differ in layout: the blocks sit at the top level, under `data/`, under
  `prometheus/`, or deeper. The tool finds the one directory that holds block directories (named by ULID, each
  with a `meta.json`) or a `wal/` directory, and says which it chose. When it finds none it fails and lists what
  it saw; when it finds more than one it fails and lists them, and `--root` picks one.
- **DUMP-4: Never write under the dump.** Opening a TSDB read-only still writes: Prometheus's own reader
  hard-links head chunk files and replays the WAL into a scratch directory, which it puts inside the data
  directory unless told otherwise (see [stack.md](stack.md#adr-002-build-on-prometheuss-tsdb-and-promql-packages)).
  The tool always gives it a directory under `--scratch`, creates that directory for the run, and removes it on
  exit. The dump is never opened for writing, so it works from a read-only mount.
- **DUMP-5: Say what the dump covers.** `info` reports the dump's start (the earliest block's start) and end
  (the latest sample in the newest block or the WAL), each block's time range, series count and status, whether
  the WAL exists, its size, and whether it was replayed, skipped or failed, the gaps, the TSDB root it chose and
  the PromQL version. Every query's summary repeats the dump's start and end, so a reader never has to guess
  why a range is empty.

## Damage

Dumps are taken from clusters in trouble and often carry a damaged block or an unfinished WAL. Prometheus
refuses to start on either, which is why engineers delete them. The tool answers from what is readable and says
what it left out.

- **DAMAGE-1: A corrupt block is a gap.** A block the TSDB cannot open is skipped. Its time range is reported as
  a gap in `info` and in every summary whose query range overlaps it, with the block's directory name and the
  error, so a flat line through a gap is never mistaken for a quiet cluster.
- **DAMAGE-2: WAL replay is optional.** `--no-wal` skips the WAL. Replay holds the head's samples in memory
  (see [LIMIT-4](#limits)), and a WAL from a busy cluster can be larger than the host allows. The summary says
  the WAL was skipped and the dump's end moves back to the newest block.
- **DAMAGE-3: A failed WAL is missing samples, not a failed run.** When replay fails, the tool answers from the
  blocks and reports that the samples after the newest block are missing, with the error. Those are the newest
  samples, often the incident window, so the summary says so in words a reader who sees only the first lines
  will notice.

## Queries

- **QUERY-1: Instant queries.** `query instant` evaluates an expression at `--time`.
- **QUERY-2: Range queries.** `query range` evaluates an expression from `--start` to `--end` every `--step`.
- **QUERY-3: Series listing.** `query series` lists the label sets of series matching any selector, within
  `--start` and `--end`.
- **QUERY-4: Label listing.** `query labels` lists label names, or with a `NAME` the values of that label,
  within `--start` and `--end`, narrowed by `--promql SELECTOR` when given.
- **QUERY-5: Metadata listing.** `query metadata` lists metric type, help and unit. Metadata isn't part of a
  block; Prometheus records it in the WAL only when its `metadata-wal-records` feature flag is on, so a dump
  usually carries none. The result says when the dump has no metadata at all, which is a different answer from
  a metric having none.
- **QUERY-6: Free-form PromQL sits behind one flag.** The only way an expression or selector string enters the
  tool is `--promql`. Named queries never take one. A caller that denies `--promql` can run only named queries,
  `info`, `query labels` without a selector and `query metadata`.
- **QUERY-7: Every result names its PromQL version.** The engine is the one Prometheus release the binary
  embeds (see [stack.md](stack.md#adr-003-one-embedded-promql-version)), which isn't necessarily the cluster's.
  Every summary carries `promql_version`, the version of the Prometheus module the binary was built with.
- **QUERY-8: Time defaults come from the dump.** `--time` and `--end` default to the dump's end and `--start` to
  2h before it. A range that doesn't overlap the dump is answered with the empty reason `outside-dump-range`
  ([OUTPUT-4](#output)), never with a silent empty result.

PromQL behaves as the embedded Prometheus release's engine behaves, including its lookback delta, staleness
handling, and the warnings and infos it attaches to a result, which pass through to the summary.

## Named queries

A named query is a PromQL expression the tool assembles from typed flags, so a caller that can only pass whole
argument values still gets a correct, escaped expression.

- **NAMED-1: Typed flags build matchers.** Each named query takes the flags in the table below and nothing else
  that reaches the expression. An exact flag (`--namespace`, `--instance`) becomes an `=` matcher with the value
  escaped by PromQL's string rules. A regex flag (`--pod`, `--device`, `--mount`) becomes an `=~` matcher with
  the value escaped as a PromQL string and left to PromQL to anchor. A flag left unset adds no matcher. The
  evaluated expression appears in the summary exactly as it was run, so a reader can paste it into Prometheus.
  A named query that needs two expressions (used and limit) runs both and reports both; the output never hides
  one side to make the other fit.

| ID | Name | Flags | Expressions, with `W` for `--window` |
| --- | --- | --- | --- |
| NAMED-2 | `pod-cpu` | `--namespace`, `--pod` | used: `sum by (namespace, pod) (rate(container_cpu_usage_seconds_total{container!="", container!="POD", ...}[W]))`; limit: `sum by (namespace, pod) (kube_pod_container_resource_limits{resource="cpu", ...})` |
| NAMED-3 | `pod-memory` | `--namespace`, `--pod` | used: `sum by (namespace, pod) (container_memory_working_set_bytes{container!="", container!="POD", ...})`; limit: `sum by (namespace, pod) (kube_pod_container_resource_limits{resource="memory", ...})` |
| NAMED-4 | `node-cpu` | `--instance` | `1 - avg by (instance) (rate(node_cpu_seconds_total{mode="idle", ...}[W]))` |
| NAMED-5 | `node-iowait` | `--instance` | `avg by (instance) (rate(node_cpu_seconds_total{mode="iowait", ...}[W]))` |
| NAMED-6 | `node-load` | `--instance` | `node_load1`, `node_load5`, `node_load15` and `instance:node_load1_per_cpu:ratio`, each `{...}` |
| NAMED-7 | `node-memory` | `--instance` | used: `1 - node_memory_MemAvailable_bytes{...} / node_memory_MemTotal_bytes{...}`; available: `node_memory_MemAvailable_bytes{...}` |
| NAMED-8 | `node-network` | `--instance`, `--device` | receive: `sum by (instance, device) (rate(node_network_receive_bytes_total{...}[W]))`; transmit: the same on `node_network_transmit_bytes_total` |
| NAMED-9 | `node-disk-write-latency` | `--instance`, `--device` | `rate(node_disk_write_time_seconds_total{...}[W]) / rate(node_disk_writes_completed_total{...}[W])`, both sides with identical matchers so the division matches on every label |
| NAMED-10 | `node-rules` | `--instance`, `--device` | the node-exporter recording rules: `instance:node_cpu_utilisation:rate1m`, `instance:node_memory_utilisation:ratio`, `instance:node_network_receive_bytes_excluding_lo:rate1m`, `instance:node_network_transmit_bytes_excluding_lo:rate1m`, `instance:node_network_receive_drop_excluding_lo:rate1m`, `instance:node_network_transmit_drop_excluding_lo:rate1m`, `instance_device:node_disk_io_time_seconds:rate1m` and `instance_device:node_disk_io_time_weighted_seconds:rate1m`, each `{...}`; `--device` applies to the `instance_device` ones |
| NAMED-11 | `vm-filesystem` | `--namespace`, `--mount` | used: `kubevirt_vmi_filesystem_used_bytes{...} / kubevirt_vmi_filesystem_capacity_bytes{...}`; free: `1 -` the same; both by `(namespace, name, mount_point)` |

`...` stands for the matchers the flags add. For `pod-cpu` and `pod-memory`, a pod that has a `used` series but
no `limit` series is listed by name in the summary, so a pod with no limit is reported rather than dropped by a
division that finds nothing to divide by.

Two of these correct the working notes they come from, and one fixes them. `node-cpu` and `node-iowait` are
already per-CPU averages, so dividing by the CPU count again is wrong. `node-disk-write-latency` must use the
same matchers on both sides; with different instances on each side the division matches nothing.

## Output

- **OUTPUT-1: JSON lines with RFC 3339 times.** Stdout is one JSON object per line. Every time is RFC 3339 in
  UTC, where the Prometheus HTTP API has Unix seconds. Sample values are strings, as in the API, so `NaN` and
  `+Inf` survive.
- **OUTPUT-2: The summary comes first.** The first line is a `summary` record. It carries the command, each
  expression as evaluated, `start`, `end` and `step` (or `time`), `promql_version`, the dump's `start` and
  `end`, the counts of series and points, the `truncated` record if any ([LIMIT-3](#limits)), the `empty`
  reason if any, the gaps and missing spans that overlap the range ([Damage](#damage)), the observed sample
  interval and the warnings ([Steps](#steps)). A reader who sees only the first few KiB sees everything that
  qualifies the result.
- **OUTPUT-3: Series records mirror the HTTP API.** After the summary, one `series` record per series, shaped
  like an entry of the API's `result`: `metric` with the label set, then `value` (instant) or `values` (range)
  as `[time, "value"]` pairs. A scalar or string result is one `scalar` or `string` record. A named query with
  several expressions marks each record with the expression's name (`used`, `limit`). `query series` emits one
  `series` record per label set, `query labels` one `labels` record, `query metadata` one `metadata` record per
  metric.
- **OUTPUT-4: Say why a result is empty.** An empty result's summary carries one `empty` reason:
  `outside-dump-range` when the range or time doesn't overlap the dump; `no-matching-series` when nothing in the
  dump matches the selectors at any time; `no-samples-in-range` when matching series exist but have no samples
  in the range. The three are different answers, and the first is the one the web UI gives silently.

An instant query's output looks like this, with the summary on one line:

```
{"kind":"summary","command":"query instant","expression":"up","time":"2026-01-01T00:00:00Z",
 "promql_version":"v3.15.1","dump":{"start":"2025-12-30T00:00:00Z","end":"2026-01-01T00:04:00Z"},
 "series":2,"points":2,"truncated":null,"empty":null,"gaps":[],"sample_interval":"1m","warnings":[]}
{"kind":"series","metric":{"__name__":"up","instance":"node-a","job":"node-exporter"},"value":["2026-01-01T00:00:00Z","1"]}
{"kind":"series","metric":{"__name__":"up","instance":"node-b","job":"node-exporter"},"value":["2026-01-01T00:00:00Z","0"]}
```

The output's shape is interface: a test may pin it, and changing it is an interface change.

## Limits

Memory is the real constraint. WAL replay holds the head's samples in memory, and a wide range query over many
series holds every point it returns. The limits bound both, and hitting one is reported, never silent.

- **LIMIT-1: Series limit.** A result has at most `--max-series` series. The default keeps the output readable
  and, with LIMIT-2, under the sandboxed caller's cap.
- **LIMIT-2: Point limit.** A range result has at most `--max-points` points across all series. The default is
  chosen so that a full result stays under 1 MiB of JSON lines.
- **LIMIT-3: Truncation is a record.** When a limit cuts a result, the summary's `truncated` carries which
  limit, the value, and how many series or points the full result had. Series are kept in the engine's order,
  and a series is dropped whole, never cut partway.
- **LIMIT-4: Samples and memory.** `--max-samples` is the engine's bound on samples one query may load, the same
  as Prometheus's `--query.max-samples`; a query over it fails with the engine's error and exit code 1.
  `--memory-limit` sets the Go runtime's soft heap limit. It is soft: the runtime collects garbage harder as it
  nears the limit, and a query can still exceed it. Before replaying the WAL the tool reports the WAL's size on
  stderr, so an engineer on a small host can choose `--no-wal`.

## Steps

OpenShift scrapes about once a minute, so samples are about 1m apart, and a spike that rises and falls between
two samples is not in the dump at all. A step coarser than the samples hides more: a 5m step over days
smooths peaks, while a 1m step within a couple of hours is the most accurate view the dump can give. A step
finer than the samples adds nothing, since PromQL repeats the last sample at each extra point.

- **STEP-1: A default step from the range.** When `--step` is unset, the step is the smallest of 1m, 5m, 15m,
  1h and 6h that yields at most 500 points per series over the range. The summary carries the step either way.
- **STEP-2: Warn when the step is coarse.** When the step is longer than the observed sample interval, the
  summary warns `step-coarse`, saying how many samples each step skips and that peaks shorter than the step are
  invisible.
- **STEP-3: Warn when the step is finer than the data.** When the step is shorter than the observed sample
  interval, the summary warns `step-finer-than-data`, saying the extra points repeat the same samples.
- **STEP-4: Say what the samples can show.** Every range summary carries `sample_interval`, the median gap
  between consecutive samples in the result's series, so a reader knows the finest view the dump can give.

## Fixtures

The repo's test dumps are synthetic, built with Prometheus's `tsdb` package: a few blocks, a WAL, and copies
with a block made corrupt, a WAL made unreadable, and the layouts of DUMP-3. A real dump can't be committed
([Privacy](standards.md#privacy)), so synthetic fixtures don't prove:

- the metric and label names a real OpenShift cluster exposes, so each named query's expression is checked
  against a real dump by hand, with `--promql` on the metric names it uses;
- the layout and contents of a dump captured by the Red Hat procedure, so DUMP-3's list of layouts grows from
  real dumps;
- the kinds of damage real dumps carry, so DAMAGE-1 and DAMAGE-3 are checked against real damaged dumps when
  one turns up;
- the WAL sizes and memory use of a busy cluster, so the limit defaults are tuned from real runs;
- whether the embedded PromQL version answers as the cluster's would, which is the open question below.

## Unknowns

What the spec assumes without having checked, to verify at the pinned Prometheus version:

- That metadata reaches a dump only through the WAL and only under a feature flag (QUERY-5).
- That the embedded release reads blocks written by every Prometheus version OpenShift ships, including
  features the older cluster didn't have, such as native histograms and out-of-order samples.
- That the pod and node metric and label names in the named queries match what OpenShift's monitoring stack
  and KubeVirt export (`mount_point` in particular).

## Open questions

Raised here, not decided:

- **The binary's name.** `package main` at the root makes it `prometheus-tsdb-reader`, which is long to type at
  a support console. A shorter name would be a repo-level decision.
- **Library or internal.** Whether the packages under the CLI are a public Go API or stay under `internal/`
  until a second consumer needs them.
- **One PromQL version or builds per release.** Whether PromQL behavior differs across the Prometheus versions
  OpenShift ships, in ways a support query would notice, and if it does, whether that calls for one binary per
  Prometheus release rather than one embedded version (see
  [stack.md](stack.md#adr-003-one-embedded-promql-version)).
- **Named queries in code or in a data file.** Whether the named queries are compiled in, or read from a data
  file an engineer can extend without a release.
