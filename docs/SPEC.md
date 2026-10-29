# Specification

## Problem

A support engineer gets a Prometheus TSDB dump from a customer's OpenShift cluster: the blocks and the write-ahead
log (WAL), usually as one `.tar.gz`, captured the way Red Hat's
[solution 5482971](https://access.redhat.com/solutions/5482971) describes. The engineer wants to ask it what the
cluster was doing around an incident.

Reading a dump with Prometheus itself means a container runtime, the cluster's image, a pull secret and a
network, and some of the places engineers work have none of them. It also fails quietly: the web UI's default
time range is now, and a dump is old, so the first query shows nothing; a damaged block stops Prometheus from
starting, so engineers delete it by hand and lose that data without a record; and a damaged WAL is repaired in
place at startup, which discards everything after the damage with one log line to say so.

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
  diagnostics (extracting, replaying the WAL, skipping a block) go to stderr. The exit code says what kind of
  run it was, and a run that fails writes one `error` record on stdout so a reader who only has stdout knows
  why ([OUTPUT-5](#output)):

  | Exit | Meaning |
  | --- | --- |
  | 0 | The tool answered, including an empty or truncated answer |
  | 1 | It couldn't: no dump, no TSDB root, an archive it refused, a PromQL error |
  | 2 | Bad arguments |
  | 3 | A limit refused the query ([LIMIT-4](#limits)); the same query over a narrower range or fewer series may pass |

- **RUN-4: One process, one query.** Each run opens the dump, replays the WAL, answers and exits. There is no
  server or session mode: the sandboxed caller can't use one, and an engineer who runs many queries extracts the
  archive once and points `--dump` at the directory ([DUMP-1](#dumps)), which leaves the WAL replay as the cost
  of each run.

## Commands

The CLI is modeled on `promtool query`, so what engineers know transfers. One deliberate difference: an
expression or selector is never a positional argument. It enters only through the `--promql` flag, so a caller
that gates on argv can deny free-form PromQL by denying that one flag ([QUERY-6](#queries)).

```
prometheus-tsdb-reader [global flags] <command> [command flags]
```

| Command | Does | Time flags |
| --- | --- | --- |
| `info` | The dump's time range, blocks, WAL state, gaps and the PromQL version | none |
| `query instant --promql EXPR` | One expression at one time | `--time` |
| `query range --promql EXPR` | One expression over a range at a step | `--start`, `--end`, `--step` |
| `query series --promql SELECTOR ...` | Series matching any of the selectors | `--start`, `--end` |
| `query labels [NAME]` | Label names, or the values of one label, optionally under `--promql SELECTOR` | `--start`, `--end` |
| `query metadata [--metric NAME]` | Metric metadata (type, help, unit) when the dump carries any | none |
| `named NAME [flags]` | A [named query](#named-queries) built from typed flags, evaluated as a range query | `--start`, `--end`, `--step`, `--window` |

Global flags:

| Flag | Meaning | Default |
| --- | --- | --- |
| `--dump PATH` | An extracted directory or an archive ([DUMP-1](#dumps)) | required |
| `--scratch DIR` | The only place the tool writes | the working directory |
| `--root PATH` | The TSDB root inside the dump, relative to the extracted directory, when [DUMP-3](#dumps) finds more than one | found |
| `--no-wal` | Leave the WAL out of the dump ([DAMAGE-2](#damage)) | off |
| `--queries FILE` | A named-query file in place of the embedded one ([NAMED-12](#named-queries)) | the embedded file |
| `--max-extract-bytes N` | Most bytes the tool will write while extracting an archive | 32 GiB |
| `--max-series N` | Series per result before truncation | 100 |
| `--max-points N` | Points per result before truncation | 20000 |
| `--max-samples N` | Samples one query may load, as Prometheus's `--query.max-samples` | 50000000 |

Time flags, on the commands the table above lists:

| Flag | Meaning | Default |
| --- | --- | --- |
| `--time T` | Instant query evaluation time | the dump's end |
| `--start T`, `--end T` | Range bounds | `--end` is the dump's end, `--start` is 2h before `--end`; for `query series` and `query labels`, the whole dump, as the HTTP API's series and label endpoints default to everything stored |
| `--step D` | Range query step | see [STEP-1](#steps) |
| `--window D` | The `[W]` range in a named query's `rate` | 5m |

`T` is an RFC 3339 time or a duration such as `6h`, which counts back from the dump's end. The defaults are
relative to the dump, never to now, because a dump is old and a range ending now is empty
([QUERY-8](#queries)). A `--start` after `--end` is a bad argument (exit 2).

## Dumps

- **DUMP-1: A directory or an archive.** `--dump` names an extracted directory or an archive: a tar stream,
  gzip-compressed or not, recognized by its content rather than its name. An archive is extracted under
  `--scratch` into a directory named for the archive, its name without `.tar.gz`, `.tgz`, `.tar` or `.gz`
  (`dump.tar.gz` becomes `dump`), and stderr names that directory so the next run can pass it as `--dump` and
  skip the extraction. The extraction stays; only the run's own directory ([DUMP-4](#dumps)) is removed on exit.
  When that directory already exists, the run fails and names it rather than extracting over it or reusing it,
  since it may hold another archive's extraction. An extraction fills a hidden directory beside it and is moved
  into place only when complete, so the named directory never holds a partial one.
- **DUMP-2: Safe extraction.** Extraction refuses an entry whose path would land outside the extraction
  directory, a symbolic or hard link whose target would, a hard link to a symbolic link (a second name for the
  link in another directory, where its target text leads elsewhere), and a device or other special file. A
  symbolic link that resolves to nothing is removed once extraction ends and named on stderr: it reaches nothing
  in the archive, and once the extraction moves under scratch its target could lead to a file created there
  later. It counts the bytes it writes and stops at `--max-extract-bytes`; a partial extraction answers wrong, so
  the run then fails with exit code 1 and says how far it got. An archive the tool refuses is reported with the
  entry that caused it.
- **DUMP-3: Find the TSDB root.** Dumps differ in layout: the blocks sit at the top level, under `data/`, under
  `prometheus/`, or deeper. The tool finds the one directory that holds block directories (named by ULID, each
  with a `meta.json`) or a `wal/` directory, and says which it chose. When it finds none it fails and lists what
  it saw; when it finds more than one it fails and lists them, and `--root` picks one.
- **DUMP-4: Never write under the dump.** Opening a TSDB for reading still writes: the head that replays the WAL
  cuts, truncates and removes its chunk files, so Prometheus's own read-only opener links the dump's
  `chunks_head/` files into a scratch directory, and a link can't leave a filesystem (see
  [stack.md](stack.md#adr-002-build-on-prometheuss-tsdb-and-promql-packages)). The tool instead copies
  `chunks_head/` into a directory it creates under `--scratch` for the run, by hard link when the dump and
  scratch share a filesystem and by copying otherwise, replays the WAL there, and removes that directory on exit,
  including when an interrupt stops the run, even partway through the replay, and when the reader of stdout goes
  away. A second interrupt stops the run at once and may leave the directory behind. The dump is opened only for
  reading, so a dump on a read-only mount, a USB disk or a container volume works. Scratch needs room for the
  extracted archive plus a copy of `chunks_head/`.
- **DUMP-5: Say what the dump covers.** `info` reports the dump's start (the earliest readable block's start, or
  the WAL's oldest sample a replay keeps when that is earlier) and end (the latest sample in a readable block or
  the WAL),
  each block's time range, series count and status, whether the WAL exists, its size, its own newest sample and
  whether it was read, skipped or failed, the gaps, the TSDB root it chose and the PromQL version. The WAL's
  newest sample comes from decoding its records in one pass, not from replaying it, so `info` is cheap in memory
  on any dump; a query makes the same pass before it replays, so its summary reports the same newest sample, at
  the cost of reading the WAL twice. A block's last sample comes from its index, since a block's own end is the
  end of the range it was cut for. Blocks can overlap, so the tool reads the index of every readable block whose
  range could hold a later sample than one already found, newest end first; a corrupt newest block leaves the end
  to the newest readable one or the WAL. A replay keeps the WAL's samples from the latest end of a block cut from
  in-order samples, as Prometheus's own open does; a block cut from out-of-order samples, or from only some
  series, doesn't move that bound, since the WAL still holds the in-order samples in its range. Nor does a
  corrupt block ([DAMAGE-1](#damage)): its `meta.json` may not read, so how it was cut is unknown, and leaving it
  out lets the WAL fill in what it holds of its range. Every query's summary repeats the dump's start and end, so
  a reader never has to guess why a range is empty. With `--no-wal`, the dump's end is the newest readable
  block's last sample for every command, `info` included, and the WAL's own newest sample is still reported as
  the WAL's.

## Damage

Dumps are taken from clusters in trouble and often carry a damaged block or an unfinished WAL. The tool answers
from what is readable and says what it left out.

- **DAMAGE-1: A corrupt block is a gap.** A block the TSDB cannot open is skipped, and so is one whose index
  fails as the tool reads it for the dump's end ([DUMP-5](#dumps)). Its time range is reported as a gap in `info`
  and in every summary whose query overlaps it, with the block's directory name and the error, so a flat line
  through a gap is never mistaken for a quiet cluster. A query overlaps a gap when the gap overlaps its time or
  range or the window its selectors read ([STEP-4](#steps)); a listing, when the gap overlaps its range. A gap is
  reported even where the replayed WAL holds samples in its range, since nothing says the WAL holds all of them.
  The range is the one the block's `meta.json` gives; when that doesn't read, the gap's `start` and `end` are
  null and it appears in every summary, since it may overlap any range. (Prometheus's own open skips such a
  block without a word.) A block whose ULID a readable block lists among its compaction parents or sources holds
  nothing the readable one lacks: it is `superseded`, listed with its error if it has one, and is not a gap.
  Queries don't read a superseded block that opens either, as Prometheus deletes it once the compacted one
  exists.
- **DAMAGE-2: WAL replay is optional.** `--no-wal` skips the WAL. Replay holds the head's samples in memory
  ([Limits](#limits)), and a WAL from a busy cluster can be larger than the host allows. The summary says the
  WAL was `skipped` and the dump's end moves back to the newest block's last sample, for `info` as for a query.
  The tool still decodes the WAL for its size and newest sample, which holds one record at a time. A dump with no
  WAL reports it `absent`, with the flag or without.
- **DAMAGE-3: A failed WAL is missing samples, not a failed run.** Prometheus itself, on a WAL read error,
  keeps what it replayed before the error and truncates the WAL after it. The tool keeps the same samples and
  truncates nothing: it answers from the blocks and the replayed part of the WAL, and reports a missing span
  that starts at the newest sample it kept and ends at the newest timestamp the decoding pass of DUMP-5 found
  in any readable WAL record after the error, or is open-ended when it found none. The span holds the newest
  samples, often the incident window, so the summary's `wal`, which comes before any result, says `failed` and
  carries the span and the error.

  A read error is a WAL record that the reader can't read, as when its checksum fails, or that doesn't decode.
  The decoding pass decodes every record type a replay does, so the two fail at the same record, and `info`,
  which doesn't replay, reports the WAL `failed` from the pass alone. An error opening a segment, rather than
  reading one, stops Prometheus's replay too, and fails the run. The span starts at the dump's newest sample
  before the error, in a readable block or a WAL record a replay keeps, the same for `info` and a query. A
  query's replay also keeps the chunks the head had written to `chunks_head/` before it stopped, which can reach
  past the error for some series but not others, so a query's dump can end inside the span, and samples in the
  span are missing for some series. Past the error, the pass reads on from the next record it can find in the
  same segment and from the start of each later one; it loses a record split across one of the WAL's 32 KiB
  pages where it picks up again, since a reader must start at a page boundary or a record. When a replay fails
  where the pass found no error, the span ends at the WAL's newest sample. With `--no-wal` nothing from the WAL
  is kept, so the WAL is `skipped` whatever the pass found, and stderr names the error. An interrupt during the
  read or the replay is a cancelled run (exit 1), not a failed WAL.

## Queries

- **QUERY-1: Instant queries.** `query instant` evaluates an expression at `--time`.
- **QUERY-2: Range queries.** `query range` evaluates an expression from `--start` to `--end` every `--step`.
- **QUERY-3: Series listing.** `query series` lists the label sets of series matching any selector, within
  `--start` and `--end`.
- **QUERY-4: Label listing.** `query labels` lists label names, or with a `NAME` the values of that label,
  within `--start` and `--end`, narrowed by `--promql SELECTOR` when given.
- **QUERY-5: Metadata listing.** `query metadata` lists metric type, help and unit. Metadata isn't part of a
  block; Prometheus writes it to the WAL only under its `metadata-wal-records` feature flag, which OpenShift's
  monitoring operator doesn't enable, so an OpenShift dump carries none. The tool reads what there is by
  decoding the WAL's series and metadata records, since the replayed head doesn't expose them. The result says
  when the dump has no metadata at all, which is a different answer from a metric having none.
- **QUERY-6: Free-form PromQL sits behind one flag.** The only way an expression or selector string enters the
  tool through argv is `--promql`, written as that argument followed by the expression as the next one. The other
  spellings Go's flag package reads (`-promql X`, `--promql=X`, `-promql=X`) are bad arguments (exit 2), so a
  caller that denies the argument `--promql` denies it in every spelling. Named queries never take one. A caller
  that denies `--promql` can run only named queries, `info`, `query labels` without a selector and `query
  metadata`. The other way PromQL reaches the tool is a named-query file ([NAMED-12](#named-queries)); a caller
  that wants only the shipped queries denies `--queries` too.
- **QUERY-7: Every result names its PromQL version.** The engine is the one Prometheus release the binary
  embeds (see [stack.md](stack.md#adr-003-one-embedded-promql-version)), which isn't necessarily the cluster's.
  Every summary carries `promql_version`: the Prometheus release the embedded module corresponds to, in the
  form engineers know (`3.15.1`), derived from the module version in the binary's build info by the mapping
  ADR-003 gives. A module version the mapping doesn't cover, such as a pseudo-version, is reported as it is.
- **QUERY-8: Time defaults come from the dump.** `--time` and `--end` default to the dump's end and `--start` to
  2h before `--end`, or for the listings to the dump's start. A range that doesn't overlap the dump is answered with the empty reason `outside-dump-range`
  ([OUTPUT-4](#output)), never with a silent empty result.

PromQL behaves as the embedded Prometheus release's engine behaves, including its lookback delta, staleness
handling, and the warnings and infos it attaches to a result, which pass through to the summary.

## Named queries

A named query is a PromQL expression from a data file, narrowed by typed flags, so a caller that can only pass
whole argument values still gets a correct, escaped expression. A named query is evaluated as a range query
with the range flags, so a reader gets the incident window, not one point; an engineer who wants an instant
answer has `query instant`.

- **NAMED-1: Typed flags become matchers through the parser.** Each named query takes the flags its definition
  lists and nothing else that reaches the expression. The tool parses the definition's PromQL with Prometheus's
  own parser, adds one matcher per set flag to every vector selector the flag applies to, sets every range
  selector's range to `--window`, and prints the result with the parser's printer. An exact flag
  (`--namespace`, `--instance`) becomes an `=` matcher; a regex flag (`--pod`, `--device`, `--mount`) becomes an
  `=~` matcher, which PromQL anchors at both ends, so `--pod checkout` matches only a pod named `checkout` and
  `--pod 'checkout-.*'` matches a prefix. Escaping is the printer's, so no layer concatenates PromQL text. A
  flag left unset adds no matcher. The evaluated expression appears in the summary exactly as it was run, in
  the printer's form: matchers sorted, the metric name folded in, whitespace normalized. A reader can paste it
  into Prometheus. A named query that needs two expressions (used and limit) runs both over one opening of the
  dump and reports both; the output never hides one side to make the other fit.
- **NAMED-12: Named queries are a data file.** The definitions live in a JSON file. The binary embeds the
  shipped one, so it still needs nothing beside it (RUN-1), and `--queries FILE` reads another in its place, so
  an engineer can add or change a query without a release. The file's shape is interface:

```json
{
  "flags": [
    {"name": "namespace", "label": "namespace", "match": "exact"},
    {"name": "pod", "label": "pod", "match": "regex"},
    {"name": "instance", "label": "instance", "match": "exact"},
    {"name": "device", "label": "device", "match": "regex"},
    {"name": "mount", "label": "mount_point", "match": "regex"}
  ],
  "queries": [
    {
      "name": "pod-cpu",
      "flags": ["namespace", "pod"],
      "expressions": [
        {"name": "used", "promql": "sum by (namespace, pod) (rate(container_cpu_usage_seconds_total{container!='', container!='POD'}[5m]))"},
        {"name": "limit", "promql": "sum by (namespace, pod) (kube_pod_container_resource_limits{resource='cpu'})"}
      ],
      "compare": {"left": "used", "right": "limit", "on": ["namespace", "pod"]}
    },
    {
      "name": "node-rules",
      "flags": ["instance", "device"],
      "expressions": [
        {"name": "cpu_utilisation", "promql": "instance:node_cpu_utilisation:rate1m", "flags": ["instance"]},
        {"name": "disk_io_time", "promql": "instance_device:node_disk_io_time_seconds:rate1m"}
      ]
    }
  ]
}
```

  `flags` declares each flag once: its name on the command line, the label it matches and whether it matches
  exactly or by regex. A query lists the flags it takes; an expression that lists `flags` of its own takes
  only those, since a `device` matcher on a series without that label matches nothing. Expressions are an
  array because their order is the output's order. A range in the file's PromQL (`[5m]`) is a placeholder
  that `--window` replaces. The optional `compare` names two expressions and the labels to pair them on: a
  label set present on the left and absent on the right is listed in the summary's `unpaired`, which is how
  `pod-cpu` reports pods with no limit. PromQL accepts single-quoted strings, and the shipped file uses them so
  that no quote inside a JSON string needs escaping. The shipped file's contents are the table below.

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
| NAMED-10 | `node-rules` | `--instance`, `--device` | the recording rules OpenShift's monitoring operator defines over node-exporter (upstream's node-mixin names them `rate5m`): `instance:node_cpu_utilisation:rate1m`, `instance:node_memory_utilisation:ratio`, `instance:node_network_receive_bytes_excluding_lo:rate1m`, `instance:node_network_transmit_bytes_excluding_lo:rate1m`, `instance:node_network_receive_drop_excluding_lo:rate1m`, `instance:node_network_transmit_drop_excluding_lo:rate1m`, `instance_device:node_disk_io_time_seconds:rate1m` and `instance_device:node_disk_io_time_weighted_seconds:rate1m`, each `{...}`; `--device` applies to the `instance_device` ones |
| NAMED-11 | `vm-filesystem` | `--namespace`, `--mount` | used: `kubevirt_vmi_filesystem_used_bytes{...} / kubevirt_vmi_filesystem_capacity_bytes{...}`; free: `1 -` the same; both by `(namespace, name, mount_point)` |

`...` stands for the matchers the flags add. `node-network` is always per interface: `--device` narrows which
interfaces, and a reader who wants a node total sums the result, since a sum over every interface would count
`lo` and the virtual pairs twice. For `pod-cpu` and `pod-memory`, the definition compares `used` with `limit`
by `namespace` and `pod`, and a pod that has a `used` series but no `limit` series is listed by name in the
summary, so a pod with no limit is reported rather than dropped by a division that finds nothing to divide by.

Two of these correct the working notes they come from, and one fixes them. `node-cpu` and `node-iowait` are
already per-CPU averages, so dividing by the CPU count again is wrong. `node-disk-write-latency` must use the
same matchers on both sides; with different instances on each side the division matches nothing.

## Output

- **OUTPUT-1: JSON lines with RFC 3339 times.** Stdout is one JSON object per line. Every time is RFC 3339 in
  UTC, where the Prometheus HTTP API has Unix seconds. Sample values are strings, as in the API, so `NaN` and
  `+Inf` survive. A native histogram sample carries the API's `histogram` object in place of the string.
- **OUTPUT-2: The summary comes first.** The first line is a `summary` record, and a reader who sees only the
  first few KiB sees everything that qualifies the result. Its fields:

  | Field | Holds |
  | --- | --- |
  | `command` | `info`, `query instant`, `query range`, `query series`, `query labels`, `query metadata` or `named NAME` |
  | `promql_version` | [QUERY-7](#queries) |
  | `dump` | `start`, `end` and `root` ([DUMP-5](#dumps)) |
  | `wal` | `state` (`replayed`, `read` for `info`, which decodes the WAL without replaying it, `skipped` under `--no-wal`, `failed` when a query's replay or `info`'s decoding hits a read error, or `absent`), `bytes`, `newest` (the newest sample in any readable record; null when the WAL holds no samples), and only when `failed`, `error` and `missing` with `start` (null when the dump holds no sample before the error) and `end` (null when open-ended) ([DAMAGE-3](#damage)) |
  | `gaps` | one entry per corrupt block the query overlaps, with `block`, `start` and `end` (both null when the block's `meta.json` doesn't read) and `error` ([DAMAGE-1](#damage)); every corrupt block for `info` |
  | `expression` or `expressions` | the expression as evaluated, or for `named` an array of `name` and `promql` |
  | `selectors` | for `query series` and `query labels`, the selectors as given, in place of an expression |
  | `time`, or `start`, `end` and `step` | the evaluation time or range, and the step that was used |
  | `series`, `points` | the counts in the result; a series listing has `series` only, a label listing neither |
  | `truncated` | null, or `limit` (`series` or `points`), `value` and `full` ([LIMIT-3](#limits)) |
  | `empty` | null, or `reason` and the `selectors` at fault, an empty list for `outside-dump-range` and `empty-evaluation` ([OUTPUT-4](#output)) |
  | `unpaired` | for a `compare` query, the label sets on the left with no match on the right ([NAMED-12](#named-queries)) |
  | `sample_interval` | [STEP-4](#steps); null when no series had two samples in the window |
  | `warnings` | each with `code` and `message`: the step warnings ([Steps](#steps)) and the engine's own annotations under the code `promql` |

- **OUTPUT-3: Records mirror the HTTP API.** After the summary, one record per line, each with a `kind`:

  | Kind | From | Holds |
  | --- | --- | --- |
  | `series` | a query, or `query series` | `metric` with the label set, then `value` (instant) or `values` (range) as `[time, "value"]` pairs, shaped like an entry of the API's `result`; `query series` gives `metric` only; a `named` record also names its `expression` |
  | `scalar`, `string` | a query whose result isn't a vector | `value` as one `[time, "value"]` pair |
  | `labels` | `query labels` | `names`, or with a `NAME` the `name` and its `values` |
  | `metadata` | `query metadata` | `metric`, `type`, `help` and `unit` |
  | `block` | `info` | `dir`, `start`, `end` (exclusive, as the block's `meta.json` gives them), `series`, `status` (`ok`, `corrupt` or `superseded`) and `error`; `start`, `end` and `series` are null when `meta.json` doesn't read, and `error` is null for a block that opened. Blocks come in start order, those without a range last |

- **OUTPUT-4: Say why a result is empty.** An empty result's summary carries one `empty` reason and the
  selectors it applies to. `outside-dump-range`: the query's time or range, before the lookback delta, range
  selectors and offsets widen it, doesn't overlap the dump. `no-matching-series`: a selector in the expression
  matches nothing in the dump at any time. `no-samples-in-range`: every selector matches series, but one of
  them has no sample the result could show in the window the engine reads for that selector. A stale marker,
  which Prometheus writes when a target disappears, isn't such a sample: an instant selector whose newest sample
  in its lookback is a stale marker shows nothing, so a target that went away just before the query time reads
  as `no-samples-in-range`. `empty-evaluation`: every selector has samples in the range and the expression still
  produced nothing, as when a comparison holds for no series or the two sides of a binary operation share no
  label set. The four are different answers, and the first is the one the web UI gives silently. The tool learns
  the selector facts from a pass over the expression's vector selectors after evaluation ([STEP-4](#steps)),
  each over its own window, so the side of `up - up offset 1d` that has no samples is the one named; for
  `no-matching-series` it selects over the dump's whole range and stops at the first match. A query's selector
  is named in the parser's printed form with its offset or `@` modifier, as `up offset 1d`. The listings use the
  same reasons and name a selector as given: an empty series listing is `outside-dump-range`,
  `no-matching-series` or `no-samples-in-range`, and so is an empty label listing. A label listing with a
  `NAME` covers only the series carrying that label, so each selector, or with none every series, is narrowed
  by `NAME!=""` and named in the parser's form: `query labels mode --promql up` on a dump whose `up` has no
  `mode` label is `no-matching-series` naming `up{mode!=""}`. A listing of label names without a selector is
  empty only where the dump has no samples, so it is `outside-dump-range` or `no-samples-in-range`.
- **OUTPUT-5: A failed run says why on stdout.** A run that exits 1 or 3 writes one `error` record as its only
  stdout line, with `exit`, `message`, and for exit 3 the `limit` and its `value`. The tool finishes its work,
  closing the dump and removing its run directory, before it writes the result, so the one failure that can
  follow part of a result is writing stdout itself; the lines already written then stand, and no `error` record
  follows them, since it would join a cut line.

An instant query's output looks like this, with the summary on one line:

```
{"kind":"summary","command":"query instant","promql_version":"3.15.1",
 "dump":{"start":"2025-12-30T00:00:00Z","end":"2026-01-01T00:04:00Z","root":"data"},
 "wal":{"state":"replayed","bytes":1048576,"newest":"2026-01-01T00:04:00Z"},"gaps":[],
 "expression":"up","time":"2026-01-01T00:04:00Z","series":2,"points":2,"truncated":null,"empty":null,
 "sample_interval":"15s","warnings":[]}
{"kind":"series","metric":{"__name__":"up","instance":"node-a","job":"node-exporter"},"value":["2026-01-01T00:04:00Z","1"]}
{"kind":"series","metric":{"__name__":"up","instance":"node-b","job":"node-exporter"},"value":["2026-01-01T00:04:00Z","0"]}
```

The output's shape is interface: a test may pin it, and changing it is an interface change.

## Limits

Memory is the real constraint. WAL replay holds the head's samples in memory, and a wide range query over many
series holds every point it returns. The limits bound the query; the WAL is bounded by knowing its size before
replay and by `--no-wal`. The runtime's soft memory limit isn't offered: it can't stop an out-of-memory kill,
only slow the process down near it, and a sandbox that caps memory already does so with its own cgroup. Hitting
a limit is reported, never silent.

- **LIMIT-1: Series limit.** A result has at most `--max-series` series. The default keeps the output readable
  and, with LIMIT-2, under the sandboxed caller's cap.
- **LIMIT-2: Point limit.** A range result has at most `--max-points` points across all series. The default is
  chosen so that a full result stays under 1 MiB of JSON lines, which is why the two limits interlock: at
  STEP-1's 500 points per series, the point limit holds forty series, so a day-long query over a hundred series
  is cut by points before series. The truncation record says which limit cut and how many whole series the
  reader got; a reader who wants every series over a long range aggregates or raises `--max-points`.
- **LIMIT-3: Truncation is a record.** When a limit cuts a result, the summary's `truncated` carries which
  limit, the value, and how many series or points the full result had. Series are dropped whole, never cut
  partway, and the ones kept are the first in label order: the engine sorts a range result by labels, and the
  tool sorts an instant result the same way before cutting, so the cut is the same on every run. That is the
  alphabetically first series, not the largest; a reader after the largest asks for `topk`.
- **LIMIT-4: Samples.** `--max-samples` is the engine's bound on samples one query may load, the same as
  Prometheus's `--query.max-samples`; a query over it stops with the engine's error, exit code 3 and an
  `error` record naming the limit. Before replaying the WAL the tool reports the WAL's size on stderr, so an
  engineer on a small host can choose `--no-wal`.

## Steps

OpenShift's monitoring operator scrapes node-exporter every 15s, the kubelet and cAdvisor every 30s and
kube-state-metrics every 1m, so the node queries run on 15s samples, the pod queries on 30s samples and the
limits on 1m samples. A spike that rises and falls between two samples is not in the dump at all. A step
coarser than the samples hides more: a 5m step over days smooths peaks. A step finer than the samples adds
nothing, since PromQL repeats the last sample at each extra point.

- **STEP-1: A default step from the range.** When `--step` is unset, the step is the smallest of 1m, 5m, 15m,
  1h and 6h that yields at most 500 points per series over the range. The floor is 1m rather than the finest
  scrape interval so that the default two-hour query over a hundred series fits the point limit; `--step 15s`
  is there for a close look at a few series. The summary carries the step either way.
- **STEP-2: Warn when the step is coarse.** When the step is longer than the observed sample interval, the
  summary warns `step-coarse`, saying how many samples each step skips and that peaks shorter than the step are
  invisible. The default step warns too, on 15s and 30s data, because the default is chosen for output size,
  not fidelity, and the reader should know what it hides.
- **STEP-3: Warn when the step is finer than the data.** When the step is shorter than the observed sample
  interval, the summary warns `step-finer-than-data`, saying the extra points repeat the same samples.
- **STEP-4: Say what the samples can show.** Every query summary carries `sample_interval`, the median gap
  between consecutive raw samples of the expression's vector selectors over the window the engine reads for the
  query: the range, or the time for an instant query, widened by the lookback delta, range selectors and offsets.
  A gap of a second or more is rounded to the second, so scrape jitter doesn't split one interval in two. A
  reader learns the finest view the dump can give. The tool gets it from a pass over the selectors after
  evaluation, which also serves OUTPUT-4. The pass doesn't read every sample again: for each series it reads up
  to 8 gaps from each of 4 points spread evenly across the window, so samples further apart than a quarter of the
  window still have their gap read. Reaching each point still decodes the chunks before it, so the pass costs
  more over a longer range. When the median gap differs between those points, as when a scrape interval changed
  or scrapes stopped partway, the summary warns `sample-interval-varies` with the median at each point, since one
  interval then misdescribes the window.

## Fixtures

The repo's test dumps are synthetic, built with Prometheus's `tsdb` package: a few blocks, a WAL written one
commit per scrape time as Prometheus writes it, with enough samples that the head has cut at least one
`chunks_head/` file (the chunk mapper creates them lazily, and a fixture without one would pass DUMP-4 on a dump
that fails), and copies with a block's index cut short or its `meta.json` unreadable, a compacted block beside
the blocks it supersedes, a WAL record whose checksum fails partway, and the layouts of DUMP-3. The fixture for QUERY-5 is written with the head's metadata WAL
records option on. A real dump can't be committed ([Privacy](standards.md#privacy)), so synthetic fixtures
don't prove:

- that the named queries' metric and label names, checked against the exporters' documentation and the
  monitoring operator's rule files, are what a real dump holds, so each expression is checked against a real
  dump by hand, with `--promql` on the metric names it uses;
- the layout and contents of a dump captured by the Red Hat procedure, so DUMP-3's list of layouts grows from
  real dumps;
- the kinds of damage real dumps carry, so DAMAGE-1 and DAMAGE-3 are checked against real damaged dumps when
  one turns up;
- the WAL sizes and memory use of a busy cluster, so the limit defaults are tuned from real runs;
- whether the embedded PromQL version answers as the cluster's would, which is an unknown below.

## Unknowns

What the spec assumes without having checked, to verify at the pinned Prometheus version:

- That the embedded release reads blocks written by every Prometheus version OpenShift ships, including
  features the older cluster didn't have, such as native histograms and out-of-order samples. Native histogram
  samples are scraped only when a config setting that defaults off is set, so a dump probably has none today.
- That the embedded engine evaluates a support query as an older cluster's engine would. Prometheus 3.0's
  migration guide lists the PromQL changes from 2.x; which of them a support query would notice is unchecked
  (see [stack.md](stack.md#adr-003-one-embedded-promql-version)).

## Open questions

Raised here, not decided:

- **The binary's name.** `package main` at the root makes the binary the repo's name, and
  `prometheus-tsdb-reader` is long to type at a support console. The candidate is `tsdbr`: short, lowercase,
  and it says what it is. Taking it means renaming the repo and module so the three agree, or keeping the repo
  name and building the binary from `cmd/tsdbr`, which [standards.md](standards.md#known-deviations) would
  record as a deviation from one binary at the root.
