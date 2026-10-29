# prometheus-tsdb-reader

Reads a Prometheus TSDB dump offline and answers PromQL the way Prometheus does.

A support engineer gets a TSDB dump from an OpenShift cluster, blocks plus a write-ahead log in a `.tar.gz`,
and wants to know what the cluster was doing around an incident. prometheus-tsdb-reader is one static binary
that opens the dump on a host with nothing else installed and no network, runs instant and range queries,
lists series, labels and metadata, and says what the dump covers and what it's missing. It can also be run by
an automated caller that passes whole argument values and never builds a PromQL string, through named queries
with typed flags.

## What it does

- **Answers PromQL from a dump.** Instant and range queries, series, labels and metadata, with the CLI
  modeled on `promtool query` and the output on the Prometheus HTTP API's response shape, as JSON lines with
  RFC 3339 times.
- **Never writes under the dump.** Prometheus's reader writes scratch files even when read-only; the tool
  puts them under `--scratch` (default: the working directory), so a read-only mount works.
- **Opens what Prometheus won't.** A corrupt block is skipped and reported as a gap. The WAL can be skipped,
  and a WAL that fails to replay is reported as the newest samples missing, not deleted by hand.
- **Says why a result is empty.** A range outside the dump, a selector nothing matches, and a series with no
  samples in the range are three different answers, and every result states the dump's start and end.
- **Bounds everything.** Series and point limits with a truncation record when one is hit, a sample limit for
  the engine, and defaults that keep a result under 1 MiB.
- **Warns about steps.** Samples are about a minute apart, so a step coarser than that hides peaks and a
  finer one adds nothing. The summary says which.
- **Named queries from typed flags.** Pod CPU and memory against limits, node CPU, iowait, load, memory,
  network and disk latency, the node-exporter recording rules, and KubeVirt VM filesystems, each built from
  flags such as `--namespace`, `--pod` and `--window` with proper escaping. They come from a file the binary
  embeds, and `--queries` reads an edited one. Free-form PromQL sits behind `--promql`, one flag a caller can
  deny.
- **Names its PromQL version.** The embedded engine is one Prometheus release, not necessarily the cluster's,
  and every result says which.

## Running it

Build a static binary and copy it to the host with the dump:

```sh
CGO_ENABLED=0 go build -o prometheus-tsdb-reader .
```

Then ask what the dump covers, run a named query, and run your own PromQL:

```sh
prometheus-tsdb-reader --dump ./dump.tar.gz --scratch /tmp/work info
prometheus-tsdb-reader --dump ./dump.tar.gz --scratch /tmp/work named node-cpu --start 6h --window 5m
prometheus-tsdb-reader --dump ./dump.tar.gz --scratch /tmp/work query range \
  --promql 'sum by (namespace) (rate(container_cpu_usage_seconds_total[5m]))' --start 2h --step 1m
```

Each command prints a summary line first, then one JSON record per series; progress goes to stderr. Times
default to the dump's end, not to now. The [specification](docs/SPEC.md) lists every command, flag, default,
warning and empty reason; the [roadmap](docs/ROADMAP.md) gives the order they get built in; and
[stack and decisions](docs/stack.md) records why it's Go on Prometheus's own packages. The packages under
the CLI are a public Go API.

`make check` runs the checks CI runs, offline. `make standards` checks the org standards copies and needs the
network.

MIT. See [LICENSE](LICENSE).
