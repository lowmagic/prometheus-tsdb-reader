# Roadmap

The order prometheus-tsdb-reader gets built in. Each slice is usable on its own, names what a user would notice
and the [spec](SPEC.md) requirements it delivers, and has a done-when someone can demonstrate. A requirement
not yet in a slice stays in the spec and waits for one here.

## 1. Tracer bullet: one instant query from a directory

A user can point the binary at an extracted dump whose blocks sit at the top level and run one instant query:

```sh
prometheus-tsdb-reader --dump ./dump --scratch /tmp/work query instant --promql up
```

They get a summary line and one series record per series on stdout, progress on stderr, and the dump is
untouched. The binary they ran is a static build.

Delivers: RUN-1, RUN-3, DUMP-4, QUERY-1, QUERY-6, QUERY-7, OUTPUT-1, OUTPUT-2, OUTPUT-3, plus the first
synthetic [fixture](SPEC.md#fixtures) and the `make check` that builds with `CGO_ENABLED=0`.

Done when: against the synthetic fixture, the command above prints a summary that names the expression, the
time and the PromQL version, then the series, and the dump directory's modification times and contents are
unchanged afterwards. The same binary, copied to a host with no Go toolchain, runs.

## 2. Any dump: archives, layouts and what the dump covers

A user can hand the tool a `.tar.gz` as captured, or a directory with the blocks under `data/` or
`prometheus/`, and ask what it holds:

```sh
prometheus-tsdb-reader --dump ./dump.tar.gz info
```

They see the dump's start and end, its blocks and its WAL. An archive that tries to write outside the scratch
directory is refused with the entry named.

Delivers: DUMP-1, DUMP-2, DUMP-3, DUMP-5, RUN-2's write boundary.

Done when: `info` on a fixture archive in each layout reports the same range; a crafted archive with a `..`
entry, a link out and an oversize entry is refused with the reason; and a run with a read-only dump and a
writable scratch succeeds.

## 3. Range queries, series and labels, with time defaults and empty reasons

A user can run a range query with no time flags and get the dump's last two hours at a sensible step, list
series and labels, and when a result is empty, read why:

```sh
prometheus-tsdb-reader --dump ./dump query range --promql 'rate(node_cpu_seconds_total{mode="idle"}[5m])'
prometheus-tsdb-reader --dump ./dump query series --promql 'up'
prometheus-tsdb-reader --dump ./dump query labels instance
```

Delivers: QUERY-2, QUERY-3, QUERY-4, QUERY-8, OUTPUT-4, STEP-1, STEP-2, STEP-3, STEP-4.

Done when: on a fixture with 1m samples, a range query with no flags ends at the dump's end with a 1m step; a
query ending a day after the dump reports `outside-dump-range`; a query for a metric the fixture lacks reports
`no-matching-series`; a query for a series that stops before the range reports `no-samples-in-range`; and
`--step 15m` warns `step-coarse` while `--step 10s` warns `step-finer-than-data`.

## 4. Damaged dumps

A user with a dump Prometheus refuses to start on gets answers from the readable parts and a record of the
rest: a corrupt block becomes a gap in the summary, `--no-wal` skips a WAL too big for the host, and a WAL that
fails to replay is reported as the newest samples missing.

Delivers: DAMAGE-1, DAMAGE-2, DAMAGE-3.

Done when: on the fixture with a corrupt block, a range query over it answers with that block's range in
`gaps`; on the fixture with an unreadable WAL, the query answers from the blocks and the summary names the
missing span and the error; and `--no-wal` on a healthy fixture moves the dump's end back to the newest block
and says so.

## 5. Bounds

A user who asks for too much gets a result that fits, with a record of what was cut, and a user on a small
host sees the WAL's size before replay starts. The defaults keep a full result under 1 MiB, which is what the
sandboxed caller keeps.

Delivers: LIMIT-1, LIMIT-2, LIMIT-3, LIMIT-4, and the rest of RUN-2.

Done when: a range query over more series than `--max-series` returns that many whole series and a `truncated`
record with the full count; the same for points; a query over `--max-samples` fails with exit code 1 and the
engine's error; a run of every command with the default limits on a fixture wider than the limits writes under
1 MiB; and the WAL's size appears on stderr before replay.

## 6. Named queries for pods and nodes

A user, or the sandboxed caller, gets the pod and node views from typed flags with no PromQL in argv:

```sh
prometheus-tsdb-reader --dump ./dump named pod-cpu --namespace openshift-monitoring --pod 'prometheus-k8s-.*'
prometheus-tsdb-reader --dump ./dump named node-cpu --window 5m --start 6h
```

The summary shows the exact expression that ran, and a pod with no limit is named in the summary rather than
missing. The queries come from a file the binary embeds, and `--queries` points at an edited copy.

Delivers: NAMED-1 through NAMED-9 and NAMED-12.

Done when: each named query runs against a fixture carrying its metrics and the summary's expression is the
one the spec's table gives; a `--namespace` value with a quote and a backslash in it produces a correctly
escaped matcher; a fixture pod with no limit series appears in the summary's list; `node-disk-write-latency`
with `--instance` set returns a result on a fixture where both sides exist; and a copy of the shipped file with
one query added runs that query through `--queries`.

## 7. Recording rules, VM filesystems and metadata

A user gets the node-exporter recording rules and the KubeVirt filesystem view by name, and can list what
metadata the dump carries, if any.

Delivers: NAMED-10, NAMED-11, QUERY-5.

Done when: `named node-rules` and `named vm-filesystem` run against fixtures carrying those series with the
expressions the spec gives; `query metadata` on a fixture without metadata says the dump carries none; and on a
fixture whose WAL holds metadata records, it lists them.

## 8. Checked against a real dump

An engineer runs the named queries against a real dump, outside the repo, and the expressions' metric and
label names are confirmed or corrected. This slice resolves the spec's [unknowns](SPEC.md#unknowns) about
metric names and dump layouts and tunes the limit defaults from real WAL sizes; nothing from the dump enters
the repo.

Delivers: the parts of the [fixtures](SPEC.md#fixtures) section that synthetic fixtures can't.

Done when: every named query has been run on a real dump and either matched series or had its expression
corrected in the spec and the code, and DUMP-3's layouts include every layout seen.
