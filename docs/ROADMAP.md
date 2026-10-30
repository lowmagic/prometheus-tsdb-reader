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
untouched. The binary they ran is a static build. The store opens the blocks and builds the head from the
WAL itself, the way [ADR-002](stack.md#adr-002-build-on-prometheuss-tsdb-and-promql-packages) describes, so
nothing in this slice is replaced later.

Delivers: RUN-1, RUN-3's exit codes 0 and 1, RUN-4, DUMP-4, QUERY-1, QUERY-6, QUERY-7, OUTPUT-1, OUTPUT-2,
OUTPUT-3, OUTPUT-5, plus the first synthetic [fixture](SPEC.md#fixtures) and the `make check` that builds with
`CGO_ENABLED=0`.

Done when: against the synthetic fixture, the command above prints a summary that names the expression, the
time and the PromQL version in release form, then the series; the dump directory's modification times and
contents are unchanged afterwards and the run's scratch directory is gone; `ldd` reports the binary isn't
dynamic; and the fixture has a `chunks_head/` file, so the copy was exercised.

## 2. Any dump: archives, layouts and what the dump covers

A user can hand the tool a `.tar.gz` as captured, or a directory with the blocks under `data/` or
`prometheus/`, and ask what it holds:

```sh
prometheus-tsdb-reader --dump ./dump.tar.gz info
```

They see the dump's start and end, its blocks and its WAL, with the WAL's newest sample found without a replay.
An archive that tries to write outside the scratch directory is refused with the entry named, and stderr says
where the extraction landed so the next run can start from it.

Delivers: DUMP-1, DUMP-2, DUMP-3 with `--root`, DUMP-5, RUN-2's write boundary.

Done when: `info` on a fixture archive in each layout reports the same range; a crafted archive with a `..`
entry, a link out and an oversize entry is refused with the reason and exit code 1; `--root` resolves a
fixture with two candidate roots; and, demonstrated by hand, a dump on a read-only mount with `--scratch` on
another filesystem answers.

## 3. Range queries, series and labels, with time defaults and empty reasons

A user can run a range query with no time flags and get the dump's last two hours at a sensible step, list
series and labels, and when a result is empty, read why:

```sh
prometheus-tsdb-reader --dump ./dump query range --promql 'rate(node_cpu_seconds_total{mode="idle"}[5m])'
prometheus-tsdb-reader --dump ./dump query series --promql 'up'
prometheus-tsdb-reader --dump ./dump query labels instance
```

Delivers: QUERY-2, QUERY-3, QUERY-4, QUERY-8, OUTPUT-4, STEP-1, STEP-2, STEP-3, STEP-4.

Done when: on a fixture with 1m samples, a range query with no flags ends at the dump's end with a 1m step and
no step warning, and `--step 15m` warns `step-coarse` while `--step 10s` warns `step-finer-than-data`; on a
fixture with 15s samples the default step warns `step-coarse` and the summary's `sample_interval` is 15s; a
query ending a day after the dump reports `outside-dump-range`; a query for a metric the fixture lacks reports
`no-matching-series` naming the selector; a query for a series that stops before the range reports
`no-samples-in-range`; and `up > 5` reports `empty-evaluation`.

## 4. Damaged dumps

A user with a dump Prometheus won't start on, or would repair by discarding part of the WAL, gets answers from
the readable parts and a record of the rest: a corrupt block becomes a gap in the summary, `--no-wal` skips a
WAL too big for the host, and a WAL that fails partway is replayed up to the failure and reported as the newest
samples missing.

Delivers: DAMAGE-1, DAMAGE-2, DAMAGE-3.

Done when: on the fixture with a corrupt block, a range query over it answers with that block's range in
`gaps`; on the fixture with a block that a compacted block supersedes, the block is listed and the summary has
no gap; on the fixture whose WAL is unreadable partway, the query returns the samples written before the
damage, and the summary's `wal.missing` starts at the newest sample kept, ends at the newest readable
timestamp after the damage, and names the error; and `--no-wal` on a healthy fixture moves the dump's end
back to the newest block and says so.

## 5. Bounds

A user who asks for too much gets a result that fits, with a record of what was cut, and a user on a small
host sees the WAL's size before replay starts. The defaults keep a full result under the sandboxed caller's
cap.

Delivers: LIMIT-1, LIMIT-2, LIMIT-3, LIMIT-4, RUN-3's exit code 3, and the rest of RUN-2. The spec sets no
memory flag, so this slice has nothing to demonstrate for memory beyond the WAL's size on stderr.

Done when: a range query over more series than `--max-series` returns that many whole series, the first in
label order, and a `truncated` record with the full count; the same for points; an instant query over the
limit is cut in label order on every run; a query over `--max-samples` exits 3 with an `error` record naming
the limit; a run of every command with the default limits on a fixture wider than the limits writes under
1 MiB; and the WAL's size appears on stderr before replay.

## 6. Named queries for pods and nodes

A user, or the sandboxed caller, gets the pod and node views from typed flags with no PromQL in argv:

```sh
prometheus-tsdb-reader --dump ./dump named pod-cpu --namespace shop --pod 'checkout-.*'
prometheus-tsdb-reader --dump ./dump named node-cpu --window 5m --start 6h
```

The summary shows the exact expression that ran, and a pod with no limit is named in the summary rather than
missing. The queries come from a file the binary embeds, and `--queries` points at an edited copy.

Delivers: NAMED-1 through NAMED-9 and NAMED-12.

Done when: each named query runs against a fixture carrying its metrics, and the summary's expression parses to
the same syntax tree as the spec's table entry with the flag matchers added (the printer sorts matchers and
normalizes spacing, so the comparison is of parsed forms, not text); a `--namespace` value with a quote and a
backslash in it matches a fixture series carrying that namespace; a fixture pod with no limit series appears
in the summary's `unpaired`; `node-disk-write-latency` with `--instance` set returns a result on a fixture
where both sides exist; and a copy of the shipped file with one query added runs that query through
`--queries`.

## 7. Recording rules, VM filesystems and metadata

A user gets the node-exporter recording rules and the KubeVirt filesystem view by name, and can list what
metadata the dump carries, if any.

Delivers: NAMED-10, NAMED-11, QUERY-5.

Done when: `named node-rules` and `named vm-filesystem` run against fixtures carrying those series with the
expressions the spec gives, under the same parsed-form comparison as slice 6; `query metadata` on a fixture
without metadata says the dump carries none; and on a fixture written with metadata WAL records on, it lists
them.

## 8. Checked against a real dump

An engineer runs the named queries against a real dump, outside the repo, and the expressions' metric and
label names are confirmed or corrected. This slice resolves what the [fixtures](SPEC.md#fixtures) section says
synthetic fixtures can't prove and tunes the limit defaults from real WAL sizes; nothing from the dump enters
the repo, so nobody can demonstrate this slice from the repo alone. Its record is the commit that changes the
spec and the code, or that says nothing needed changing.

Delivers: the parts of the [fixtures](SPEC.md#fixtures) section that synthetic fixtures can't.

Done when: every named query has been run on a real dump and either matched series or had its expression
corrected in the spec and the code, and DUMP-3's layouts include every layout seen.

To run it, outside the repo and with the dump on the engineer's machine:

1. `info` on the archive, and note the root it chose and the WAL's size. A layout DUMP-3 doesn't find is a
   change to DUMP-3.
2. Each `named` query with no flags, then with one `--namespace` or `--instance` from the dump. For an
   expression whose summary `empty` is `no-matching-series`, run `query series --promql` on each metric name it
   uses and `query labels NAME --promql METRIC` on each label it matches on, to find the real name.
3. `vm-filesystem` on a dump from a cluster running KubeVirt, and `query metadata` on one, if any, written with
   metadata WAL records on.
4. Each command with the default limits, and note the bytes written and the run's peak memory against the
   WAL's size; tune the defaults if a result passes 1 MiB.
5. Record what changed, or that nothing did, in one commit to the spec, `named/queries.json` and the tests. No
   label set, sample or host name from the dump goes in it.
