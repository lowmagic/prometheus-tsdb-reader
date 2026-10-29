# Stack and decisions

| Layer | Choice |
| --- | --- |
| Language | Go, stdlib `flag`, `encoding/json`, `log/slog` |
| Storage and query | `github.com/prometheus/prometheus`: its `tsdb`, `storage` and `promql` packages |
| Build | `CGO_ENABLED=0`, one static binary per OS and architecture |
| Development | golangci-lint, `go test -race`, synthetic fixtures built with `tsdb` |
| License | MIT |

## ADR-001: Go

The [spec](SPEC.md#running-it) wants one static binary that runs on a host with nothing installed and no
network (RUN-1), and that an automated caller can run with argv only (RUN-2). Go cross-compiles to a static
binary for every host the engineers use, and Prometheus itself is written in Go, so the code that reads its
storage format and evaluates its query language is available as a library in the same language. No other
language has that, so a different choice would mean reimplementing the TSDB format or shelling out to a
Prometheus binary, which is the situation the tool exists to end.

## ADR-002: Build on Prometheus's `tsdb` and `promql` packages

The tool answers PromQL the way Prometheus does ([Queries](SPEC.md#queries)). The only way to be sure of that
is to run Prometheus's own engine over Prometheus's own storage reader, so the tool imports them rather than
reimplementing the block format, the index, the WAL or the query language. The facts the design rests on,
checked against prometheus/prometheus at commit d690972 and to re-check at the version `go.mod` pins:

- `tsdb.OpenDBReadOnly(dir, sandboxDirRoot, logger)` (`tsdb/db.go:577`) opens a data directory without a
  lock. It creates its scratch directory inside the data directory unless given another root, and opening a
  querier (`db.go:665`) hard-links the head chunk files into it and replays the WAL there. "Read-only"
  therefore still writes, which is why the spec's DUMP-4 always passes a root under `--scratch`.
- `promql.Engine.NewInstantQuery` and `NewRangeQuery` (`promql/engine.go:788` and `:835`) accept any
  `storage.Queryable`, so the engine runs over the read-only DB unchanged.
- `promtool query` always needs a server URL (`cmd/promtool/main.go:184`), so it can't query a dump, and
  `prometheus/client_golang` is instrumentation plus an HTTP client and can't read a TSDB. Neither is a
  shortcut.

What the choice costs: the Prometheus module is large, so the binary is tens of megabytes and the build pulls
a long dependency tree; the module's version decides the PromQL version (ADR-003); and the `tsdb` package's
behavior on damaged input is Prometheus's, so tolerating damage ([Damage](SPEC.md#damage)) means opening
blocks one at a time around its errors rather than relying on it to skip them.

## ADR-003: One embedded PromQL version

The binary embeds one Prometheus release's engine: the newest stable release at the time `go.mod` is pinned,
bumped deliberately and not by every dependency update. Every result names that version (QUERY-7), because the
cluster that wrote the dump ran whichever release its OpenShift version ships, and the two may differ.

Pinning the newest release rather than the one a given cluster runs is a bet that one engine reads every
dump: Prometheus 3's migration guide says 3.x reads storage written by 2.x, and a newer reader is at least as
new as any cluster's writer, so a dump never uses a storage feature the reader lacks. The bet in the other
direction, that the newer engine evaluates a support query the same way the cluster's would, is the
[open question](SPEC.md#open-questions) the spec records. Prometheus 3.0's migration guide lists the PromQL
changes from 2.x; which of them a support query would notice is unknown until checked. If the differences
matter, the alternative is one build per Prometheus release that OpenShift ships, with the release in the
binary's name, and this record changes.
