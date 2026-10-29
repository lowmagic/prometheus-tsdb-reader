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
reimplementing the block format, the index, the WAL or the query language. `go.mod` pins the Prometheus
module; the facts below hold at `v0.315.1` and are re-checked whenever the pin moves.

The engine runs over any `storage.Queryable`: `promql.Engine.NewInstantQuery` and `NewRangeQuery` take one as
an argument. Neither `promtool query`, which takes a server URL, nor `prometheus/client_golang`, which is
instrumentation plus an HTTP client, can read a dump, so neither is a shortcut.

The `tsdb` package has a read-only opener, `tsdb.OpenDBReadOnly`, and the tool doesn't use it. It is where the
facts the design must honor are visible, and four of them rule it out:

- Its querier hard-links every `chunks_head/` file into a sandbox directory with `os.Link`, with no fallback,
  because the head that replays the WAL cuts, truncates and removes chunk files. The sandbox goes inside the
  data directory unless given another root, and a hard link can't cross filesystems, so a dump on a read-only
  mount or another volume than scratch fails to open. This is why DUMP-4 copies the files itself.
- Its block listing fails as a whole when any block is corrupt, unless a compacted child supersedes it. DAMAGE-1
  needs the readable blocks.
- Each querier it hands out replays the WAL again, and it supports one querier at a time. A named query with
  two expressions would replay twice.
- It exposes neither the head's newest timestamp, which DUMP-5 reports, nor the metadata records the WAL
  carries, which QUERY-5 lists.

The pieces it is built from are exported, so the `store` layer assembles the same queryable itself:
`tsdb.OpenBlock` on each block directory, catching the error per block; `tsdb.NewHead` over `wlog.Open` on the
dump's `wal/` directory with the chunk directory under scratch, then `Head.Init` from the latest end of a
block cut from in-order samples, as Prometheus's own open sets it,
keeping the head on a replay error because it holds what was replayed before it; `tsdb.NewBlockQuerier` over
each block and over `tsdb.NewRangeHead`, joined by `storage.NewMergeQuerier`. The newest timestamp and the
metadata come from one decoding pass over the WAL with `tsdb/wlog`'s segment reader and `tsdb/record`'s
decoder, which builds no head. The one thing the library would do that the tool doesn't is repair: Prometheus's
own server truncates a WAL after a read error, and the tool reports the loss instead ([DAMAGE-3](SPEC.md#damage)).

What the choice costs: the Prometheus module is large, so the binary is tens of megabytes and the build pulls
a long dependency tree; the module's version decides the PromQL version (ADR-003); and the `store` layer
reaches into `tsdb` at several points, each of which a module bump may move.

## ADR-003: One embedded PromQL version

The binary embeds one Prometheus release's engine: the current stable release at the time `go.mod` is pinned,
bumped deliberately and not by every dependency update. Every result names that version (QUERY-7), because the
cluster that wrote the dump ran whichever release its OpenShift version ships, and the two may differ.

Pinning the current release rather than the one a given cluster runs is a bet that one engine reads every
dump. The code supports it: the index reader accepts index formats 1 to 3 while the writer emits 2, so a newer
reader opens what any older writer produced. (Prometheus 3's migration guide states the other direction, that a
TSDB written by 3.x needs at least 2.55 to read it, which doesn't bear on this choice.) The current monitoring
operator ships Prometheus 3.15.1, so "the cluster's older engine" means older OpenShift releases. Whether the
newer engine evaluates a support query the same way the cluster's would is an [unknown](SPEC.md#unknowns) the
spec records. One build per Prometheus release is the fallback if a checked difference matters, and this record
changes then.

The release's semver covers the server, not the Go module. Prometheus tags the module at major version 0, which
is Go's signal that the API may change between any two versions, and the mapping is mechanical: a 3.x release
`3.MINOR.PATCH` is module `v0.3MINOR.PATCH` (3.15.1 is `v0.315.1`), and a 2.x release `2.MINOR.PATCH` is
`v0.MINOR.PATCH`. QUERY-7 reports the release form, read back from the module version in the binary's build
info, because `prometheus/common`'s version variable is set only by Prometheus's own build and is empty in a
library build. Each bump of the pin is expected to touch the `store` and `query` layers, and the pin moves on
its own commit, with the API changes it needed.

## ADR-004: Named queries are data

The named queries live in a JSON file the binary embeds, and `--queries` reads another in its place
([NAMED-12](SPEC.md#named-queries)). An engineer adds a query by editing a file, with no release. JSON because
`encoding/json` is in the stdlib and the file is small, and because PromQL's single-quoted strings keep a
matcher free of escapes inside a JSON string, so YAML would buy nothing for its dependency. The flags don't
reach the expression as text: the tool parses the file's PromQL with Prometheus's parser, adds matchers to the
syntax tree and prints it back, so escaping is the parser's and there is no string templating to get wrong.
The file's shape is interface, and changing it is an interface change.
