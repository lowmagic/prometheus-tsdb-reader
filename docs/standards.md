# Coding standards

prometheus-tsdb-reader follows the [org standards](org-standards.md) and the standards for its topics
([go](org-standards-go.md)), unchanged copies of
[junglestyle's](https://github.com/junglestyle/.github/tree/main/code-standards). `.org-standards` lists the
topics it copies beyond the general one. This file adds prometheus-tsdb-reader's own rules and examples.
Where it differs from an org rule, it says so, and it wins.

## Dependencies

- Prometheus's own `tsdb`, `storage` and `promql` packages are the stack
  ([ADR-002](stack.md#adr-002-build-on-prometheuss-tsdb-and-promql-packages)). Nothing in this repo parses a
  block, an index, a chunk or a WAL, and nothing evaluates PromQL, except by calling them.
- The binary builds with `CGO_ENABLED=0`. A dependency that needs cgo is out.

## Structure

The layers, in the order data flows. Each one talks to the next through an explicit interface and knows
nothing of the others' internals. One package per layer, named for the layer; whether they live under
`internal/` is an [open question](SPEC.md#open-questions).

| Layer | Owns | Mustn't know |
| --- | --- | --- |
| `dump` | Accepting a path or archive, extracting safely under scratch, finding the TSDB root, listing blocks and the WAL | PromQL, the output format, flags |
| `store` | Opening the TSDB read-only with the scratch root, skipping corrupt blocks, replaying or skipping the WAL, the dump's range and gaps | archives and layouts, PromQL text, the output format |
| `query` | Running an expression through the engine, time defaults, step defaults and warnings, limits and truncation, empty reasons | files, archives, flags, JSON |
| `named` | Turning typed values into escaped matchers and the spec's expressions | storage, output, flags |
| `render` | Writing the summary and records as JSON lines | where a result came from, the engine |
| CLI (`main`) | Flags, wiring the layers, exit codes, stderr progress | the internals of any of them |

- One code path evaluates a query, whether it came from `--promql` or a named query: `named` produces an
  expression and `query` runs it. A named query is never a second evaluator.
- The only place a PromQL string is built from parts is `named`. Nowhere else concatenates matchers.
- Scratch is passed down from `main`. No layer picks its own writable place, and no layer writes under the
  dump.

## Errors and recovery

- A damaged block or WAL is a reported gap ([Damage](SPEC.md#damage)), never a swallowed error and never a
  fatal one. The one place that catches a `tsdb` open error decides between the two and records which.
- A limit that cuts a result produces a truncation record. Cutting without the record is a bug.

## Testing

- The behaviors worth a test: safe extraction (traversal, links out, size), finding the TSDB root in each
  layout, never writing under the dump, a corrupt block becoming a gap, `--no-wal` and a failed WAL being
  reported, the three empty reasons, each limit and its truncation record, the step warnings, matcher escaping,
  the PromQL version in the summary, and the output's shape.
- The JSON lines output is interface: another program reads it, so a test may pin its shape. Warning and
  progress text is checked by meaning (the warning's code), not its wording.
- Fixtures are synthetic, built with the `tsdb` package in a test helper, never checked in as binary blobs
  from a real dump. The [spec](SPEC.md#fixtures) says what they don't prove.

## Privacy

- No real dump, or any part of one, enters the repo: no block, no WAL, no label set and no sample. The names,
  namespaces and instances in fixtures and docs are invented.
- Docs describe the automated caller by its constraints ([RUN-2](SPEC.md#running-it)) and environments by
  role, such as "a host with no container runtime".

## Docs

- The [spec](SPEC.md) is where the docs describe what a user can observe: flags, defaults, warning codes,
  empty reasons and the output's shape. The README shows how to run it and links there.
- The [roadmap](ROADMAP.md) cites spec IDs and doesn't restate requirements.

## Prose

- The notes the named queries come from and the places engineers work appear only by role, never by case,
  customer, cluster or machine name.

## Known deviations

Where prometheus-tsdb-reader doesn't follow the org standards yet. These are debt to pay down, not overrides:
each one should go.

None yet.
