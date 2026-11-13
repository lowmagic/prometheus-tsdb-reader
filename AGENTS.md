# Working on prometheus-tsdb-reader

Reads a Prometheus TSDB dump offline and answers PromQL the way Prometheus does.

Read before changing behavior:

- [Specification](docs/SPEC.md): what prometheus-tsdb-reader does, with the requirement IDs the roadmap cites
- [Roadmap](docs/ROADMAP.md): the order it gets built in, one usable slice at a time
- [Coding standards](docs/standards.md): how the code should read and which layer mustn't know what, on top of
  the org's [principles](https://github.com/lowmagic/.github/blob/main/PRINCIPLES.md) and standards
  (`docs/org-standards*.md`)
- [Stack and decisions](docs/stack.md): Go on Prometheus's own `tsdb` and `promql` packages, and why
- [README](README.md): what it is and how to run it

Docs live in `docs/` and change with behavior. Never commit hostnames, tokens, or deployment config, and never
any part of a real dump (see [Privacy](docs/standards.md#privacy)).

Run `make check` before committing. It runs everything CI runs, except the org standards check:
`make standards` (needs the network).
