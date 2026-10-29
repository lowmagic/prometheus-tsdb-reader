# Working on prometheus-tsdb-reader

A Go library and CLI for reading and querying Prometheus TSDB dumps offline.

Read before changing behavior:

- [README](README.md): what prometheus-tsdb-reader does and how to run it
- [Coding standards](docs/standards.md): how the code should read

Docs live in `docs/` and change with behavior. Never commit hostnames, tokens, or deployment config (see
Privacy in the [org standards](docs/org-standards.md)).

`make check` runs everything CI runs, except the org standards check: `make standards` (needs the network).
