// Command prometheus-tsdb-reader reads a Prometheus TSDB dump offline and answers PromQL the way Prometheus does.
package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/junglestyle/prometheus-tsdb-reader/dump"
	"github.com/junglestyle/prometheus-tsdb-reader/query"
	"github.com/junglestyle/prometheus-tsdb-reader/render"
	"github.com/junglestyle/prometheus-tsdb-reader/store"
)

// Exit codes, as the spec's RUN-3 gives them.
const (
	exitFailed  = 1
	exitBadArgs = 2
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, os.Args[1:], os.Stdout, os.Stderr)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		var usage *usageError
		if errors.As(err, &usage) {
			os.Exit(exitBadArgs)
		}
		os.Exit(exitFailed)
	}
}

// usageError is a run refused for its arguments, before anything was read.
type usageError struct {
	msg string
}

func (e *usageError) Error() string { return e.msg }

func usagef(format string, args ...any) error {
	return &usageError{msg: fmt.Sprintf(format, args...)}
}

// run executes one command. Stdout gets only JSON lines: the result, or for a failed run one error record.
func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	out := bufio.NewWriter(stdout)
	err := execute(ctx, args, out, stderr)
	var usage *usageError
	if err != nil && !errors.As(err, &usage) {
		out.Reset(stdout)
		err = errors.Join(err, render.Error(out, exitFailed, err.Error()))
	}
	return errors.Join(err, out.Flush())
}

// defaultMaxExtractBytes is the spec's default for --max-extract-bytes: 32 GiB.
const defaultMaxExtractBytes = 32 << 30

const usage = `usage: prometheus-tsdb-reader --dump PATH [global flags] COMMAND [command flags]

commands:
  info                                    what the dump covers: its range, blocks and WAL
  query instant --promql EXPR [--time T]  one expression at one time

global flags:`

// config is what the global flags say about where the dump is and where the run may write.
type config struct {
	dump     string
	dumpOpts dump.Options
	logger   *slog.Logger
}

func execute(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	global := flag.NewFlagSet("prometheus-tsdb-reader", flag.ContinueOnError)
	global.SetOutput(stderr)
	global.Usage = func() {
		fmt.Fprintln(stderr, usage)
		global.PrintDefaults()
	}
	var cfg config
	global.StringVar(&cfg.dump, "dump", "", "the dump: an extracted `directory` or a tar archive, gzip-compressed or not")
	global.StringVar(&cfg.dumpOpts.Scratch, "scratch", ".", "the only `directory` the tool writes under")
	global.StringVar(&cfg.dumpOpts.Root, "root", "",
		"the TSDB root `directory`, relative to the dump's, when the dump holds more than one")
	global.Int64Var(&cfg.dumpOpts.MaxExtractBytes, "max-extract-bytes", defaultMaxExtractBytes,
		"the most `bytes` extracting an archive may write")
	if err := global.Parse(args); err != nil {
		return flagError(err)
	}
	if cfg.dump == "" {
		global.Usage()
		return usagef("--dump is required")
	}
	if cfg.dumpOpts.MaxExtractBytes <= 0 {
		return usagef("--max-extract-bytes must be positive, not %d", cfg.dumpOpts.MaxExtractBytes)
	}
	cfg.logger = slog.New(slog.NewTextHandler(stderr, nil))

	rest := global.Args()
	switch {
	case len(rest) >= 1 && rest[0] == "info":
		return info(ctx, cfg, rest[1:], stdout, stderr)
	case len(rest) >= 2 && rest[0] == "query" && rest[1] == "instant":
		return queryInstant(ctx, cfg, rest[2:], stdout, stderr)
	}
	global.Usage()
	return usagef("unknown command %q", rest)
}

func info(ctx context.Context, cfg config, args []string, stdout, stderr io.Writer) (err error) {
	fs := flag.NewFlagSet("info", flag.ContinueOnError)
	fs.SetOutput(stderr)
	if err := fs.Parse(args); err != nil {
		return flagError(err)
	}
	if fs.NArg() > 0 {
		return usagef("unexpected argument %q: info takes none", fs.Arg(0))
	}

	o, err := cfg.open(ctx, false)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, o.st.Close()) }()

	var blocks []render.Block
	for _, b := range o.st.Blocks() {
		blocks = append(blocks, render.Block{Dir: b.Dir, Start: b.Start, End: b.End, Series: b.Series, Status: b.Status})
	}
	return render.Info(stdout, o.summary("info"), blocks)
}

func queryInstant(ctx context.Context, cfg config, args []string, stdout, stderr io.Writer) (err error) {
	fs := flag.NewFlagSet("query instant", flag.ContinueOnError)
	fs.SetOutput(stderr)
	expr := fs.String("promql", "", "the PromQL `expression` to evaluate")
	var at query.Time
	fs.Func("time", "evaluation time: RFC 3339, or a duration such as 6h back from the dump's end (default the dump's end)",
		func(s string) (err error) {
			at, err = query.ParseTime(s)
			return err
		})
	if err := fs.Parse(args); err != nil {
		return flagError(err)
	}
	if fs.NArg() > 0 {
		return usagef("unexpected argument %q: an expression enters only through --promql", fs.Arg(0))
	}
	if *expr == "" {
		return usagef("--promql is required")
	}

	o, err := cfg.open(ctx, true)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, o.st.Close()) }()

	t := at.At(o.end)
	res, err := query.NewEngine().Instant(ctx, o.st, *expr, t)
	if err != nil {
		return err
	}
	return render.Instant(stdout, o.summary("query instant"), *expr, t, res)
}

// opened is a dump opened for one command, with its range.
type opened struct {
	d          dump.Dump
	st         *store.Store
	start, end time.Time
}

// open finds and opens the dump, replaying its WAL when a query needs the head.
func (c config) open(ctx context.Context, replay bool) (_ *opened, err error) {
	d, err := dump.Open(ctx, c.dump, c.dumpOpts, c.logger)
	if err != nil {
		return nil, err
	}
	st, err := store.Open(ctx, d, store.Options{Scratch: c.dumpOpts.Scratch, Replay: replay}, c.logger)
	if err != nil {
		return nil, err
	}
	start, end, ok := st.Range()
	if !ok {
		return nil, errors.Join(fmt.Errorf("the dump at %s holds no samples", c.dump), st.Close())
	}
	return &opened{d: d, st: st, start: start, end: end}, nil
}

func (o *opened) summary(command string) render.Summary {
	version, ok := query.PromQLVersion()
	if !ok {
		version = "unknown"
	}
	wal := o.st.WAL()
	return render.Summary{
		Command:       command,
		PromQLVersion: version,
		Dump:          render.Dump{Start: o.start, End: o.end, Root: o.d.RootName},
		WAL:           render.WAL{State: wal.State, Bytes: wal.Bytes, Newest: wal.Newest, Sampled: wal.Sampled},
	}
}

// flagError turns a flag package error into a usage error, and a request for help into success.
func flagError(err error) error {
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	return &usageError{msg: err.Error()}
}
