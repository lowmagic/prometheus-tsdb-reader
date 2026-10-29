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
	"strings"
	"syscall"
	"time"

	"github.com/prometheus/common/model"

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
  query range --promql EXPR [--start T] [--end T] [--step D]
                                          one expression over a range at a step
  query series --promql SELECTOR... [--start T] [--end T]
                                          the series matching any of the selectors
  query labels [NAME] [--promql SELECTOR...] [--start T] [--end T]
                                          label names, or the values of NAME

T is an RFC 3339 time or a duration such as 6h back from the dump's end.

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
	commands := map[string]func(context.Context, config, []string, io.Writer, io.Writer) error{
		"info":          info,
		"query instant": queryInstant,
		"query range":   queryRange,
		"query series":  querySeries,
		"query labels":  queryLabels,
	}
	for words := 1; words <= 2 && words <= len(rest); words++ {
		if command, ok := commands[strings.Join(rest[:words], " ")]; ok {
			err := command(ctx, cfg, rest[words:], stdout, stderr)
			if errors.Is(err, query.ErrStartAfterEnd) {
				return &usageError{msg: err.Error()}
			}
			return err
		}
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
	fs := newFlagSet("query instant", stderr)
	expr := fs.String("promql", "", "the PromQL `expression` to evaluate")
	at := timeFlag(fs, "time", "evaluation time (default the dump's end)")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if *expr == "" {
		return usagef("--promql is required")
	}

	o, err := cfg.open(ctx, true)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, o.st.Close()) }()
	res, err := query.NewEngine().Instant(ctx, o.queryDump(), *expr, *at)
	if err != nil {
		return err
	}
	return render.Instant(stdout, o.summary("query instant"), *expr, res)
}

func queryRange(ctx context.Context, cfg config, args []string, stdout, stderr io.Writer) (err error) {
	fs := newFlagSet("query range", stderr)
	expr := fs.String("promql", "", "the PromQL `expression` to evaluate")
	var span query.Span
	timeVar(fs, &span.Start, "start", "range start (default 2h before the end)")
	timeVar(fs, &span.End, "end", "range end (default the dump's end)")
	fs.Func("step", "the `duration` between points, such as 1m (default from the range: the smallest of 1m, 5m, 15m, 1h "+
		"and 6h that gives at most 500 points)", func(s string) error {
		d, err := model.ParseDuration(s)
		if err != nil || d <= 0 {
			return fmt.Errorf("step %q is not a positive duration", s)
		}
		span.Step = time.Duration(d)
		return nil
	})
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if *expr == "" {
		return usagef("--promql is required")
	}

	o, err := cfg.open(ctx, true)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, o.st.Close()) }()
	res, err := query.NewEngine().Range(ctx, o.queryDump(), *expr, span)
	if err != nil {
		return err
	}
	return render.Range(stdout, o.summary("query range"), *expr, res)
}

func querySeries(ctx context.Context, cfg config, args []string, stdout, stderr io.Writer) (err error) {
	fs := newFlagSet("query series", stderr)
	selectors := selectorsFlag(fs)
	start := timeFlag(fs, "start", "range start (default the dump's start)")
	end := timeFlag(fs, "end", "range end (default the dump's end)")
	if err := parseFlags(fs, args); err != nil {
		return err
	}
	if len(*selectors) == 0 {
		return usagef("--promql is required, once per selector")
	}

	o, err := cfg.open(ctx, true)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, o.st.Close()) }()
	l, err := query.NewEngine().Series(ctx, o.queryDump(), *selectors, *start, *end)
	if err != nil {
		return err
	}
	return render.Series(stdout, o.summary("query series"), l)
}

func queryLabels(ctx context.Context, cfg config, args []string, stdout, stderr io.Writer) (err error) {
	// NAME comes before the flags, as in promtool, and the flag package stops at the first non-flag argument.
	var name string
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		name, args = args[0], args[1:]
	}
	fs := newFlagSet("query labels", stderr)
	selectors := selectorsFlag(fs)
	start := timeFlag(fs, "start", "range start (default the dump's start)")
	end := timeFlag(fs, "end", "range end (default the dump's end)")
	if err := parseFlags(fs, args); err != nil {
		return err
	}

	o, err := cfg.open(ctx, true)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, o.st.Close()) }()
	l, err := query.NewEngine().Labels(ctx, o.queryDump(), name, *selectors, *start, *end)
	if err != nil {
		return err
	}
	return render.Labels(stdout, o.summary("query labels"), l)
}

func newFlagSet(name string, stderr io.Writer) *flag.FlagSet {
	fs := flag.NewFlagSet(name, flag.ContinueOnError)
	fs.SetOutput(stderr)
	return fs
}

// parseFlags parses a command's flags and refuses positional arguments, so an expression or selector enters
// only through --promql, the one flag a caller can deny.
func parseFlags(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		return flagError(err)
	}
	if fs.NArg() > 0 {
		return usagef("unexpected argument %q: an expression or selector enters only through --promql", fs.Arg(0))
	}
	return nil
}

func timeFlag(fs *flag.FlagSet, name, usage string) *query.Time {
	var t query.Time
	timeVar(fs, &t, name, usage)
	return &t
}

func timeVar(fs *flag.FlagSet, t *query.Time, name, usage string) {
	fs.Func(name, usage+": RFC 3339, or a duration such as 6h back from the dump's end", func(s string) (err error) {
		*t, err = query.ParseTime(s)
		return err
	})
}

func selectorsFlag(fs *flag.FlagSet) *[]string {
	var selectors []string
	fs.Func("promql", "a series `selector`; repeat for more", func(s string) error {
		selectors = append(selectors, s)
		return nil
	})
	return &selectors
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

func (o *opened) queryDump() query.Dump {
	return query.Dump{Queryable: o.st, Start: o.start, End: o.end}
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
