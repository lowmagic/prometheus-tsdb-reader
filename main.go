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

func execute(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	global := flag.NewFlagSet("prometheus-tsdb-reader", flag.ContinueOnError)
	global.SetOutput(stderr)
	dumpPath := global.String("dump", "", "the extracted dump `directory`")
	scratch := global.String("scratch", ".", "the only `directory` the tool writes under")
	global.Usage = func() {
		fmt.Fprintln(stderr, "usage: prometheus-tsdb-reader --dump DIR [--scratch DIR] query instant --promql EXPR [--time T]")
		global.PrintDefaults()
	}
	if err := global.Parse(args); err != nil {
		return flagError(err)
	}
	if *dumpPath == "" {
		global.Usage()
		return usagef("--dump is required")
	}

	rest := global.Args()
	if len(rest) >= 2 && rest[0] == "query" && rest[1] == "instant" {
		return queryInstant(ctx, *dumpPath, *scratch, rest[2:], stdout, stderr)
	}
	global.Usage()
	return usagef("unknown command %q", rest)
}

func queryInstant(ctx context.Context, dumpPath, scratch string, args []string, stdout, stderr io.Writer) (err error) {
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

	logger := slog.New(slog.NewTextHandler(stderr, nil))
	d, err := dump.Open(dumpPath)
	if err != nil {
		return err
	}
	logger.Info("found TSDB", "root", d.Root, "blocks", len(d.Blocks), "wal", d.WAL != "")

	st, err := store.Open(ctx, d, scratch, logger)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, st.Close()) }()

	start, end, ok := st.Range()
	if !ok {
		return fmt.Errorf("the dump at %s holds no samples", dumpPath)
	}
	t := at.At(end)
	res, err := query.NewEngine().Instant(ctx, st, *expr, t)
	if err != nil {
		return err
	}

	version, ok := query.PromQLVersion()
	if !ok {
		version = "unknown"
	}
	wal := st.WAL()
	return render.Instant(stdout, render.Summary{
		Command:       "query instant",
		PromQLVersion: version,
		Dump:          render.Dump{Start: start, End: end, Root: d.RootName},
		WAL:           render.WAL{State: wal.State, Bytes: wal.Bytes},
		Expression:    *expr,
		Time:          t,
	}, res)
}

// flagError turns a flag package error into a usage error, and a request for help into success.
func flagError(err error) error {
	if errors.Is(err, flag.ErrHelp) {
		return nil
	}
	return &usageError{msg: err.Error()}
}
