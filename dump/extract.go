package dump

import (
	"archive/tar"
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
)

// RefusedError is an archive the tool won't extract, and the entry that decided it.
type RefusedError struct {
	Archive string
	Entry   string
	Reason  string
}

func (e *RefusedError) Error() string {
	return fmt.Sprintf("refuse archive %s at entry %q: %s", e.Archive, e.Entry, e.Reason)
}

// extract writes the archive at path into a new directory under opts.Scratch named for it, and returns that
// directory. A failed or refused extraction leaves nothing behind.
func extract(ctx context.Context, path string, opts Options, logger *slog.Logger) (_ string, err error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open archive: %w", err)
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	tr, err := tarReader(f)
	if err != nil {
		return "", fmt.Errorf("read %s: %w", path, err)
	}

	target := filepath.Join(opts.Scratch, extractionName(path))
	if _, err := os.Lstat(target); err == nil {
		return "", fmt.Errorf("%s already exists: pass it as --dump to use that extraction, or remove it to extract %s again",
			target, path)
	}
	// Extraction fills a hidden directory and renames it into place only when complete, so the target never
	// holds a partial extraction, which would answer wrong.
	partial, err := os.MkdirTemp(opts.Scratch, "."+filepath.Base(target)+".partial-")
	if err != nil {
		return "", fmt.Errorf("create extraction directory under %s: %w", opts.Scratch, err)
	}
	defer func() {
		if err != nil {
			err = errors.Join(err, os.RemoveAll(partial))
		}
	}()

	logger.Info("extracting archive", "archive", path, "to", target)
	x := extraction{archive: path, max: opts.MaxExtractBytes}
	if err := x.run(ctx, tr, partial); err != nil {
		return "", err
	}
	for _, name := range x.dangling {
		logger.Warn("removed a symbolic link that leads nowhere in the archive", "entry", name)
	}
	if err := os.Rename(partial, target); err != nil {
		return "", fmt.Errorf("move extraction into place: %w", err)
	}
	logger.Info("extracted archive; pass this directory as --dump to skip extracting it again",
		"dir", target, "entries", x.entries, "bytes", x.written)
	return target, nil
}

// extractionName names an archive's extraction: its name without the archive suffix.
func extractionName(path string) string {
	base := filepath.Base(path)
	for _, s := range []string{".tar.gz", ".tgz", ".tar", ".gz"} {
		if name, ok := strings.CutSuffix(base, s); ok && name != "" {
			return name
		}
	}
	return base + ".extracted"
}

// tarReader recognizes a tar stream, gzip-compressed or not, by its content rather than its name.
func tarReader(r io.Reader) (*tar.Reader, error) {
	br := bufio.NewReader(r)
	if magic, _ := br.Peek(2); bytes.Equal(magic, []byte{0x1f, 0x8b}) { // A short read means it isn't gzip.
		gz, err := gzip.NewReader(br)
		if err != nil {
			return nil, err
		}
		br = bufio.NewReader(gz)
	}
	// A POSIX or GNU tar header has "ustar" at offset 257.
	if header, _ := br.Peek(262); len(header) < 262 || string(header[257:262]) != "ustar" {
		return nil, errors.New("neither a directory nor a tar archive, gzip-compressed or not")
	}
	return tar.NewReader(br), nil
}

// extraction is one archive being written under a directory, with what it has written so far.
type extraction struct {
	archive string
	max     int64
	entries int
	written int64
	links   []string
	// dangling are the symbolic links removed for resolving to nothing.
	dangling []string
}

func (x *extraction) refuse(entry, format string, args ...any) error {
	return &RefusedError{Archive: x.archive, Entry: entry, Reason: fmt.Sprintf(format, args...)}
}

func (x *extraction) run(ctx context.Context, tr *tar.Reader, dir string) (err error) {
	// os.Root refuses any path that would leave dir, through ".." or a symbolic link, so nothing the archive
	// says can write outside it.
	root, err := os.OpenRoot(dir)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, root.Close()) }()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fmt.Errorf("read %s after %d entries: %w", x.archive, x.entries, err)
		}
		if err := x.entry(root, tr, hdr); err != nil {
			return err
		}
		x.entries++
	}
	return x.checkLinks(root, dir)
}

func (x *extraction) entry(root *os.Root, tr *tar.Reader, hdr *tar.Header) error {
	name := hdr.Name
	if !filepath.IsLocal(name) {
		return x.refuse(name, "its path leaves the extraction directory")
	}
	if hdr.Typeflag != tar.TypeDir {
		if err := root.MkdirAll(filepath.Dir(name), 0o750); err != nil {
			return fmt.Errorf("extract %s: %w", name, err)
		}
	}

	switch hdr.Typeflag {
	case tar.TypeDir:
		if err := root.MkdirAll(name, 0o750); err != nil {
			return fmt.Errorf("extract %s: %w", name, err)
		}
	case tar.TypeReg:
		if hdr.Size > x.max-x.written {
			return x.refuse(name, "extracting its %d bytes would pass --max-extract-bytes %d, after %d bytes in %d entries",
				hdr.Size, x.max, x.written, x.entries)
		}
		n, err := writeFile(root, name, tr)
		x.written += n
		if err != nil {
			return fmt.Errorf("extract %s: %w", name, err)
		}
	case tar.TypeSymlink:
		if filepath.IsAbs(hdr.Linkname) || !filepath.IsLocal(filepath.Join(filepath.Dir(name), hdr.Linkname)) {
			return x.refuse(name, "its link target %s leaves the extraction directory", hdr.Linkname)
		}
		if err := root.Symlink(hdr.Linkname, name); err != nil {
			return fmt.Errorf("extract %s: %w", name, err)
		}
		x.links = append(x.links, name)
	case tar.TypeLink:
		if !filepath.IsLocal(hdr.Linkname) {
			return x.refuse(name, "its link target %s leaves the extraction directory", hdr.Linkname)
		}
		// A hard link to a symbolic link is a second symbolic link in another directory, where the same target
		// text can lead somewhere else, so it is refused rather than checked again.
		if target, err := root.Lstat(hdr.Linkname); err == nil && target.Mode()&os.ModeSymlink != 0 {
			return x.refuse(name, "its link target %s is a symbolic link", hdr.Linkname)
		}
		if err := root.Link(hdr.Linkname, name); err != nil {
			return fmt.Errorf("extract %s: %w", name, err)
		}
	default:
		return x.refuse(name, "it is a special file (tar type %q), not a file, directory or link", hdr.Typeflag)
	}
	return nil
}

func writeFile(root *os.Root, name string, r io.Reader) (_ int64, err error) {
	f, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o640)
	if err != nil {
		return 0, err
	}
	defer func() { err = errors.Join(err, f.Close()) }()
	return io.Copy(f, r)
}

// checkLinks refuses a symbolic link that resolves outside dir, and removes one that resolves to nothing. A
// target can stay inside by its text and leave by what it passes through, as with b -> . and a -> b/.., which is
// the parent of dir. A dangling link can lead out the same way, as a -> b/../later does, to a file that appears
// beside the extraction once it moves under scratch, and it reaches nothing in the archive, so it goes.
func (x *extraction) checkLinks(root *os.Root, dir string) error {
	resolvedDir, err := filepath.EvalSymlinks(dir)
	if err != nil {
		return err
	}
	for _, name := range x.links {
		resolved, err := filepath.EvalSymlinks(filepath.Join(dir, name))
		if errors.Is(err, os.ErrNotExist) {
			if err := root.Remove(name); err != nil {
				return fmt.Errorf("remove dangling link %s: %w", name, err)
			}
			x.dangling = append(x.dangling, name)
			continue
		}
		if err != nil {
			return x.refuse(name, "its link target can't be resolved: %v", err)
		}
		if resolved != resolvedDir && !strings.HasPrefix(resolved, resolvedDir+string(filepath.Separator)) {
			return x.refuse(name, "its link target resolves outside the extraction directory")
		}
	}
	return nil
}
