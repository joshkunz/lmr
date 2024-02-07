// Command lmr is the main entrypoint for little map reduce.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base32"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"sync/atomic"

	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
)

type mapResult struct {
	Key  string
	Path string
}

type MapStage struct {
	ID  string
	Dir string
}

func (m MapStage) CollectResults() ([]mapResult, error) {
	dirents, err := os.ReadDir(m.Dir)
	if err != nil {
		return nil, err
	}

	var out []mapResult

	for _, dent := range dirents {
		if dent.IsDir() {
			return nil, fmt.Errorf("unexpected directory %q in stage output", dent.Name())
		}

		out = append(out, mapResult{
			Key:  dent.Name(),
			Path: filepath.Join(m.Dir, dent.Name()),
		})
	}

	return out, nil
}

type mapFunc func(ctx context.Context, stage MapStage, chunk []byte) ([]mapResult, error)

type lazyFile struct {
	name string

	once   sync.Once
	f      *os.File
	poison error
}

func (l *lazyFile) Exists() bool {
	return l.poison == nil && l.f != nil
}

func (l *lazyFile) Close() error {
	if l.poison != nil {
		return l.poison
	}
	if l.f == nil {
		return nil
	}
	return l.f.Close()
}

func (l *lazyFile) Write(bs []byte) (int, error) {
	l.once.Do(func() {
		l.f, l.poison = os.Create(l.name)
	})
	if l.poison != nil {
		return 0, l.poison
	}
	// l.f is guaranteed to be non-nil at this point
	return l.f.Write(bs)
}

func execMapper(prog string) (mapFunc, error) {
	base, err := func() (func(ctx context.Context) *exec.Cmd, error) {
		if _, err := exec.LookPath(prog); err == nil {
			// this is a binary on path
			return func(ctx context.Context) *exec.Cmd {
				return exec.CommandContext(ctx, prog)
			}, nil
		}

		stat, err := os.Stat(prog)
		if err == nil && (stat.Mode() == 0 || stat.Mode() == fs.ModeSymlink) {
			// this is probably an executable
			// TODO: Should we try to tell if this is actually executable?
			// maybe even has at least some executable set?
			path, err := filepath.Abs(prog)
			if err != nil {
				return nil, err
			}
			return func(ctx context.Context) *exec.Cmd {
				return exec.CommandContext(ctx, path)
			}, nil
		}

		// Otherwise, assume this is a valid shell command
		shell := os.Getenv("SHELL")
		if shell == "" {
			return nil, fmt.Errorf("SHELL unset and mapper does not appear to be an executable")
		}
		return func(ctx context.Context) *exec.Cmd {
			return exec.CommandContext(ctx, shell, "-c", prog)
		}, nil
	}()
	if err != nil {
		return nil, err
	}

	return func(ctx context.Context, stage MapStage, chunk []byte) ([]mapResult, error) {
		defaultPath := filepath.Join(stage.Dir, "default")
		lf := &lazyFile{name: defaultPath}
		defer lf.Close()

		cmd := base(ctx)
		cmd.Stdin = bytes.NewReader(chunk)
		cmd.Stdout = lf
		cmd.Stderr = os.Stderr
		cmd.Dir = stage.Dir

		if err := cmd.Run(); err != nil {
			return nil, err
		}

		// Lazy file produces a valid output if used, so we can just rely
		// on regular stage collection.
		return stage.CollectResults()
	}, nil
}

type reduceFunc func(key string, paths []string, out io.Writer) error

func concatReducer(_ string, paths []string, out io.Writer) error {
	for _, path := range paths {
		f, err := os.Open(path)
		if err != nil {
			return fmt.Errorf("failed to open %q: %w", path, err)
		}

		if _, err := io.Copy(out, f); err != nil {
			f.Close()
			return fmt.Errorf("failed to read %q: %w", path, err)
		}

		f.Close()
	}
	return nil
}

type limit struct {
	unlimited bool
	value     uint
}

var unlimited = limit{unlimited: true}

type Chunker interface {
	HasNext() bool
	Next() []byte
	Err() error
}

type lineChunker struct {
	scan *bufio.Scanner
}

var _ Chunker = lineChunker{}

func (l lineChunker) HasNext() bool {
	return l.scan.Scan()
}

func (l lineChunker) Next() []byte {
	// Specifically using Text here so we get a new chunk. Bytes may be
	// overwritten between calls.
	return []byte(l.scan.Text())
}

func (l lineChunker) Err() error {
	return l.scan.Err()
}

type runner struct {
	mapper  mapFunc
	reducer reduceFunc

	root string

	nextMapID atomic.Uint32
}

func (r *runner) newRoot() (string, error) {
	if _, err := os.Stat(".lmr"); os.IsNotExist(err) {
		if err := os.Mkdir(".lmr", 0o777); err != nil {
			return "", fmt.Errorf("failed to create .lmr: %w", err)
		}
	}

	if _, err := os.Stat(".lmr/map"); err == nil {
		if err := os.RemoveAll(".lmr/map"); err != nil {
			return "", fmt.Errorf("failed to clear map stage root: %w", err)
		}
	}

	if err := os.Mkdir(".lmr/map", 0o777); err != nil {
		return "", fmt.Errorf("failed to create map stage: %w", err)
	}

	r.root = ".lmr/map"

	return r.root, nil
}

func (r *runner) mapID() string {
	id := r.nextMapID.Add(1)
	raw := make([]byte, binary.Size(id))
	binary.LittleEndian.PutUint32(raw, id)
	return "map_" + base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw)
}

func (r *runner) Run(ctx context.Context, c Chunker) error {
	root, err := r.newRoot()
	if err != nil {
		return err
	}
	sem := make(chan struct{}, runtime.NumCPU())

	results := make(chan []mapResult)
	resultsReady := make(chan struct{})
	groups := make(map[string][]string)
	go func() {
		for group := range results {
			for idx := range group {
				groups[group[idx].Key] = append(groups[group[idx].Key], group[idx].Path)
			}
		}
		close(resultsReady)
	}()

	eg, egCtx := errgroup.WithContext(ctx)
	egCtx, cancel := context.WithCancel(egCtx)
	defer cancel() // catch-all to make sure it's always cancelled

	for c.HasNext() {
		sem <- struct{}{}
		chunk := c.Next()
		stageID := r.mapID()

		eg.Go(func() error {
			// Release the sem at the end of this function.
			defer func() { <-sem }()

			stage := MapStage{
				ID:  stageID,
				Dir: filepath.Join(root, stageID),
			}
			if err := os.Mkdir(stage.Dir, 0o777); err != nil {
				return fmt.Errorf("failed to create map stage: %w", err)
			}
			out, err := r.mapper(egCtx, stage, chunk)
			if err != nil {
				return err
			}
			results <- out
			return nil
		})
	}

	if c.Err() != nil {
		cancel()
		return fmt.Errorf("failed to parse all input chunks: %w", err)
	}

	if err := eg.Wait(); err != nil {
		return fmt.Errorf("map stage failure: %w", err)
	}
	close(results)

	<-resultsReady

	if len(groups) > 1 {
		return fmt.Errorf("too many results, only one key currently supported")
	}

	for k, paths := range groups {
		if err := r.reducer(k, paths, os.Stdout); err != nil {
			return err
		}
	}

	return nil
}

func main() {
	cmd := cobra.Command{
		Use: "lmr",
		RunE: func(cmd *cobra.Command, args []string) error {
			if len(args) < 1 {
				return fmt.Errorf("a mapper must be provided")
			}

			if len(args) > 1 {
				return fmt.Errorf("too many command line arguments")
			}

			mapper, err := execMapper(args[0])
			if err != nil {
				return err
			}
			reducer := concatReducer
			chunker := lineChunker{bufio.NewScanner(os.Stdin)}

			r := runner{
				mapper:  mapper,
				reducer: reducer,
			}

			return r.Run(cmd.Context(), chunker)
		},
	}
	cmd.Execute()
}
