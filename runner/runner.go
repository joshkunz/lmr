package runner

import (
	"context"
	"encoding/base32"
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sync/atomic"

	"github.com/joshkunz/lmr/chunker"
	"golang.org/x/sync/errgroup"
)

type MapStage struct {
	ID         string
	Dir        string
	ResultsDir string
}

func (m *MapStage) init() error {
	if err := os.Mkdir(m.Dir, 0o777); err != nil {
		return fmt.Errorf("failed to create map stage: %w", err)
	}
	m.ResultsDir = filepath.Join(m.Dir, "results")
	if err := os.Mkdir(m.ResultsDir, 0o777); err != nil {
		return fmt.Errorf("failed to create results directory: %w", err)
	}
	return nil
}

func (m MapStage) CollectResults() ([]MapResult, error) {
	dirents, err := os.ReadDir(m.ResultsDir)
	if err != nil {
		return nil, err
	}

	var out []MapResult

	for _, dent := range dirents {
		if dent.IsDir() {
			return nil, fmt.Errorf("unexpected directory %q in stage output", dent.Name())
		}

		out = append(out, MapResult{
			Key:  dent.Name(),
			Path: filepath.Join(m.ResultsDir, dent.Name()),
		})
	}

	return out, nil
}

type MapResult struct {
	Key  string
	Path string
}

type Mapper interface {
	Map(context.Context, MapStage, []byte) ([]MapResult, error)
}

type MapperFunc func(context.Context, MapStage, []byte) ([]MapResult, error)

func (f MapperFunc) Map(ctx context.Context, stage MapStage, chunk []byte) ([]MapResult, error) {
	return f(ctx, stage, chunk)
}

type Reducer interface {
	Reduce(key string, paths []string, out io.Writer) error
}

type ReducerFunc func(string, []string, io.Writer) error

func (f ReducerFunc) Reduce(key string, paths []string, out io.Writer) error {
	return f(key, paths, out)
}

type limit struct {
	unlimited bool
	value     uint
}

var unlimited = limit{unlimited: true}

type Runner struct {
	mapper  Mapper
	reducer Reducer

	mapRoot string

	nextMapID atomic.Uint32
}

func NewRunner(mapper Mapper, reducer Reducer, root string) (*Runner, error) {
	r := &Runner{
		mapper:  mapper,
		reducer: reducer,
	}

	if _, err := os.Stat(root); os.IsNotExist(err) {
		if err := os.Mkdir(root, 0o777); err != nil {
			return nil, fmt.Errorf("failed to create .lmr: %w", err)
		}
	}

	mapRoot := filepath.Join(root, "map")

	if _, err := os.Stat(mapRoot); err == nil {
		if err := os.RemoveAll(mapRoot); err != nil {
			return nil, fmt.Errorf("failed to clear map stage root: %w", err)
		}
	}

	if err := os.Mkdir(mapRoot, 0o777); err != nil {
		return nil, fmt.Errorf("failed to create map stage: %w", err)
	}

	r.mapRoot = mapRoot

	return r, nil
}

func (r *Runner) mapID() string {
	id := r.nextMapID.Add(1)
	raw := make([]byte, binary.Size(id))
	binary.LittleEndian.PutUint32(raw, id)
	return "map_" + base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(raw)
}

func (r *Runner) Run(ctx context.Context, c chunker.Chunker) error {
	sem := make(chan struct{}, runtime.NumCPU())

	results := make(chan []MapResult)
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
				Dir: filepath.Join(r.mapRoot, stageID),
			}
			if err := stage.init(); err != nil {
				return err
			}

			out, err := r.mapper.Map(egCtx, stage, chunk)
			if err != nil {
				return err
			}
			results <- out
			return nil
		})
	}

	if err := c.Err(); err != nil {
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
		if err := r.reducer.Reduce(k, paths, os.Stdout); err != nil {
			return err
		}
	}

	return nil
}
