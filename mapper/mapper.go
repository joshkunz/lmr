package mapper

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/joshkunz/lmr/runner"
)

type execFunc func(context.Context, ...string) *exec.Cmd

func parseExec(prog string) (execFunc, error) {
	if _, err := exec.LookPath(prog); err == nil {
		// this is a binary on path
		return func(ctx context.Context, extraArgs ...string) *exec.Cmd {
			return exec.CommandContext(ctx, prog, extraArgs...)
		}, nil
	}

	stat, err := os.Stat(prog)
	if err == nil && (stat.Mode() == 0 || stat.Mode() == fs.ModeSymlink) {
		// this is *probably* an executable
		// TODO: Should we try to tell if this is actually executable?
		// maybe even has at least some executable set?
		path, err := filepath.Abs(prog)
		if err != nil {
			return nil, err
		}
		return func(ctx context.Context, extraArgs ...string) *exec.Cmd {
			return exec.CommandContext(ctx, path, extraArgs...)
		}, nil
	}

	// Otherwise, assume this is a valid shell command
	shell := os.Getenv("SHELL")
	if shell == "" {
		return nil, fmt.Errorf("SHELL unset and mapper does not appear to be an executable")
	}
	return func(ctx context.Context, extraArgs ...string) *exec.Cmd {
		args := []string{"-c", prog}
		if len(extraArgs) > 0 {
			// If we need any extra args, we need to pre-pend "--" so they
			// are intepreted as script args rather than shell args.
			args = append(args, "--")
			args = append(args, extraArgs...)
		}

		return exec.CommandContext(ctx, shell, args...)
	}, nil
}

type execOptions struct {
	DefaultKey string
}

type ExecOption func(*execOptions)

func ExecWithDefaultKey(key string) ExecOption {
	return func(o *execOptions) {
		o.DefaultKey = key
	}
}

func ExecWithNoDefaultKey() ExecOption {
	// key "" is a special key that disables default output.
	return ExecWithDefaultKey("")
}

type execMapper struct {
	opts    *execOptions
	builder execFunc
}

func (m execMapper) Map(ctx context.Context, stage runner.MapStage, chunk []byte) ([]runner.MapResult, error) {
	stdout, err := os.Create(filepath.Join(stage.Dir, "stdout"))
	if err != nil {
		return nil, fmt.Errorf("failed to create output log file: %w", err)
	}
	defer stdout.Close()

	stderr, err := os.Create(filepath.Join(stage.Dir, "stderr"))
	if err != nil {
		return nil, fmt.Errorf("failed to create error log file: %w", err)
	}
	defer stderr.Close()

	cmd := m.builder(ctx)
	cmd.Stdin = bytes.NewReader(chunk)
	cmd.Stdout = stdout
	cmd.Stderr = stderr

	env := os.Environ()
	env = append(env, "LMR_RESULTS_DIR="+stage.ResultsDir)
	cmd.Env = env

	if err := cmd.Run(); err != nil {
		return nil, err
	}

	if m.opts.DefaultKey != "" {
		stdout.Close()
		if err := os.Link(stdout.Name(), filepath.Join(stage.ResultsDir, m.opts.DefaultKey)); err != nil {
			return nil, fmt.Errorf("failed to produce default output: %w", err)
		}
	}

	// Lazy file produces a valid output if used, so we can just rely
	// on regular stage collection.
	return stage.CollectResults()
}

func Exec(prog string, opts ...ExecOption) (runner.Mapper, error) {
	options := execOptions{
		DefaultKey: "default",
	}
	for _, o := range opts {
		o(&options)
	}

	parsedProg, err := parseExec(prog)
	if err != nil {
		return nil, fmt.Errorf("failed to parse prog %q: %w", prog, err)
	}

	return execMapper{
		opts:    &options,
		builder: parsedProg,
	}, nil
}
