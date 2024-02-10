// Command lmr is the main entrypoint for little map reduce.
package main

import (
	"fmt"
	"io"
	"os"

	"github.com/spf13/cobra"

	"github.com/joshkunz/lmr/chunker"
	"github.com/joshkunz/lmr/mapper"
	"github.com/joshkunz/lmr/runner"
)

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

			m, err := mapper.Exec(args[0])
			if err != nil {
				return err
			}

			r := runner.ReducerFunc(concatReducer)
			chunker := chunker.NewLine(os.Stdin)

			rnr, err := runner.NewRunner(m, r, ".lmr")
			if err != nil {
				return err
			}

			return rnr.Run(cmd.Context(), chunker)
		},
	}
	cmd.Execute()
}
