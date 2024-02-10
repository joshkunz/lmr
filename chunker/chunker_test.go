package chunker

import (
	"strings"
	"testing"

	"github.com/google/go-cmp/cmp"
)

func chunks(t *testing.T, c Chunker) []string {
	t.Helper()
	var out []string
	for c.HasNext() {
		tok := c.Next()
		out = append(out, string(tok))
	}

	if err := c.Err(); err != nil {
		t.Fatalf("chunker failed: %s", err)
	}
	return out
}

func TestLine(t *testing.T) {
	tests := []struct {
		name  string
		input string
		want  []string
	}{
		{
			name:  "empty",
			input: "",
			want:  nil,
		},
		{
			name:  "basic",
			input: "some line input",
			want:  []string{"some line input"},
		},
		{
			name:  "multiline",
			input: "line a\nline b",
			want:  []string{"line a", "line b"},
		},
		{
			name:  "trailing newline",
			input: "line a\nline b\n",
			want:  []string{"line a", "line b"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := NewLine(strings.NewReader(test.input))

			out := chunks(t, c)

			if diff := cmp.Diff(test.want, out); diff != "" {
				t.Errorf("unexpected chunk diff (want->got):\n%s", diff)
			}
		})
	}
}
