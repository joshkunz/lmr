package chunker

import (
	"bufio"
	"io"
)

type Chunker interface {
	HasNext() bool
	Next() []byte
	Err() error
}

type line struct {
	scan *bufio.Scanner
}

// NewLine returns a new Line chunker. It splits the given reader input
// on newline characters.
func NewLine(r io.Reader) Chunker {
	return line{
		scan: bufio.NewScanner(r),
	}
}

var _ Chunker = line{}

func (l line) HasNext() bool {
	return l.scan.Scan()
}

func (l line) Next() []byte {
	// Specifically using Text here so we get a new chunk. Bytes may be
	// overwritten between calls.
	return []byte(l.scan.Text())
}

func (l line) Err() error {
	return l.scan.Err()
}
