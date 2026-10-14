package common

import (
	"bufio"
	"context"
	"os"
	"sync"
)

const (
	defaultWriterBufferSize  = 1 << 20 // 1 MiB buffered writer
	defaultWriterChannelSize = 1024
)

// RDFWriter asynchronously writes RDF content to a file. Calls to Write are
// safe for concurrent use.
type RDFWriter struct {
	file   *os.File
	writer *bufio.Writer

	ch chan string

	wg sync.WaitGroup

	errMu sync.Mutex
	err   error

	closeOnce sync.Once
}

// NewRDFWriter creates a new RDFWriter for the provided path. The directory is
// not created automatically and must exist beforehand.
func NewRDFWriter(path string) (*RDFWriter, error) {
	file, err := os.Create(path)
	if err != nil {
		return nil, err
	}

	writer := &RDFWriter{
		file:   file,
		writer: bufio.NewWriterSize(file, defaultWriterBufferSize),
		ch:     make(chan string, defaultWriterChannelSize),
	}

	writer.wg.Add(1)
	go writer.run()

	return writer, nil
}

func (w *RDFWriter) run() {
	defer w.wg.Done()

	for data := range w.ch {
		if data == "" {
			continue
		}

		if w.Err() != nil {
			// Drain remaining entries to unblock writers.
			continue
		}

		if _, err := w.writer.WriteString(data); err != nil {
			w.setErr(err)
		}
	}

	if err := w.writer.Flush(); err != nil {
		w.setErr(err)
	}
}

// Write enqueues content for writing. It blocks if the internal buffer is full
// and returns as soon as the data has been accepted or the context is cancelled.
func (w *RDFWriter) Write(ctx context.Context, content string) error {
	if content == "" {
		return nil
	}

	if err := w.Err(); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case w.ch <- content:
		return nil
	}
}

// Close waits until all buffered data is written and closes the underlying
// file.
func (w *RDFWriter) Close() error {
	w.closeOnce.Do(func() {
		close(w.ch)
	})

	w.wg.Wait()

	if err := w.file.Close(); err != nil {
		w.setErr(err)
	}

	return w.Err()
}

// Err returns the first error encountered by the writer, if any.
func (w *RDFWriter) Err() error {
	w.errMu.Lock()
	defer w.errMu.Unlock()
	return w.err
}

func (w *RDFWriter) setErr(err error) {
	if err == nil {
		return
	}

	w.errMu.Lock()
	if w.err == nil {
		w.err = err
	}
	w.errMu.Unlock()
}
