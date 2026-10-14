package common

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

const (
	defaultChunkedWriterBufferSize  = 1 << 20 // 1 MiB buffered writer
	defaultChunkedWriterChannelSize = 1024
	defaultChunkSize                = 1_000_000 // 1 million records per chunk
)

// DefaultChunkSize returns the default chunk size for RDF file chunking.
func DefaultChunkSize() int {
	return defaultChunkSize
}

// ChunkedGzipRDFWriter writes RDF content to chunked, gzip-compressed files.
// When the chunk size is reached, it automatically closes the current file
// and opens a new one with the next chunk number.
type ChunkedGzipRDFWriter struct {
	basePath   string
	chunkSize  int
	currentChunk int
	recordCount int

	file   *os.File
	gzipWriter *gzip.Writer
	writer *bufio.Writer

	ch chan string

	wg sync.WaitGroup

	mu    sync.Mutex
	errMu sync.Mutex
	err   error

	closeOnce sync.Once
}

// NewChunkedGzipRDFWriter creates a new ChunkedGzipRDFWriter for the provided
// base path. Files will be named like: base.rdf.gz, base_1.rdf.gz, base_2.rdf.gz, etc.
// The directory must exist beforehand.
func NewChunkedGzipRDFWriter(basePath string, chunkSize int) (*ChunkedGzipRDFWriter, error) {
	if chunkSize <= 0 {
		chunkSize = defaultChunkSize
	}

	writer := &ChunkedGzipRDFWriter{
		basePath:    basePath,
		chunkSize:   chunkSize,
		currentChunk: 0,
		recordCount:  0,
		ch:          make(chan string, defaultChunkedWriterChannelSize),
	}

	if err := writer.openNextChunk(); err != nil {
		return nil, err
	}

	writer.wg.Add(1)
	go writer.run()

	return writer, nil
}

func (w *ChunkedGzipRDFWriter) openNextChunk() error {
	if w.file != nil {
		if err := w.closeCurrentFile(); err != nil {
			return err
		}
	}

	var path string
	if w.currentChunk == 0 {
		// First chunk: use base path
		path = w.basePath
	} else {
		// Subsequent chunks: add chunk number before .gz extension
		// Handle both .rdf.gz and .gz cases
		base := w.basePath
		var ext string
		if strings.HasSuffix(base, ".rdf.gz") {
			ext = ".rdf.gz"
			base = strings.TrimSuffix(base, ext)
		} else if strings.HasSuffix(base, ".gz") {
			ext = ".gz"
			base = strings.TrimSuffix(base, ext)
		} else {
			ext = filepath.Ext(base)
			base = strings.TrimSuffix(base, ext)
		}
		path = fmt.Sprintf("%s_%d%s", base, w.currentChunk, ext)
	}

	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("create chunk file %s: %w", path, err)
	}

	gzipWriter := gzip.NewWriter(file)
	writer := bufio.NewWriterSize(gzipWriter, defaultChunkedWriterBufferSize)

	w.file = file
	w.gzipWriter = gzipWriter
	w.writer = writer
	w.recordCount = 0

	return nil
}

func (w *ChunkedGzipRDFWriter) closeCurrentFile() error {
	if w.writer != nil {
		if err := w.writer.Flush(); err != nil {
			return err
		}
	}

	if w.gzipWriter != nil {
		if err := w.gzipWriter.Close(); err != nil {
			return err
		}
	}

	if w.file != nil {
		if err := w.file.Close(); err != nil {
			return err
		}
	}

	w.file = nil
	w.gzipWriter = nil
	w.writer = nil

	return nil
}

func (w *ChunkedGzipRDFWriter) run() {
	defer w.wg.Done()

	for data := range w.ch {
		if data == "" {
			continue
		}

		if w.Err() != nil {
			// Drain remaining entries to unblock writers.
			continue
		}

		w.mu.Lock()
		needsNewChunk := w.recordCount >= w.chunkSize
		w.mu.Unlock()

		if needsNewChunk {
			if err := w.rotateChunk(); err != nil {
				w.setErr(err)
				continue
			}
		}

		w.mu.Lock()
		w.recordCount++
		w.mu.Unlock()

		if _, err := w.writer.WriteString(data); err != nil {
			w.setErr(err)
			continue
		}
	}

	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.closeCurrentFile(); err != nil {
		w.setErr(err)
	}
}

func (w *ChunkedGzipRDFWriter) rotateChunk() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.currentChunk++
	return w.openNextChunk()
}

// Write enqueues content for writing. It blocks if the internal buffer is full
// and returns as soon as the data has been accepted or the context is cancelled.
func (w *ChunkedGzipRDFWriter) Write(ctx context.Context, content string) error {
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
// file and gzip writer.
func (w *ChunkedGzipRDFWriter) Close() error {
	w.closeOnce.Do(func() {
		close(w.ch)
	})

	w.wg.Wait()

	return w.Err()
}

// Err returns the first error encountered by the writer, if any.
func (w *ChunkedGzipRDFWriter) Err() error {
	w.errMu.Lock()
	defer w.errMu.Unlock()
	return w.err
}

func (w *ChunkedGzipRDFWriter) setErr(err error) {
	if err == nil {
		return
	}

	w.errMu.Lock()
	if w.err == nil {
		w.err = err
	}
	w.errMu.Unlock()
}

