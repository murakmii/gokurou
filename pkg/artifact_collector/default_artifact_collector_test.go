package artifact_collector

import (
	"bytes"
	"testing"
)

type basicMockArtifactStorage struct {
	putted [][]byte
}

func newBasicMockArtifactStorage() *basicMockArtifactStorage {
	return &basicMockArtifactStorage{putted: make([][]byte, 0)}
}

func (s *basicMockArtifactStorage) put(_ string, data []byte) error {
	s.putted = append(s.putted, data)
	return nil
}

func buildDefaultArtifactCollector() (*defaultArtifactCollector, *basicMockArtifactStorage) {
	storage := newBasicMockArtifactStorage()

	return &defaultArtifactCollector{
		storage:     storage,
		prefix:      "test",
		buffer:      bytes.NewBuffer(nil),
		bufCount:    0,
		maxBuffered: 3,
		errCount:    0,
	}, storage
}

func TestDefaultArtifactCollector_Collect(t *testing.T) {
	t.Run("収集された結果がバッファ上限内の場合", func(t *testing.T) {
		collector, storage := buildDefaultArtifactCollector()
		_ = collector.Collect([]byte("abc"))

		if collector.bufCount != 1 {
			t.Errorf("collector.bufCount = %d, want = 1", collector.bufCount)
		}

		if bytes.Compare(collector.buffer.Bytes(), []byte("abc\n")) != 0 {
			t.Errorf("collector.buffer.Bytes() = %v, want = 'abc\\n'", collector.buffer.Bytes())
		}

		if len(storage.putted) != 0 {
			t.Errorf("len(storage.putted) = %d, want = 0", len(storage.putted))
		}
	})

	t.Run("収集された結果がバッファ上限に達した場合", func(t *testing.T) {
		collector, storage := buildDefaultArtifactCollector()
		_ = collector.Collect([]byte("abc"))
		_ = collector.Collect([]byte("def"))
		_ = collector.Collect([]byte("ghi"))

		if collector.bufCount != 0 {
			t.Errorf("collector.bufCount = %d, want = 0", collector.bufCount)
		}

		if bytes.Compare(collector.buffer.Bytes(), []byte("")) != 0 {
			t.Errorf("collector.buffer.Bytes() = %v, want = ''", collector.buffer.Bytes())
		}

		if len(storage.putted) != 1 {
			t.Errorf("len(storage.putted) = %d, want = 1", len(storage.putted))
		}

		if bytes.Compare(storage.putted[0], []byte("abc\ndef\nghi\n")) != 0 {
			t.Errorf("storage.putted[0] = %v, want = 'abc\\ndef\\nghi\\n'", storage.putted[0])
		}
	})
}

func TestDefaultArtifactCollector_Finish(t *testing.T) {
	collector, storage := buildDefaultArtifactCollector()
	_ = collector.Collect([]byte("abc"))
	_ = collector.Finish()

	if len(storage.putted) != 1 {
		t.Errorf("len(storage.putted) = %d, want = 1", len(storage.putted))
	}

	if bytes.Compare(storage.putted[0], []byte("abc\n")) != 0 {
		t.Errorf("storage.putted[0] = %v, want = 'abc\\n'", storage.putted[0])
	}
}
