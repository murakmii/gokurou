package artifact_gatherer

import (
	"bytes"
	"testing"
)

type basicMockArtifactStorage struct {
	putted [][]byte
}

type testArtifact struct {
	Number int `json:"number"`
}

func newBasicMockArtifactStorage() *basicMockArtifactStorage {
	return &basicMockArtifactStorage{putted: make([][]byte, 0)}
}

func (s *basicMockArtifactStorage) put(_ string, data []byte) error {
	s.putted = append(s.putted, data)
	return nil
}

func buildDefaultArtifactCollector() (*builtInArtifactGatherer, *basicMockArtifactStorage) {
	storage := newBasicMockArtifactStorage()

	return &builtInArtifactGatherer{
		storage:     storage,
		prefix:      "test",
		buffer:      bytes.NewBuffer(nil),
		bufCount:    0,
		maxBuffered: 3,
		errCount:    0,
	}, storage
}

func TestDefaultArtifactCollector_Collect(t *testing.T) {
	t.Run("収集された結果がバッファ上限内の場合、バッファを続ける", func(t *testing.T) {
		collector, storage := buildDefaultArtifactCollector()
		_ = collector.Collect(testArtifact{Number: 123})

		if collector.bufCount != 1 {
			t.Errorf("collector.bufCount = %d, want = 1", collector.bufCount)
		}

		if bytes.Compare(collector.buffer.Bytes(), []byte("{\"number\":123}\n")) != 0 {
			t.Errorf("collector.buffer.Bytes() = %s, want = '{\"number\":123}\n'", string(collector.buffer.Bytes()))
		}

		if len(storage.putted) != 0 {
			t.Errorf("len(storage.putted) = %d, want = 0", len(storage.putted))
		}
	})

	t.Run("収集された結果がバッファ上限に達した場合、結果を保存する", func(t *testing.T) {
		collector, storage := buildDefaultArtifactCollector()
		_ = collector.Collect(testArtifact{Number: 111})
		_ = collector.Collect(testArtifact{Number: 222})
		_ = collector.Collect(testArtifact{Number: 333})

		if collector.bufCount != 0 {
			t.Errorf("collector.bufCount = %d, want = 0", collector.bufCount)
		}

		if bytes.Compare(collector.buffer.Bytes(), []byte("")) != 0 {
			t.Errorf("collector.buffer.Bytes() = %v, want = ''", collector.buffer.Bytes())
		}

		if len(storage.putted) != 1 {
			t.Errorf("len(storage.putted) = %d, want = 1", len(storage.putted))
		}

		want := []byte("{\"number\":111}\n{\"number\":222}\n{\"number\":333}\n")
		if bytes.Compare(storage.putted[0], want) != 0 {
			t.Errorf("storage.putted[0] = %v, want = '%s'", storage.putted[0], string(want))
		}
	})
}

func TestDefaultArtifactCollector_Finish(t *testing.T) {
	collector, storage := buildDefaultArtifactCollector()
	_ = collector.Collect(testArtifact{Number: 123})
	_ = collector.Finish()

	if len(storage.putted) != 1 {
		t.Errorf("len(storage.putted) = %d, want = 1", len(storage.putted))
	}

	if bytes.Compare(storage.putted[0], []byte("{\"number\":123}\n")) != 0 {
		t.Errorf("storage.putted[0] = %v, want = '{\"number\":123}\n'", storage.putted[0])
	}
}
