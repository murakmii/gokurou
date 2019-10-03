package artifact_gatherer

import (
	"bytes"
	"testing"

	"github.com/murakmii/gokurou/pkg/gokurou"
)

type sampleAtrtifact struct {
	Number int `json:"number"`
}

type mockArtifactStorage struct {
	putted [][]byte
}

func (s *mockArtifactStorage) put(_ string, data []byte) error {
	s.putted = append(s.putted, data)
	return nil
}

func buildBuiltInArtifactGatherer(maxBuffered int) (*mockArtifactStorage, *builtInArtifactGatherer) {
	storage := &mockArtifactStorage{putted: make([][]byte, 0)}
	return storage, &builtInArtifactGatherer{
		storage:     storage,
		keyPrefix:   "test",
		buffer:      bytes.NewBuffer(nil),
		maxBuffered: maxBuffered,
	}
}
func TestBuiltInArtifactGatherer_Collect(t *testing.T) {
	ctx := gokurou.MustRootContext(gokurou.NewConfiguration(1, 1))

	t.Run("Marshalできる場合、バッファしてからアップロードする", func(t *testing.T) {
		storage, ag := buildBuiltInArtifactGatherer(20)

		if err := ag.Collect(ctx, sampleAtrtifact{Number: 123}); err != nil {
			t.Errorf("Collect() = %v", err)
		}

		if err := ag.Collect(ctx, sampleAtrtifact{Number: 456}); err != nil {
			t.Errorf("Collect() = %v", err)
		}

		got := storage.putted
		want := []byte("{\"number\":123}\n{\"number\":456}\n")

		if len(got) != 1 || bytes.Compare(got[0], want) != 0 {
			t.Errorf("Collect() uploads %q, want = %q", got, string(want))
		}

		if ag.buffer.Len() != 0 {
			t.Errorf("Collect() does NOT clear buffer")
		}
	})

	t.Run("Marshalできない場合でもnilを返す", func(t *testing.T) {
		_, ag := buildBuiltInArtifactGatherer(20)
		got := ag.Collect(ctx, make(chan struct{}))

		if got != nil {
			t.Errorf("Collect() = %v, want no error", got)
		}

		if ag.buffer.Len() != 0 {
			t.Errorf("Collect() buffers bytes")
		}
	})
}

func TestBuiltInArtifactGatherer_Finish(t *testing.T) {
	ctx := gokurou.MustRootContext(gokurou.NewConfiguration(1, 1))

	t.Run("バッファが空の場合、何事もなく終了する", func(t *testing.T) {
		storage, ag := buildBuiltInArtifactGatherer(20)
		if err := ag.Finish(); err != nil {
			t.Errorf("Finsh() = %v", err)
		}

		if len(storage.putted) != 0 {
			t.Errorf("Finish() uploads %q", storage.putted)
		}
	})

	t.Run("バッファ済みのデータが残っている場合、アップロードする", func(t *testing.T) {
		storage, ag := buildBuiltInArtifactGatherer(20)

		if err := ag.Collect(ctx, sampleAtrtifact{Number: 123}); err != nil {
			t.Errorf("Collect() = %v", err)
		}

		if err := ag.Finish(); err != nil {
			t.Errorf("Finsh() = %v", err)
		}

		got := storage.putted
		want := []byte("{\"number\":123}\n")

		if len(got) != 1 || bytes.Compare(got[0], want) != 0 {
			t.Errorf("Finish() uploads %q, want = %s", got, string(want))
		}
	})
}
