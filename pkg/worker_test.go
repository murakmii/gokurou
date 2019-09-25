package pkg

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/murakmii/gokurou/pkg/html"

	"golang.org/x/xerrors"
)

var globalArtifact []string

// Syncronizerのモック。".org"なURLはロックを獲れないことにする
type mockSyncronizer struct{}

func buildMockSynchronizer(_ *Configuration) (Synchronizer, error) {
	return &mockSyncronizer{}, nil
}

func (s *mockSyncronizer) AllocNextGWN() (uint16, error) {
	return 1, nil
}

func (s *mockSyncronizer) LockByIPAddrOf(host string) (bool, error) {
	return !strings.HasSuffix(host, ".org"), nil
}

func (s *mockSyncronizer) Finish() error { return nil }

// ArtifactCollectorのモック。単にglobalArtifactに結果を溜め込む
type mockArtifactCollector struct{}

func buildMockArtifactCollector(_ context.Context, _ *Configuration) (ArtifactCollector, error) {
	return &mockArtifactCollector{}, nil
}

func (ac *mockArtifactCollector) Collect(artifact interface{}) error {
	s, ok := artifact.(string)
	if !ok {
		return xerrors.New("can't convert artifact to string")
	}

	globalArtifact = append(globalArtifact, s)
	return nil
}

func (ac *mockArtifactCollector) Finish() error { return nil }

// URLFrontierのモック。queueをURLのキューとしクロール対象の供給と保存を行う
type mockURLFrontier struct {
	queue []*html.SanitizedURL
}

func buildMockURLFrontier(_ context.Context, _ *Configuration) (URLFrontier, error) {
	initialURL, err := html.SanitizedURLFromString("http://1.com")
	if err != nil {
		panic(err)
	}

	return &mockURLFrontier{queue: []*html.SanitizedURL{initialURL}}, nil
}

func (f *mockURLFrontier) Push(ctx context.Context, url *html.SanitizedURL) error {
	f.queue = append(f.queue, url)
	return nil
}

func (f *mockURLFrontier) Pop(ctx context.Context) (*html.SanitizedURL, error) {
	if len(f.queue) == 0 {
		return nil, nil
	} else {
		url := f.queue[0]
		f.queue = f.queue[1:]
		return url, nil
	}
}

func (s *mockURLFrontier) Finish() error { return nil }

// Crawlerのモック。与えられたURLから次のクロール対象となるURLを生成していく
type mockCrawler struct{}

func buildMockCrawler(_ context.Context, _ *Configuration) (Crawler, error) {
	return &mockCrawler{}, nil
}

func (c *mockCrawler) Crawl(ctx context.Context, url *html.SanitizedURL, out OutputPipeline) error {
	parts := strings.Split(url.Host(), ".")
	no, err := strconv.Atoi(parts[0])
	if err != nil {
		panic(err)
	}

	out.OutputArtifact(ctx, url.String())

	if no >= 5 {
		return nil
	}

	nextComURL, err := html.SanitizedURLFromString(fmt.Sprintf("http://%d.com", no+1))
	if err != nil {
		panic(err)
	}

	nextOrgURL, err := html.SanitizedURLFromString(fmt.Sprintf("http://%d.org", no+1))
	if err != nil {
		panic(err)
	}

	out.OutputCollectedURL(ctx, nextComURL)
	out.OutputCollectedURL(ctx, nextOrgURL)
	return nil
}

func (c *mockCrawler) Finish() error { return nil }

func buildConfiguration() *Configuration {
	conf := NewConfiguration(1)
	conf.Machines = 1
	conf.NewArtifactCollector = buildMockArtifactCollector
	conf.NewURLFrontier = buildMockURLFrontier
	conf.NewSynchronizer = buildMockSynchronizer
	conf.NewCrawler = buildMockCrawler
	return conf
}

// 各種モックでWorkerをセットアップし、10秒動かした後停止して収集した結果を確認する
func TestWorker_Start(t *testing.T) {
	globalArtifact = make([]string, 0)

	worker := NewWorker()
	conf := buildConfiguration()
	ctx, cancel := context.WithCancel(RootContext())
	wg := &sync.WaitGroup{}
	wg.Add(1)

	worker.Start(ctx, wg, conf)

	go func() {
		time.Sleep(3 * time.Second)
		cancel()
	}()

	wg.Wait()

	wantArtifacts := []string{"http://1.com", "http://2.com", "http://3.com", "http://4.com", "http://5.com"}
	if len(globalArtifact) != len(wantArtifacts) {
		t.Errorf("Start() collects %d artifacts, want = %d", len(globalArtifact), len(wantArtifacts))
	}

	for i, wantArtifact := range wantArtifacts {
		if globalArtifact[i] != wantArtifact {
			t.Errorf("Start() collects artifact '%s', want = '%s'", globalArtifact[i], wantArtifact)
		}
	}
}
