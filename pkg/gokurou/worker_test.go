package gokurou

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/murakmii/gokurou/pkg/gokurou/www"

	"golang.org/x/xerrors"
)

var globalArtifact []string

// Coordinatorのモック。".org"なURLはロックを獲れないことにする
type mockCoordinator struct{}

func buildMockTokenizer(_ *Configuration) (Coordinator, error) {
	return &mockCoordinator{}, nil
}

func (s *mockCoordinator) AllocNextGWN() (uint16, error) {
	return 1, nil
}

func (s *mockCoordinator) LockByIPAddrOf(host string) (bool, error) {
	return !strings.HasSuffix(host, ".org"), nil
}

func (s *mockCoordinator) Finish() error { return nil }

// ArtifactGathererのモック。単にglobalArtifactに結果を溜め込む
type mockArtifactGatherer struct{}

func buildMockArtifactGatherer(_ context.Context, _ *Configuration) (ArtifactGatherer, error) {
	return &mockArtifactGatherer{}, nil
}

func (ag *mockArtifactGatherer) Collect(_ context.Context, artifact interface{}) error {
	s, ok := artifact.(string)
	if !ok {
		return xerrors.New("can't convert artifact to string")
	}

	globalArtifact = append(globalArtifact, s)
	return nil
}

func (ag *mockArtifactGatherer) Finish() error { return nil }

// URLFrontierのモック。queueをURLのキューとしクロール対象の供給と保存を行う
type mockURLFrontier struct {
	queue []*www.SanitizedURL
}

func buildMockURLFrontier(_ context.Context, _ *Configuration) (URLFrontier, error) {
	initialURL, err := www.SanitizedURLFromString("http://1.com")
	if err != nil {
		panic(err)
	}

	return &mockURLFrontier{queue: []*www.SanitizedURL{initialURL}}, nil
}

func (f *mockURLFrontier) Push(ctx context.Context, url *www.SanitizedURL) error {
	f.queue = append(f.queue, url)
	return nil
}

func (f *mockURLFrontier) Pop(ctx context.Context) (*www.SanitizedURL, error) {
	if len(f.queue) == 0 {
		return nil, nil
	} else {
		url := f.queue[0]
		f.queue = f.queue[1:]
		return url, nil
	}
}

func (f *mockURLFrontier) Finish() error { return nil }

// Crawlerのモック。与えられたURLから次のクロール対象となるURLを生成していく
type mockCrawler struct{}

func buildMockCrawler(_ context.Context, _ *Configuration) (Crawler, error) {
	return &mockCrawler{}, nil
}

func (c *mockCrawler) Crawl(ctx context.Context, url *www.SanitizedURL, out OutputPipeline) error {
	parts := strings.Split(url.Host(), ".")
	no, err := strconv.Atoi(parts[0])
	if err != nil {
		panic(err)
	}

	out.OutputArtifact(ctx, url.String())

	if no >= 5 {
		return nil
	}

	nextComURL, err := www.SanitizedURLFromString(fmt.Sprintf("http://%d.com", no+1))
	if err != nil {
		panic(err)
	}

	nextOrgURL, err := www.SanitizedURLFromString(fmt.Sprintf("http://%d.org", no+1))
	if err != nil {
		panic(err)
	}

	out.OutputCollectedURL(ctx, nextComURL)
	out.OutputCollectedURL(ctx, nextOrgURL)
	return nil
}

func (c *mockCrawler) Finish() error { return nil }

func buildConfiguration() *Configuration {
	conf := NewConfiguration(1, 1)
	conf.Machines = 1
	conf.ArtifactGathererProvider = buildMockArtifactGatherer
	conf.URLFrontierProvider = buildMockURLFrontier
	conf.CoordinatorProvider = buildMockTokenizer
	conf.CrawlerProvider = buildMockCrawler
	return conf
}

// 各種モックでWorkerをセットアップし、10秒動かした後停止して収集した結果を確認する
func TestWorker_Start(t *testing.T) {
	globalArtifact = make([]string, 0)

	worker := NewWorker()
	conf := buildConfiguration()
	ctx, _ := context.WithTimeout(MustRootContext(conf), 3*time.Second)

	worker.Start(ctx, conf)

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
