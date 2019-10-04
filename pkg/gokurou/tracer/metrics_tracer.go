package tracer

import (
	"context"
	"sync"
	"time"

	"golang.org/x/xerrors"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/aws/aws-sdk-go/service/cloudwatch"

	"github.com/murakmii/gokurou/pkg/gokurou"
)

const (
	namespaceConfKey            = "built_in.tracer.namespace"
	crawledCountDimNameConfKey  = "built_in.tracer.crawled_count_dimention_name"
	crawledCountDimValueConfKey = "built_in.tracer.crawed_count_dimention_value"
)

// 動作をトレースしてメトリクスとして外部(CloudWatch)に送信するトレーサー
type metricsTracer struct {
	m            *sync.Mutex
	wg           *sync.WaitGroup
	timeProvider func() time.Time

	client metricsClient
	ns     string

	window int64

	crawled     int
	crawledDimN string
	crawledDimV string
}

// 外部(CloudWatch)送信するためのClient
type metricsClient interface {
	put(ns, name, dimName, dimValue string, value float64, timestamp time.Time) error
}

// metricsClientの実装
type cloudWatchMetricsClient struct {
	client *cloudwatch.CloudWatch
}

// metricsTracerをTracerとして生成して返す
func NewMetricsTracer(conf *gokurou.Configuration) (gokurou.Tracer, error) {
	client, err := newCloudWatchMetricsClient(conf)
	if err != nil {
		return nil, xerrors.Errorf("failed to build cloudwatch client: %w", err)
	}

	return &metricsTracer{
		m:            &sync.Mutex{},
		wg:           &sync.WaitGroup{},
		timeProvider: time.Now,
		client:       client,
		ns:           conf.MustOptionAsString(namespaceConfKey),
		window:       time.Now().Truncate(1 * time.Minute).Unix(),
		crawled:      0,
		crawledDimN:  conf.MustOptionAsString(crawledCountDimNameConfKey),
		crawledDimV:  conf.MustOptionAsString(crawledCountDimValueConfKey),
	}, nil
}

// クロールをトレースして1分間の間に発生したクロール回数をCloudWatchにCPM(Crawl per minutes)として送信する
func (tracer *metricsTracer) TraceCrawled(ctx context.Context, _ error) {
	now := tracer.timeProvider()
	currentWindow := now.Truncate(1 * time.Minute).Unix()

	windowUpdated := false
	count := 0

	tracer.m.Lock()
	if tracer.window != currentWindow {
		tracer.wg.Wait()
		windowUpdated = true
		count = tracer.crawled
		tracer.window = currentWindow
		tracer.crawled = 1
	} else {
		tracer.crawled++
	}
	defer tracer.m.Unlock()

	if windowUpdated {
		tracer.wg.Add(1)
		go func() {
			defer tracer.wg.Done()
			if err := tracer.client.put(tracer.ns, "CPM", tracer.crawledDimN, tracer.crawledDimV, float64(count), now.Truncate(1*time.Minute)); err != nil {
				gokurou.LoggerFromContext(ctx).Warnf("metrics tracer error: %v", err)
			}
		}()
	}
}

func (tracer *metricsTracer) Finish() error {
	tracer.wg.Wait()
	return nil
}

func newCloudWatchMetricsClient(conf *gokurou.Configuration) (metricsClient, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	cred := credentials.NewStaticCredentials(conf.AwsAccessKeyID, conf.AwsSecretAccessKey, "")
	config := aws.NewConfig().WithCredentials(cred).WithRegion(conf.AwsRegion).WithMaxRetries(5)

	return &cloudWatchMetricsClient{client: cloudwatch.New(sess, config)}, nil
}

func (m *cloudWatchMetricsClient) put(ns, name, dimName, dimValue string, value float64, timestamp time.Time) error {
	_, err := m.client.PutMetricData(&cloudwatch.PutMetricDataInput{
		MetricData: []*cloudwatch.MetricDatum{
			{
				Dimensions: []*cloudwatch.Dimension{
					{
						Name:  &dimName,
						Value: &dimValue,
					},
				},
				MetricName: &name,
				Timestamp:  &timestamp,
				Value:      &value,
			},
		},
		Namespace: &ns,
	})

	return err
}
