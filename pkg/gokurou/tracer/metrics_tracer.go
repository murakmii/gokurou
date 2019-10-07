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
	namespaceConfKey = "built_in.tracer.namespace"
	dimNameConfKey   = "built_in.tracer.dimention_name"
	dimValueConfKey  = "built_in.tracer.dimention_value"
)

// 動作をトレースしてメトリクスとして外部(CloudWatch)に送信するトレーサー
type metricsTracer struct {
	client   metricsClient
	ns       string
	dimName  string
	dimValue string

	crawled      metrics
	crawlLatency metrics
}

// 外部(CloudWatch)送信するためのClient
type metricsClient interface {
	put(ctx context.Context, ns string, e *emitted, dimName, dimValue string)
	finish()
}

// metricsClientの実装
type cloudWatchMetricsClient struct {
	client *cloudwatch.CloudWatch
	wg     *sync.WaitGroup
}

type metrics interface {
	add(value float64) *emitted
}

type emitted struct {
	name      *string
	unit      *string
	value     *float64
	timestamp *time.Time
}

type sumInMinuteMetrics struct {
	m            *sync.Mutex
	timeProvider func() time.Time
	window       *time.Time
	count        *float64
	n            string
	u            string
}

type avgInMinuteMetrics struct {
	m            *sync.Mutex
	timeProvider func() time.Time
	window       *time.Time
	avg          *float64
	count        int
	n            string
	u            string
}

func newCountInMinuetMetrics(timeProvider func() time.Time, name string, unit string) *sumInMinuteMetrics {
	defaultCount := 0.0
	return &sumInMinuteMetrics{
		m:            &sync.Mutex{},
		timeProvider: timeProvider,
		count:        &defaultCount,
		n:            name,
		u:            unit,
	}
}

func (m *sumInMinuteMetrics) add(value float64) *emitted {
	var e *emitted
	nowWindow := m.timeProvider().Truncate(1 * time.Minute)

	m.m.Lock()
	defer m.m.Unlock()

	if m.window != nil && *m.window != nowWindow {
		e = &emitted{
			name:      &m.n,
			unit:      &m.u,
			value:     m.count,
			timestamp: m.window,
		}

		newCount := 0.0
		m.count = &newCount
		m.window = &nowWindow
	} else if m.window == nil {
		m.window = &nowWindow
	}

	*m.count += value
	return e
}

func newAvgInMinuetMetrics(timeProvider func() time.Time, name string, unit string) *avgInMinuteMetrics {
	return &avgInMinuteMetrics{
		m:            &sync.Mutex{},
		timeProvider: timeProvider,
		n:            name,
		u:            unit,
	}
}

func (m *avgInMinuteMetrics) add(value float64) *emitted {
	var e *emitted
	nowWindow := m.timeProvider().Truncate(1 * time.Minute)

	m.m.Lock()
	defer m.m.Unlock()

	if m.window != nil && *m.window != nowWindow {
		e = &emitted{
			name:      &m.n,
			unit:      &m.u,
			value:     m.avg,
			timestamp: m.window,
		}

		m.avg = nil
		m.count = 0
		m.window = &nowWindow
	} else if m.window == nil {
		m.window = &nowWindow
	}

	m.count++
	if m.count == 1 {
		m.avg = &value
	} else {
		diff := (value - *m.avg) / float64(m.count)
		*m.avg += diff
	}

	return e
}

// metricsTracerをTracerとして生成して返す
func NewMetricsTracer(conf *gokurou.Configuration) (gokurou.Tracer, error) {
	client, err := newCloudWatchMetricsClient(conf)
	if err != nil {
		return nil, xerrors.Errorf("failed to build cloudwatch client: %w", err)
	}

	return &metricsTracer{
		client:   client,
		ns:       conf.MustOptionAsString(namespaceConfKey),
		dimName:  conf.MustOptionAsString(dimNameConfKey),
		dimValue: conf.MustOptionAsString(dimValueConfKey),

		crawled:      newCountInMinuetMetrics(time.Now, "CPM", "Count"),
		crawlLatency: newAvgInMinuetMetrics(time.Now, "Crawl Latency", "Seconds"),
	}, nil
}

// クロールをトレースして1分間の間に発生したクロール回数をCloudWatchに送信する
func (tracer *metricsTracer) TraceCrawled(ctx context.Context, _ error) {
	if e := tracer.crawled.add(1); e != nil {
		tracer.client.put(ctx, tracer.ns, e, tracer.dimName, tracer.dimValue)
	}
}

// 1 HTTP GETをトレースして1分間の間に発生したGETリクエストのレイテンシの平均をCloudWatchに送信する
func (tracer *metricsTracer) TraceGetRequest(ctx context.Context, elapsed float64) {
	if e := tracer.crawlLatency.add(elapsed); e != nil {
		tracer.client.put(ctx, tracer.ns, e, tracer.dimName, tracer.dimValue)
	}
}

func (tracer *metricsTracer) Finish() error {
	tracer.client.finish()
	return nil
}

func newCloudWatchMetricsClient(conf *gokurou.Configuration) (metricsClient, error) {
	sess, err := session.NewSession()
	if err != nil {
		return nil, err
	}

	cred := credentials.NewStaticCredentials(conf.AwsAccessKeyID, conf.AwsSecretAccessKey, "")
	config := aws.NewConfig().WithCredentials(cred).WithRegion(conf.AwsRegion).WithMaxRetries(5)

	return &cloudWatchMetricsClient{
		client: cloudwatch.New(sess, config),
		wg:     &sync.WaitGroup{},
	}, nil
}

func (m *cloudWatchMetricsClient) put(ctx context.Context, ns string, e *emitted, dimName, dimValue string) {
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		_, err := m.client.PutMetricData(&cloudwatch.PutMetricDataInput{
			MetricData: []*cloudwatch.MetricDatum{
				{
					Dimensions: []*cloudwatch.Dimension{
						{
							Name:  &dimName,
							Value: &dimValue,
						},
					},
					MetricName: e.name,
					Timestamp:  e.timestamp,
					Value:      e.value,
					Unit:       e.unit,
				},
			},
			Namespace: &ns,
		})

		if err != nil {
			gokurou.LoggerFromContext(ctx).Warnf("failed to put metrics: %v", err)
		}
	}()
}

func (m *cloudWatchMetricsClient) finish() {
	m.wg.Wait()
	return
}
