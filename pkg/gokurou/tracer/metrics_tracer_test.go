package tracer

import (
	"context"
	"reflect"
	"sync"
	"testing"
	"time"
)

type mockMetricsClient struct {
	puttedValues []float64
	puttedTimes  []int64
}

func buildMockMetricsClient() *mockMetricsClient {
	return &mockMetricsClient{
		puttedValues: make([]float64, 0),
		puttedTimes:  make([]int64, 0),
	}
}

func (m *mockMetricsClient) put(ns, name, dimName, dimValue string, value float64, timestamp time.Time) error {
	m.puttedValues = append(m.puttedValues, value)
	m.puttedTimes = append(m.puttedTimes, timestamp.Unix())
	return nil
}

func buildTimeProvider(times ...time.Time) func() time.Time {
	return func() time.Time {
		tm := times[0]
		times = times[1:]
		return tm
	}
}

func buildMetricsTracer(client metricsClient, timeProvider func() time.Time) *metricsTracer {
	return &metricsTracer{
		m:            &sync.Mutex{},
		wg:           &sync.WaitGroup{},
		timeProvider: timeProvider,
		client:       client,
		ns:           "NS",
		window:       timeProvider().Truncate(1 * time.Minute).Unix(),
		crawled:      0,
		crawledDimN:  "Environment",
		crawledDimV:  "Test",
	}
}

func TestMetricsTracer_TraceCrawled(t *testing.T) {
	type want struct {
		values []float64
		times  []int64
	}

	tests := []struct {
		name string
		in   []time.Time
		want want
	}{
		{
			name: "1分毎に記録された場合、それぞれをメトリクスとして送信する",
			in: []time.Time{
				time.Unix(10, 0),
				time.Unix(59, 0),
				time.Unix(61, 0),
				time.Unix(130, 0),
				time.Unix(180, 0),
			},
			want: want{
				values: []float64{1.0, 1.0, 1.0},
				times:  []int64{60, 120, 180},
			},
		},
		{
			name: "1分間に複数回記録された場合、それぞれをバッファしてからメトリクスとして送信する",
			in: []time.Time{
				time.Unix(0, 0),
				time.Unix(1, 0),
				time.Unix(2, 0),
				time.Unix(3, 0),
				time.Unix(60, 0),
				time.Unix(61, 0),
				time.Unix(62, 0),
				time.Unix(120, 0),
			},
			want: want{
				values: []float64{3.0, 3.0},
				times:  []int64{60, 120},
			},
		},
	}

	for _, tt := range tests {
		client := buildMockMetricsClient()
		tracer := buildMetricsTracer(client, buildTimeProvider(tt.in...))

		for i := 0; i < len(tt.in)-1; i++ {
			tracer.TraceCrawled(context.Background(), nil)
		}
		_ = tracer.Finish()

		gotValues := client.puttedValues
		gotTimes := client.puttedTimes
		if !reflect.DeepEqual(gotValues, tt.want.values) || !reflect.DeepEqual(gotTimes, tt.want.times) {
			t.Errorf("TraceCrawled() = {%+v,%+v}, want = {%+v,%+v}", gotValues, gotTimes, tt.want.values, tt.want.times)
		}
	}
}
