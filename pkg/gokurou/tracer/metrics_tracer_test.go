package tracer

import (
	"context"
	"reflect"
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

func (m *mockMetricsClient) put(_ context.Context, ns string, e *emitted, dimName, dimValue string) {
	m.puttedValues = append(m.puttedValues, *e.value)
	m.puttedTimes = append(m.puttedTimes, e.timestamp.Unix())
}

func (m *mockMetricsClient) finish() {}

func buildTimeProvider(times ...time.Time) func() time.Time {
	return func() time.Time {
		tm := times[0]
		times = times[1:]
		return tm
	}
}

func buildMetricsTracer(client metricsClient, timeProvider func() time.Time) *metricsTracer {
	return &metricsTracer{
		client:   client,
		ns:       "NS",
		dimName:  "Environment",
		dimValue: "Test",

		startedCrawl: newSumInMinuetMetrics(timeProvider, "", ""),
		gathered:     newSumInMinuetMetrics(timeProvider, "", ""),
		crawlLatency: newAvgInMinuetMetrics(timeProvider, "", ""),
	}
}

func TestMetricsTracer_TraceStartedCrawl(t *testing.T) {
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
				time.Unix(59, 0),
				time.Unix(61, 0),
				time.Unix(130, 0),
				time.Unix(200, 0),
			},
			want: want{
				values: []float64{1.0, 1.0, 1.0},
				times:  []int64{0, 60, 120},
			},
		},
		{
			name: "1分間に複数回記録された場合、それぞれをバッファしてからメトリクスとして送信する",
			in: []time.Time{
				time.Unix(1, 0),
				time.Unix(2, 0),
				time.Unix(3, 0),
				time.Unix(60, 0),
				time.Unix(61, 0),
				time.Unix(62, 0),
				time.Unix(120, 0),
				time.Unix(130, 0),
				time.Unix(180, 0),
			},
			want: want{
				values: []float64{3.0, 3.0, 2.0},
				times:  []int64{0, 60, 120},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := buildMockMetricsClient()
			tracer := buildMetricsTracer(client, buildTimeProvider(tt.in...))

			for i := 0; i < len(tt.in); i++ {
				tracer.TraceStartedCrawl(context.Background())
			}
			_ = tracer.Finish()

			gotValues := client.puttedValues
			gotTimes := client.puttedTimes
			if !reflect.DeepEqual(gotValues, tt.want.values) || !reflect.DeepEqual(gotTimes, tt.want.times) {
				t.Errorf("TraceStartedCrawl() = {%+v,%+v}, want = {%+v,%+v}", gotValues, gotTimes, tt.want.values, tt.want.times)
			}
		})

	}
}

func TestMetricsTracer_TraceGathered(t *testing.T) {
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
				time.Unix(59, 0),
				time.Unix(61, 0),
				time.Unix(130, 0),
				time.Unix(200, 0),
			},
			want: want{
				values: []float64{1.0, 1.0, 1.0},
				times:  []int64{0, 60, 120},
			},
		},
		{
			name: "1分間に複数回記録された場合、それぞれをバッファしてからメトリクスとして送信する",
			in: []time.Time{
				time.Unix(1, 0),
				time.Unix(2, 0),
				time.Unix(3, 0),
				time.Unix(60, 0),
				time.Unix(61, 0),
				time.Unix(62, 0),
				time.Unix(120, 0),
				time.Unix(130, 0),
				time.Unix(180, 0),
			},
			want: want{
				values: []float64{3.0, 3.0, 2.0},
				times:  []int64{0, 60, 120},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := buildMockMetricsClient()
			tracer := buildMetricsTracer(client, buildTimeProvider(tt.in...))

			for i := 0; i < len(tt.in); i++ {
				tracer.TraceGathered(context.Background())
			}
			_ = tracer.Finish()

			gotValues := client.puttedValues
			gotTimes := client.puttedTimes
			if !reflect.DeepEqual(gotValues, tt.want.values) || !reflect.DeepEqual(gotTimes, tt.want.times) {
				t.Errorf("TraceGathered() = {%+v,%+v}, want = {%+v,%+v}", gotValues, gotTimes, tt.want.values, tt.want.times)
			}
		})

	}
}

func TestMetricsTracer_TraceGetRequest(t *testing.T) {
	type in struct {
		times  []time.Time
		values []float64
	}

	type want struct {
		values []float64
		times  []int64
	}

	tests := []struct {
		name string
		in   in
		want want
	}{
		{
			name: "1分毎に記録された場合、それぞれをメトリクスとして送信する",
			in: in{
				times: []time.Time{
					time.Unix(59, 0),
					time.Unix(61, 0),
					time.Unix(130, 0),
					time.Unix(200, 0),
				},
				values: []float64{
					0.100,
					0.200,
					0.300,
					0.400,
				},
			},
			want: want{
				values: []float64{0.100, 0.200, 0.300},
				times:  []int64{0, 60, 120},
			},
		},
		{
			name: "1分間に複数回記録された場合、それぞれをバッファしてからメトリクスとして送信する",
			in: in{
				times: []time.Time{
					time.Unix(1, 0),
					time.Unix(2, 0),
					time.Unix(3, 0),
					time.Unix(60, 0),
					time.Unix(70, 0),
					time.Unix(80, 0),
					time.Unix(120, 0),
				},
				values: []float64{
					0.100,
					0.900,
					0.200,
					1.1,
					2.9,
					0.2,
					1.0,
				},
			},
			want: want{
				values: []float64{0.400, 1.4},
				times:  []int64{0, 60},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			client := buildMockMetricsClient()
			tracer := buildMetricsTracer(client, buildTimeProvider(tt.in.times...))

			for i := 0; i < len(tt.in.times); i++ {
				tracer.TraceGetRequest(context.Background(), tt.in.values[i])
			}
			_ = tracer.Finish()

			gotValues := client.puttedValues
			gotTimes := client.puttedTimes
			if !reflect.DeepEqual(gotValues, tt.want.values) || !reflect.DeepEqual(gotTimes, tt.want.times) {
				t.Errorf("TraceStartedCrawl() = {%+v,%+v}, want = {%+v,%+v}", gotValues, gotTimes, tt.want.values, tt.want.times)
			}
		})

	}
}
