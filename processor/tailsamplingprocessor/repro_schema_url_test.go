// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package tailsamplingprocessor

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor/internal/metadata"
)

// TestReproTailSamplingPreservesSchemaURLs intentionally fails on the affected
// implementation. It checks downstream output, including a sampled-cache hit.
// It uses the existing test controller instead of sleeping for decision_wait.
func TestReproTailSamplingPreservesSchemaURLs(t *testing.T) {
	const (
		resourceURL = "https://opentelemetry.io/schemas/1.27.0"
		scopeURL    = "https://opentelemetry.io/schemas/1.28.0"
	)
	traceID := uInt64ToTraceID(1)
	newInput := func(spanID uint64) ptrace.Traces {
		traces := ptrace.NewTraces()
		rs := traces.ResourceSpans().AppendEmpty()
		rs.SetSchemaUrl(resourceURL)
		rs.Resource().Attributes().PutStr("service.name", "schema-url-repro")
		ss := rs.ScopeSpans().AppendEmpty()
		ss.SetSchemaUrl(scopeURL)
		ss.Scope().SetName("repro-instrumentation")
		ss.Scope().SetVersion("1.0.0")
		ss.Scope().Attributes().PutStr("repro.scope", "original")
		span := ss.Spans().AppendEmpty()
		span.SetTraceID(traceID)
		span.SetSpanID(uInt64ToSpanID(spanID))
		span.SetName("repro-span")
		if spanID != 1 {
			span.SetParentSpanID(uInt64ToSpanID(1))
		}
		return traces
	}

	for _, strategy := range []samplingStrategy{samplingStrategyTraceComplete, samplingStrategySpanIngest} {
		t.Run(string(strategy), func(t *testing.T) {
			controller := newTestTSPController()
			sink := new(consumertest.TracesSink)
			p, err := newTracesProcessor(t.Context(), processortest.NewNopSettings(metadata.Type), sink, Config{
				SamplingStrategy: strategy,
				DecisionWait:     defaultTestDecisionWait,
				NumTraces:        defaultNumTraces,
				PolicyCfgs:       testPolicy, // always_sample
				DecisionCache:    DecisionCacheConfig{SampledCacheSize: 10},
				Options:          []Option{withTestController(controller)},
			})
			require.NoError(t, err)
			require.NoError(t, p.Start(t.Context(), componenttest.NewNopHost()))
			defer func() { require.NoError(t, p.Shutdown(t.Context())) }()

			for i, phase := range []string{"initial_decision", "late_span_sampled_cache"} {
				t.Run(phase, func(t *testing.T) {
					if i == 1 {
						_, cached := shard0(p).sampledIDCache.Get(traceID)
						require.True(t, cached, "the late span must take the sampled-cache path")
					}
					input := newInput(uint64(i + 1))
					require.NoError(t, p.ConsumeTraces(t.Context(), input))
					// Two ticks evaluate the initial trace in trace-complete mode.
					// They also synchronize processing for span-ingest/cache hits.
					controller.waitForTick()
					controller.waitForTick()

					output := sink.AllTraces()
					require.Len(t, output, i+1)
					require.Equal(t, 1, output[i].SpanCount())
					require.Equal(t, 1, output[i].ResourceSpans().Len())
					rs := output[i].ResourceSpans().At(0)
					require.Equal(t, 1, rs.ScopeSpans().Len())
					ss := rs.ScopeSpans().At(0)
					sourceRS := input.ResourceSpans().At(0)
					sourceSS := sourceRS.ScopeSpans().At(0)

					// Other metadata and the span survive. The input is unchanged.
					assert.Equal(t, sourceRS.Resource(), rs.Resource())
					assert.Equal(t, sourceSS.Scope(), ss.Scope())
					assert.Equal(t, sourceSS.Spans().At(0), ss.Spans().At(0))
					assert.Equal(t, resourceURL, sourceRS.SchemaUrl())
					assert.Equal(t, scopeURL, sourceSS.SchemaUrl())

					t.Logf("resource schema URL: input=%q output=%q", resourceURL, rs.SchemaUrl())
					t.Logf("scope schema URL: input=%q output=%q", scopeURL, ss.SchemaUrl())
					assert.Equal(t, resourceURL, rs.SchemaUrl(), "ResourceSpans.SchemaUrl must survive tail sampling")
					assert.Equal(t, scopeURL, ss.SchemaUrl(), "ScopeSpans.SchemaUrl must survive tail sampling")
				})
			}
		})
	}
}
