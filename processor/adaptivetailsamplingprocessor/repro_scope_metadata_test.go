package adaptivetailsamplingprocessor

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/consumer/consumertest"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/adaptivetailsamplingprocessor/internal/metadata"
)

func TestReproScopeMetadata(t *testing.T) {
	sink := &consumertest.TracesSink{}
	cfg := createDefaultConfig().(*Config)
	cfg.DecisionDelay = 10 * time.Millisecond
	cfg.Rules = []RuleConfig{{Name: "sample-all", Sampler: SamplerConfig{Type: AlwaysSample}}}
	p, err := newProcessor(processortest.NewNopSettings(metadata.Type), cfg, sink)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Start(t.Context(), nil); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := p.Shutdown(t.Context()); err != nil {
			t.Error(err)
		}
	})

	td := ptrace.NewTraces()
	rs := td.ResourceSpans().AppendEmpty()
	for i, variant := range []string{"a", "b"} {
		ss := rs.ScopeSpans().AppendEmpty()
		ss.Scope().SetName("lib")
		ss.Scope().SetVersion("1")
		ss.Scope().Attributes().PutStr("variant", variant)
		ss.SetSchemaUrl("schema-" + variant)
		span := ss.Spans().AppendEmpty()
		span.SetTraceID(pcommon.TraceID{1})
		span.SetSpanID(pcommon.SpanID{byte(i + 1)})
		span.SetName("root")
		if i == 1 {
			span.SetName("child")
			span.SetParentSpanID(pcommon.SpanID{1})
		}
	}

	if err := p.ConsumeTraces(t.Context(), td); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for sink.SpanCount() != 2 {
		if time.Now().After(deadline) {
			t.Fatalf("expected 2 output spans, got %d", sink.SpanCount())
		}
		time.Sleep(time.Millisecond)
	}
	out := sink.AllTraces()
	if len(out) != 1 {
		t.Fatalf("expected 1 output batch, got %d", len(out))
	}
	if n := out[0].ResourceSpans().Len(); n != 1 {
		t.Fatalf("expected 1 ResourceSpans, got %d", n)
	}
	scopes := out[0].ResourceSpans().At(0).ScopeSpans()
	if scopes.Len() != 2 {
		t.Errorf("expected 2 ScopeSpans, got %d", scopes.Len())
	}
	for _, ss := range scopes.All() {
		if ss.Scope().Name() != "lib" || ss.Scope().Version() != "1" {
			t.Errorf("expected scope lib/1, got %s/%s", ss.Scope().Name(), ss.Scope().Version())
		}
		variant, ok := ss.Scope().Attributes().Get("variant")
		if !ok {
			t.Fatal("missing scope attribute: variant")
		}
		for _, span := range ss.Spans().All() {
			t.Logf("span=%s variant=%s schema_url=%s", span.Name(), variant.Str(), ss.SchemaUrl())
			want := "a"
			if span.Name() == "child" {
				want = "b"
			}
			if variant.Str() != want {
				t.Errorf("span=%s: expected variant=%s, got %s", span.Name(), want, variant.Str())
			}
			if ss.SchemaUrl() != "schema-"+want {
				t.Errorf("span=%s: expected schema_url=schema-%s, got %s", span.Name(), want, ss.SchemaUrl())
			}
		}
	}
}
