package observability

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.26.0"
)

// Providers holds the initialized OTel SDK providers and exporters.
// Shutdown flushes remaining signals and should be called on process exit.
type Providers struct {
	MeterProvider  *metric.MeterProvider
	TracerProvider *sdktrace.TracerProvider
	Metrics        *Metrics
	shutdowns      []func(context.Context) error
}

// Shutdown flushes signals and closes exporters. Safe to call multiple times.
func (p *Providers) Shutdown(ctx context.Context) error {
	var errs []error
	for _, fn := range p.shutdowns {
		if err := fn(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Bootstrap sets up the OTel MeterProvider + TracerProvider. Metrics are pushed
// via OTLP/HTTP when OTEL_EXPORTER_OTLP_METRICS_ENDPOINT is set; traces are
// pushed via OTLP/gRPC when OTEL_EXPORTER_OTLP_ENDPOINT is set. With neither
// set, instruments still register but their samples go nowhere — fine for
// tests and local runs.
//
// service should be "skafka" or "skafka-operator"; it becomes service.name in
// exported resource attributes.
func Bootstrap(ctx context.Context, service string) (*Providers, error) {
	res, err := buildResource(ctx, service)
	if err != nil {
		return nil, fmt.Errorf("observability: resource: %w", err)
	}

	p := &Providers{}

	// --- Meter provider (OTLP/HTTP push, gated on env var) ---
	mpOpts := []metric.Option{metric.WithResource(res)}
	metricsEndpoint := os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT")
	if metricsEndpoint != "" {
		metricExporter, err := otlpmetrichttp.New(ctx,
			otlpmetrichttp.WithEndpointURL(metricsEndpoint),
		)
		if err != nil {
			return nil, fmt.Errorf("observability: otlp metric exporter: %w", err)
		}
		mpOpts = append(mpOpts, metric.WithReader(metric.NewPeriodicReader(metricExporter)))
		p.shutdowns = append(p.shutdowns, metricExporter.Shutdown)
	}
	p.MeterProvider = metric.NewMeterProvider(mpOpts...)
	otel.SetMeterProvider(p.MeterProvider)
	p.shutdowns = append(p.shutdowns, p.MeterProvider.Shutdown)

	// Build central metric registry so the rest of the codebase has one source.
	metrics, err := NewMetrics(p.MeterProvider.Meter(service))
	if err != nil {
		return nil, fmt.Errorf("observability: metrics: %w", err)
	}
	p.Metrics = metrics
	SetGlobal(metrics)

	// Phase 10 Gap #3c: register the v3 runtime ObservableGauges. Until
	// SetGaugeSource is called by the runtime owner (cmd/skafka after
	// the cluster runtime is up), the callback returns zero-valued
	// samples so dashboards see present-but-flat instead of missing.
	if err := installRuntimeGauges(p.MeterProvider.Meter(service)); err != nil {
		return nil, fmt.Errorf("observability: install gauges: %w", err)
	}

	// --- Tracer provider (OTLP/gRPC, gated on env var) ---
	var spanProcessor sdktrace.SpanProcessor
	if endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); endpoint != "" {
		insecure := strings.EqualFold(os.Getenv("OTEL_EXPORTER_OTLP_INSECURE"), "true")
		opts := []otlptracegrpc.Option{otlptracegrpc.WithEndpoint(stripScheme(endpoint))}
		if insecure {
			opts = append(opts, otlptracegrpc.WithInsecure())
		}
		traceExporter, err := otlptrace.New(ctx, otlptracegrpc.NewClient(opts...))
		if err != nil {
			return nil, fmt.Errorf("observability: otlp trace exporter: %w", err)
		}
		spanProcessor = sdktrace.NewBatchSpanProcessor(traceExporter)
	}

	tpOpts := []sdktrace.TracerProviderOption{
		sdktrace.WithResource(res),
		sdktrace.WithSampler(buildSampler()),
	}
	if spanProcessor != nil {
		tpOpts = append(tpOpts, sdktrace.WithSpanProcessor(spanProcessor))
	}
	p.TracerProvider = sdktrace.NewTracerProvider(tpOpts...)
	otel.SetTracerProvider(p.TracerProvider)
	p.shutdowns = append(p.shutdowns, p.TracerProvider.Shutdown)

	slog.Info("observability: initialized",
		"service", service,
		"otlp_metrics", metricsEndpoint != "",
		"otlp_traces", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "")
	return p, nil
}

func buildResource(ctx context.Context, service string) (*resource.Resource, error) {
	attrs := []attribute.KeyValue{
		semconv.ServiceName(service),
	}
	if v := os.Getenv("OTEL_SERVICE_VERSION"); v != "" {
		attrs = append(attrs, semconv.ServiceVersion(v))
	}
	if v := os.Getenv("MY_POD_NAME"); v != "" {
		attrs = append(attrs, semconv.K8SPodName(v))
		// Prometheus's OTLP receiver promotes service.instance.id to the
		// `instance` label on every series; without it, all brokers'
		// metrics flatten under the same `job=skafka` and per-broker
		// drill-down in Grafana isn't possible.
		attrs = append(attrs, semconv.ServiceInstanceID(v))
	}
	if v := os.Getenv("SKAFKA_NAMESPACE"); v != "" {
		attrs = append(attrs, semconv.K8SNamespaceName(v))
	}
	return resource.New(ctx,
		resource.WithAttributes(attrs...),
		resource.WithFromEnv(), // honor OTEL_RESOURCE_ATTRIBUTES
	)
}

func buildSampler() sdktrace.Sampler {
	ratio := 0.1
	if v := os.Getenv("OTEL_TRACES_SAMPLER_ARG"); v != "" {
		if f, err := strconv.ParseFloat(v, 64); err == nil && f >= 0 && f <= 1 {
			ratio = f
		}
	}
	return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratio))
}

// stripScheme removes a leading http:// or https:// from an OTLP endpoint,
// since otlptracegrpc.WithEndpoint expects "host:port" not a URL.
func stripScheme(s string) string {
	for _, prefix := range []string{"http://", "https://", "grpc://"} {
		if strings.HasPrefix(s, prefix) {
			return s[len(prefix):]
		}
	}
	return s
}
