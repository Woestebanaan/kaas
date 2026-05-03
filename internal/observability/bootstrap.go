package observability

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	promexp "go.opentelemetry.io/otel/exporters/prometheus"
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
	metricsServer  *http.Server
}

// Shutdown flushes signals and closes exporters. Safe to call multiple times.
func (p *Providers) Shutdown(ctx context.Context) error {
	var errs []error
	if p.metricsServer != nil {
		_ = p.metricsServer.Shutdown(ctx)
	}
	for _, fn := range p.shutdowns {
		if err := fn(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Bootstrap sets up the OTel MeterProvider + TracerProvider, serves Prometheus
// metrics on SKAFKA_METRICS_ADDR (default :9464), and optionally pushes traces
// via OTLP when OTEL_EXPORTER_OTLP_ENDPOINT is set.
//
// service should be "skafka" or "skafka-operator"; it becomes service.name in
// exported resource attributes.
func Bootstrap(ctx context.Context, service string) (*Providers, error) {
	res, err := buildResource(ctx, service)
	if err != nil {
		return nil, fmt.Errorf("observability: resource: %w", err)
	}

	p := &Providers{}

	// --- Meter provider (Prometheus + optional OTLP) ---
	promExporter, err := promexp.New()
	if err != nil {
		return nil, fmt.Errorf("observability: prometheus exporter: %w", err)
	}

	mpOpts := []metric.Option{
		metric.WithResource(res),
		metric.WithReader(promExporter),
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

	// --- Tracer provider (OTLP only if endpoint is set) ---
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

	// --- Prometheus scrape endpoint ---
	if err := startMetricsServer(ctx, p); err != nil {
		return nil, err
	}

	slog.Info("observability: initialized",
		"service", service,
		"metrics_addr", metricsAddr(),
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

func metricsAddr() string {
	if a := os.Getenv("SKAFKA_METRICS_ADDR"); a != "" {
		return a
	}
	return ":9464"
}

func startMetricsServer(ctx context.Context, p *Providers) error {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.Handler())

	srv := &http.Server{
		Addr:              metricsAddr(),
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}
	p.metricsServer = srv
	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Warn("metrics server exited", "err", err)
		}
	}()
	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()
	return nil
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
