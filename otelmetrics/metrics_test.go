// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otelmetrics

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

func TestProviderRecordsThroughInjectedReader(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider, err := New(context.Background(), Options{Readers: []sdkmetric.Reader{reader}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	if !provider.IsEnabled() {
		t.Fatal("provider is disabled")
	}
	counter, err := provider.Meter("test").Int64Counter("fit.future.counter")
	if err != nil {
		t.Fatalf("Int64Counter: %v", err)
	}
	counter.Add(context.Background(), 3)

	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(collected.ScopeMetrics) != 1 || len(collected.ScopeMetrics[0].Metrics) != 1 {
		t.Fatalf("unexpected metrics: %+v", collected.ScopeMetrics)
	}
	if collected.ScopeMetrics[0].Metrics[0].Name != "fit.future.counter" {
		t.Fatalf("metric name = %q", collected.ScopeMetrics[0].Metrics[0].Name)
	}
}

func TestDefaultOptionsSignalPrecedenceAndCommonHTTPPath(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://collector.example/base")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "https://metrics.example/custom")
	t.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", "grpc")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_PROTOCOL", "http/protobuf")
	t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "2500")

	opts := DefaultOptions()
	if opts.Endpoint != "https://metrics.example/custom" || opts.EndpointIsCommon {
		t.Fatalf("signal endpoint did not win: %+v", opts)
	}
	if opts.Protocol != "http/protobuf" {
		t.Fatalf("protocol = %q", opts.Protocol)
	}
	if opts.ExportInterval.String() != "2.5s" {
		t.Fatalf("interval = %s", opts.ExportInterval)
	}

	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")
	opts = DefaultOptions()
	if got := httpEndpoint(opts.Endpoint, opts.EndpointIsCommon); got != "https://collector.example/base/v1/metrics" {
		t.Fatalf("common HTTP endpoint = %q", got)
	}

	// A signal-specific value loaded from a config file must override a common
	// environment endpoint without inheriting the common /v1/metrics suffix.
	opts.Endpoint = "https://metrics.example/from-config"
	opts.EndpointIsCommon = false
	if got := httpEndpoint(opts.Endpoint, opts.EndpointIsCommon); got != opts.Endpoint {
		t.Fatalf("signal-specific config endpoint = %q", got)
	}
}

func TestExporterValidation(t *testing.T) {
	for _, test := range []struct {
		value string
		ok    bool
	}{
		{value: "otlp,console", ok: true},
		{value: "stdout,stdout", ok: true},
		{value: "none", ok: true},
		{value: "none,otlp", ok: false},
		{value: "prometheus", ok: false},
	} {
		_, err := parseExporters(test.value)
		if (err == nil) != test.ok {
			t.Errorf("parseExporters(%q) error = %v, ok=%v", test.value, err, test.ok)
		}
	}
}

func TestGlobalLifecycleHandlesOutOfOrderShutdown(t *testing.T) {
	first := testProvider(t)
	second := testProvider(t)
	restoreFirst := InstallGlobal(first)
	if !globalTargets(first) {
		t.Fatal("first provider was not routed globally")
	}
	restoreSecond := InstallGlobal(second)
	if !globalTargets(second) {
		t.Fatal("second provider was not routed globally")
	}

	restoreFirst()
	if !globalTargets(second) {
		t.Fatal("restoring older owner replaced current provider")
	}
	restoreSecond()
	if globalTargets(first) || globalTargets(second) {
		t.Fatal("final restore left an owned provider routed globally")
	}
	if err := first.Shutdown(context.Background()); err != nil {
		t.Fatalf("first Shutdown: %v", err)
	}
	if err := second.Shutdown(context.Background()); err != nil {
		t.Fatalf("second Shutdown: %v", err)
	}
}

func TestShutdownRestoresForgottenGlobalInstallation(t *testing.T) {
	provider := testProvider(t)
	InstallGlobal(provider)
	if !globalTargets(provider) {
		t.Fatal("provider was not installed")
	}
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if globalTargets(provider) {
		t.Fatal("Shutdown left a closed global meter provider installed")
	}
}

func TestPackageLevelInstrumentSurvivesTwoProviderLifecycles(t *testing.T) {
	counter, err := otel.Meter("package-level").Int64Counter("fit.routing.lifecycle")
	if err != nil {
		t.Fatal(err)
	}
	first, firstReader := testProviderWithReader(t)
	InstallGlobal(first)
	counter.Add(context.Background(), 1)
	if got := collectInt64Sum(t, firstReader, "fit.routing.lifecycle"); got != 1 {
		t.Fatalf("first lifecycle sum = %d", got)
	}
	if err := first.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	second, secondReader := testProviderWithReader(t)
	InstallGlobal(second)
	counter.Add(context.Background(), 2)
	if got := collectInt64Sum(t, secondReader, "fit.routing.lifecycle"); got != 2 {
		t.Fatalf("second lifecycle sum = %d", got)
	}
	if err := second.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPackageLevelObservableRegistrationSurvivesProviderLifecycle(t *testing.T) {
	meter := otel.Meter("package-level-observable")
	gauge, err := meter.Int64ObservableGauge("fit.routing.observable")
	if err != nil {
		t.Fatal(err)
	}
	registration, err := meter.RegisterCallback(func(_ context.Context, observer metric.Observer) error {
		observer.ObserveInt64(gauge, 7)
		return nil
	}, gauge)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = registration.Unregister() })

	first, firstReader := testProviderWithReader(t)
	InstallGlobal(first)
	if got := collectInt64Gauge(t, firstReader, "fit.routing.observable"); got != 7 {
		t.Fatalf("first lifecycle gauge = %d", got)
	}
	if err := first.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	second, secondReader := testProviderWithReader(t)
	InstallGlobal(second)
	if got := collectInt64Gauge(t, secondReader, "fit.routing.observable"); got != 7 {
		t.Fatalf("second lifecycle gauge = %d", got)
	}
	if err := second.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestRoutingProviderReusesEquivalentMeterScopes(t *testing.T) {
	provider := newRoutingMeterProvider(metricnoop.NewMeterProvider())
	first := provider.Meter(
		"scope",
		metric.WithInstrumentationVersion("1.0.0"),
		metric.WithInstrumentationAttributes(attribute.String("owner", "fit")),
	)
	second := provider.Meter(
		"scope",
		metric.WithInstrumentationAttributes(attribute.String("owner", "fit")),
		metric.WithInstrumentationVersion("1.0.0"),
	)
	if first != second {
		t.Fatal("equivalent meter scopes were not reused")
	}
	if distinct := provider.Meter("scope", metric.WithInstrumentationVersion("2.0.0")); distinct == first {
		t.Fatal("distinct meter scopes were incorrectly reused")
	}
	if got := len(provider.meters); got != 2 {
		t.Fatalf("retained meter scopes = %d, want 2", got)
	}
}

func TestInstallCannotRaceBlockingShutdown(t *testing.T) {
	provider := testProvider(t)
	InstallGlobal(provider)

	// Force Shutdown and InstallGlobal to contend for the lifecycle lock. It is
	// valid for install to linearize first or second, but no owner may survive.
	shutdownDone := make(chan error, 1)
	installDone := make(chan struct{})
	globalMu.Lock()
	go func() {
		restore := InstallGlobal(provider)
		restore()
		close(installDone)
	}()
	go func() { shutdownDone <- provider.Shutdown(context.Background()) }()
	globalMu.Unlock()
	select {
	case <-installDone:
	case <-time.After(time.Second):
		t.Fatal("InstallGlobal did not finish")
	}
	select {
	case err := <-shutdownDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Shutdown did not finish")
	}
	if globalTargets(provider) {
		t.Fatal("closed provider was reinstalled")
	}
}

func TestEnvironmentDurationOverflowUsesFallback(t *testing.T) {
	t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "9223372036854775807")
	if got := DefaultOptions().ExportInterval; got != defaultExportInterval {
		t.Fatalf("overflow interval = %s", got)
	}
}

func TestErrorHandlerReceivesRuntimeFailureWithoutFrameworkFormatting(t *testing.T) {
	secret := errors.New("transport secret")
	var observed error
	handler := otel.ErrorHandlerFunc(func(err error) { observed = err })
	provider := testProvider(t)
	provider.errorHandler = handler
	restore := InstallGlobal(provider)
	otel.Handle(secret)
	restore()
	if !errors.Is(observed, secret) {
		t.Fatalf("handler observed %v", observed)
	}
}

func TestErrorHandlerRemainsInstalledDuringSDKShutdown(t *testing.T) {
	want := errors.New("reader shutdown failure")
	exporter := &shutdownErrorExporter{shutdownError: want}
	var observed error
	provider, err := New(context.Background(), Options{
		MetricExporter: exporter,
		ErrorHandler: otel.ErrorHandlerFunc(func(err error) {
			observed = err
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	InstallGlobal(provider)
	if err := provider.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !errors.Is(observed, want) {
		t.Fatalf("shutdown error handler observed %v, want %v", observed, want)
	}
}

func TestSDKDisabledReturnsNoopProvider(t *testing.T) {
	t.Setenv("OTEL_SDK_DISABLED", "true")
	enabled := true
	provider, err := New(context.Background(), Options{Enabled: &enabled, Exporters: "console"})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if provider.IsEnabled() {
		t.Fatal("provider enabled while OTEL_SDK_DISABLED=true")
	}
	var _ metric.MeterProvider = provider.MeterProvider()
}

func testProvider(t *testing.T) *Provider {
	t.Helper()
	provider, _ := testProviderWithReader(t)
	return provider
}

func testProviderWithReader(t *testing.T) (*Provider, *sdkmetric.ManualReader) {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	enabled := true
	provider, err := New(context.Background(), Options{Enabled: &enabled, Readers: []sdkmetric.Reader{reader}})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return provider, reader
}

func globalTargets(provider *Provider) bool {
	globalMu.Lock()
	defer globalMu.Unlock()
	if globalRouter == nil {
		return false
	}
	owned, ok := globalRouter.current().(*ownedMeterProvider)
	return ok && owned.owner != nil && owned.owner.provider == provider && owned.owner.meterActive
}

func collectInt64Sum(t *testing.T, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	for _, scope := range collected.ScopeMetrics {
		for _, value := range scope.Metrics {
			if value.Name != name {
				continue
			}
			sum, ok := value.Data.(metricdata.Sum[int64])
			if !ok || len(sum.DataPoints) == 0 {
				t.Fatalf("metric %s data = %#v", name, value.Data)
			}
			return sum.DataPoints[0].Value
		}
	}
	t.Fatalf("metric %s was not collected", name)
	return 0
}

func collectInt64Gauge(t *testing.T, reader *sdkmetric.ManualReader, name string) int64 {
	t.Helper()
	var collected metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	for _, scope := range collected.ScopeMetrics {
		for _, value := range scope.Metrics {
			if value.Name != name {
				continue
			}
			gauge, ok := value.Data.(metricdata.Gauge[int64])
			if !ok || len(gauge.DataPoints) == 0 {
				t.Fatalf("metric %s data = %#v", name, value.Data)
			}
			return gauge.DataPoints[0].Value
		}
	}
	t.Fatalf("metric %s was not collected", name)
	return 0
}

type shutdownErrorExporter struct {
	mu            sync.Mutex
	shutdownError error
	shutdown      bool
}

func (*shutdownErrorExporter) Temporality(kind sdkmetric.InstrumentKind) metricdata.Temporality {
	return sdkmetric.DefaultTemporalitySelector(kind)
}

func (*shutdownErrorExporter) Aggregation(kind sdkmetric.InstrumentKind) sdkmetric.Aggregation {
	return sdkmetric.DefaultAggregationSelector(kind)
}

func (*shutdownErrorExporter) Export(context.Context, *metricdata.ResourceMetrics) error { return nil }

func (*shutdownErrorExporter) ForceFlush(context.Context) error { return nil }

func (exporter *shutdownErrorExporter) Shutdown(context.Context) error {
	exporter.mu.Lock()
	defer exporter.mu.Unlock()
	if exporter.shutdown {
		return nil
	}
	exporter.shutdown = true
	otel.Handle(exporter.shutdownError)
	return nil
}
