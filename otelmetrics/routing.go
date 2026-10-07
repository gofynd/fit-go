// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.

package otelmetrics

import (
	"context"
	"errors"
	"sync"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
)

// routingMeterProvider is installed once and changes its target over time.
// This is necessary because OTel's initial global provider delegates meters and
// instruments only on the first SetMeterProvider call.
type routingMeterProvider struct {
	metric.MeterProvider

	mu     sync.Mutex
	target metric.MeterProvider
	meters map[meterScope]*routingMeter
}

type meterScope struct {
	name       string
	version    string
	schemaURL  string
	attributes attribute.Distinct
}

func newRoutingMeterProvider(target metric.MeterProvider) *routingMeterProvider {
	if target == nil {
		target = metricnoop.NewMeterProvider()
	}
	return &routingMeterProvider{
		MeterProvider: metricnoop.NewMeterProvider(),
		target:        target,
		meters:        make(map[meterScope]*routingMeter),
	}
}

func (provider *routingMeterProvider) Meter(name string, options ...metric.MeterOption) metric.Meter {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	config := metric.NewMeterConfig(options...)
	attributes := config.InstrumentationAttributes()
	scope := meterScope{
		name:       name,
		version:    config.InstrumentationVersion(),
		schemaURL:  config.SchemaURL(),
		attributes: attributes.Equivalent(),
	}
	if meter, exists := provider.meters[scope]; exists {
		return meter
	}
	meter := newRoutingMeter(provider.target.Meter(name, options...), name, options)
	provider.meters[scope] = meter
	return meter
}

func (provider *routingMeterProvider) current() metric.MeterProvider {
	provider.mu.Lock()
	defer provider.mu.Unlock()
	return provider.target
}

func (provider *routingMeterProvider) setTarget(target metric.MeterProvider) {
	if target == nil {
		target = metricnoop.NewMeterProvider()
	}
	provider.mu.Lock()
	provider.target = target
	failed := false
	for _, meter := range provider.meters {
		if err := meter.setTarget(target.Meter(meter.name, meter.options...)); err != nil {
			failed = true
		}
	}
	provider.mu.Unlock()
	if failed {
		// Never hand raw provider errors to the process-global default handler.
		otel.Handle(errors.New("otelmetrics: failed to rebind a routed meter"))
	}
}

type routingMeter struct {
	metric.Meter

	mu            sync.Mutex
	name          string
	options       []metric.MeterOption
	target        metric.Meter
	instruments   []routedInstrument
	registrations []*routingRegistration
}

func newRoutingMeter(target metric.Meter, name string, options []metric.MeterOption) *routingMeter {
	return &routingMeter{
		Meter:   metricnoop.NewMeterProvider().Meter(name, options...),
		name:    name,
		options: append([]metric.MeterOption(nil), options...),
		target:  target,
	}
}

func (meter *routingMeter) setTarget(target metric.Meter) error {
	meter.mu.Lock()
	defer meter.mu.Unlock()
	meter.target = target
	var errs []error
	for _, instrument := range meter.instruments {
		if err := instrument.rebind(target); err != nil {
			errs = append(errs, err)
		}
	}
	for _, registration := range meter.registrations {
		if err := registration.rebind(target); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

type routedInstrument interface {
	rebind(metric.Meter) error
}

type instrumentRoute[T any] struct {
	mu      sync.RWMutex
	current T
	bind    func(metric.Meter) (T, error)
}

func (route *instrumentRoute[T]) rebind(meter metric.Meter) error {
	next, err := route.bind(meter)
	if err != nil {
		return err
	}
	route.mu.Lock()
	route.current = next
	route.mu.Unlock()
	return nil
}

func (route *instrumentRoute[T]) value() T {
	route.mu.RLock()
	defer route.mu.RUnlock()
	return route.current
}

type routingInt64Counter struct {
	metric.Int64Counter
	instrumentRoute[metric.Int64Counter]
}

func (instrument *routingInt64Counter) Add(ctx context.Context, value int64, options ...metric.AddOption) {
	instrument.value().Add(ctx, value, options...)
}

func (instrument *routingInt64Counter) Enabled(ctx context.Context) bool {
	return instrument.value().Enabled(ctx)
}

type routingInt64UpDownCounter struct {
	metric.Int64UpDownCounter
	instrumentRoute[metric.Int64UpDownCounter]
}

func (instrument *routingInt64UpDownCounter) Add(ctx context.Context, value int64, options ...metric.AddOption) {
	instrument.value().Add(ctx, value, options...)
}

func (instrument *routingInt64UpDownCounter) Enabled(ctx context.Context) bool {
	return instrument.value().Enabled(ctx)
}

type routingInt64Histogram struct {
	metric.Int64Histogram
	instrumentRoute[metric.Int64Histogram]
}

func (instrument *routingInt64Histogram) Record(ctx context.Context, value int64, options ...metric.RecordOption) {
	instrument.value().Record(ctx, value, options...)
}

func (instrument *routingInt64Histogram) Enabled(ctx context.Context) bool {
	return instrument.value().Enabled(ctx)
}

type routingInt64Gauge struct {
	metric.Int64Gauge
	instrumentRoute[metric.Int64Gauge]
}

func (instrument *routingInt64Gauge) Record(ctx context.Context, value int64, options ...metric.RecordOption) {
	instrument.value().Record(ctx, value, options...)
}

func (instrument *routingInt64Gauge) Enabled(ctx context.Context) bool {
	return instrument.value().Enabled(ctx)
}

type routingFloat64Counter struct {
	metric.Float64Counter
	instrumentRoute[metric.Float64Counter]
}

func (instrument *routingFloat64Counter) Add(ctx context.Context, value float64, options ...metric.AddOption) {
	instrument.value().Add(ctx, value, options...)
}

func (instrument *routingFloat64Counter) Enabled(ctx context.Context) bool {
	return instrument.value().Enabled(ctx)
}

type routingFloat64UpDownCounter struct {
	metric.Float64UpDownCounter
	instrumentRoute[metric.Float64UpDownCounter]
}

func (instrument *routingFloat64UpDownCounter) Add(ctx context.Context, value float64, options ...metric.AddOption) {
	instrument.value().Add(ctx, value, options...)
}

func (instrument *routingFloat64UpDownCounter) Enabled(ctx context.Context) bool {
	return instrument.value().Enabled(ctx)
}

type routingFloat64Histogram struct {
	metric.Float64Histogram
	instrumentRoute[metric.Float64Histogram]
}

func (instrument *routingFloat64Histogram) Record(ctx context.Context, value float64, options ...metric.RecordOption) {
	instrument.value().Record(ctx, value, options...)
}

func (instrument *routingFloat64Histogram) Enabled(ctx context.Context) bool {
	return instrument.value().Enabled(ctx)
}

type routingFloat64Gauge struct {
	metric.Float64Gauge
	instrumentRoute[metric.Float64Gauge]
}

func (instrument *routingFloat64Gauge) Record(ctx context.Context, value float64, options ...metric.RecordOption) {
	instrument.value().Record(ctx, value, options...)
}

func (instrument *routingFloat64Gauge) Enabled(ctx context.Context) bool {
	return instrument.value().Enabled(ctx)
}

type routingInt64ObservableCounter struct {
	metric.Int64ObservableCounter
	instrumentRoute[metric.Int64ObservableCounter]
	owner *routingMeter
}

func (instrument *routingInt64ObservableCounter) currentObservable() metric.Observable {
	return instrument.value()
}
func (instrument *routingInt64ObservableCounter) currentInt64Observable() metric.Int64Observable {
	return instrument.value()
}
func (instrument *routingInt64ObservableCounter) ownerMeter() *routingMeter { return instrument.owner }

type routingInt64ObservableUpDownCounter struct {
	metric.Int64ObservableUpDownCounter
	instrumentRoute[metric.Int64ObservableUpDownCounter]
	owner *routingMeter
}

func (instrument *routingInt64ObservableUpDownCounter) currentObservable() metric.Observable {
	return instrument.value()
}
func (instrument *routingInt64ObservableUpDownCounter) currentInt64Observable() metric.Int64Observable {
	return instrument.value()
}
func (instrument *routingInt64ObservableUpDownCounter) ownerMeter() *routingMeter {
	return instrument.owner
}

type routingInt64ObservableGauge struct {
	metric.Int64ObservableGauge
	instrumentRoute[metric.Int64ObservableGauge]
	owner *routingMeter
}

func (instrument *routingInt64ObservableGauge) currentObservable() metric.Observable {
	return instrument.value()
}
func (instrument *routingInt64ObservableGauge) currentInt64Observable() metric.Int64Observable {
	return instrument.value()
}
func (instrument *routingInt64ObservableGauge) ownerMeter() *routingMeter { return instrument.owner }

type routingFloat64ObservableCounter struct {
	metric.Float64ObservableCounter
	instrumentRoute[metric.Float64ObservableCounter]
	owner *routingMeter
}

func (instrument *routingFloat64ObservableCounter) currentObservable() metric.Observable {
	return instrument.value()
}
func (instrument *routingFloat64ObservableCounter) currentFloat64Observable() metric.Float64Observable {
	return instrument.value()
}
func (instrument *routingFloat64ObservableCounter) ownerMeter() *routingMeter {
	return instrument.owner
}

type routingFloat64ObservableUpDownCounter struct {
	metric.Float64ObservableUpDownCounter
	instrumentRoute[metric.Float64ObservableUpDownCounter]
	owner *routingMeter
}

func (instrument *routingFloat64ObservableUpDownCounter) currentObservable() metric.Observable {
	return instrument.value()
}
func (instrument *routingFloat64ObservableUpDownCounter) currentFloat64Observable() metric.Float64Observable {
	return instrument.value()
}
func (instrument *routingFloat64ObservableUpDownCounter) ownerMeter() *routingMeter {
	return instrument.owner
}

type routingFloat64ObservableGauge struct {
	metric.Float64ObservableGauge
	instrumentRoute[metric.Float64ObservableGauge]
	owner *routingMeter
}

func (instrument *routingFloat64ObservableGauge) currentObservable() metric.Observable {
	return instrument.value()
}
func (instrument *routingFloat64ObservableGauge) currentFloat64Observable() metric.Float64Observable {
	return instrument.value()
}
func (instrument *routingFloat64ObservableGauge) ownerMeter() *routingMeter { return instrument.owner }

type routedObservable interface {
	metric.Observable
	currentObservable() metric.Observable
	ownerMeter() *routingMeter
}

func (meter *routingMeter) Int64Counter(name string, options ...metric.Int64CounterOption) (metric.Int64Counter, error) {
	meter.mu.Lock()
	defer meter.mu.Unlock()
	current, err := meter.target.Int64Counter(name, options...)
	if err != nil {
		return nil, err
	}
	instrument := &routingInt64Counter{Int64Counter: current}
	instrument.current = current
	instrument.bind = func(target metric.Meter) (metric.Int64Counter, error) {
		return target.Int64Counter(name, options...)
	}
	meter.instruments = append(meter.instruments, instrument)
	return instrument, nil
}

func (meter *routingMeter) Int64UpDownCounter(name string, options ...metric.Int64UpDownCounterOption) (metric.Int64UpDownCounter, error) {
	meter.mu.Lock()
	defer meter.mu.Unlock()
	current, err := meter.target.Int64UpDownCounter(name, options...)
	if err != nil {
		return nil, err
	}
	instrument := &routingInt64UpDownCounter{Int64UpDownCounter: current}
	instrument.current = current
	instrument.bind = func(target metric.Meter) (metric.Int64UpDownCounter, error) {
		return target.Int64UpDownCounter(name, options...)
	}
	meter.instruments = append(meter.instruments, instrument)
	return instrument, nil
}

func (meter *routingMeter) Int64Histogram(name string, options ...metric.Int64HistogramOption) (metric.Int64Histogram, error) {
	meter.mu.Lock()
	defer meter.mu.Unlock()
	current, err := meter.target.Int64Histogram(name, options...)
	if err != nil {
		return nil, err
	}
	instrument := &routingInt64Histogram{Int64Histogram: current}
	instrument.current = current
	instrument.bind = func(target metric.Meter) (metric.Int64Histogram, error) {
		return target.Int64Histogram(name, options...)
	}
	meter.instruments = append(meter.instruments, instrument)
	return instrument, nil
}

func (meter *routingMeter) Int64Gauge(name string, options ...metric.Int64GaugeOption) (metric.Int64Gauge, error) {
	meter.mu.Lock()
	defer meter.mu.Unlock()
	current, err := meter.target.Int64Gauge(name, options...)
	if err != nil {
		return nil, err
	}
	instrument := &routingInt64Gauge{Int64Gauge: current}
	instrument.current = current
	instrument.bind = func(target metric.Meter) (metric.Int64Gauge, error) {
		return target.Int64Gauge(name, options...)
	}
	meter.instruments = append(meter.instruments, instrument)
	return instrument, nil
}

func (meter *routingMeter) Float64Counter(name string, options ...metric.Float64CounterOption) (metric.Float64Counter, error) {
	meter.mu.Lock()
	defer meter.mu.Unlock()
	current, err := meter.target.Float64Counter(name, options...)
	if err != nil {
		return nil, err
	}
	instrument := &routingFloat64Counter{Float64Counter: current}
	instrument.current = current
	instrument.bind = func(target metric.Meter) (metric.Float64Counter, error) {
		return target.Float64Counter(name, options...)
	}
	meter.instruments = append(meter.instruments, instrument)
	return instrument, nil
}

func (meter *routingMeter) Float64UpDownCounter(name string, options ...metric.Float64UpDownCounterOption) (metric.Float64UpDownCounter, error) {
	meter.mu.Lock()
	defer meter.mu.Unlock()
	current, err := meter.target.Float64UpDownCounter(name, options...)
	if err != nil {
		return nil, err
	}
	instrument := &routingFloat64UpDownCounter{Float64UpDownCounter: current}
	instrument.current = current
	instrument.bind = func(target metric.Meter) (metric.Float64UpDownCounter, error) {
		return target.Float64UpDownCounter(name, options...)
	}
	meter.instruments = append(meter.instruments, instrument)
	return instrument, nil
}

func (meter *routingMeter) Float64Histogram(name string, options ...metric.Float64HistogramOption) (metric.Float64Histogram, error) {
	meter.mu.Lock()
	defer meter.mu.Unlock()
	current, err := meter.target.Float64Histogram(name, options...)
	if err != nil {
		return nil, err
	}
	instrument := &routingFloat64Histogram{Float64Histogram: current}
	instrument.current = current
	instrument.bind = func(target metric.Meter) (metric.Float64Histogram, error) {
		return target.Float64Histogram(name, options...)
	}
	meter.instruments = append(meter.instruments, instrument)
	return instrument, nil
}

func (meter *routingMeter) Float64Gauge(name string, options ...metric.Float64GaugeOption) (metric.Float64Gauge, error) {
	meter.mu.Lock()
	defer meter.mu.Unlock()
	current, err := meter.target.Float64Gauge(name, options...)
	if err != nil {
		return nil, err
	}
	instrument := &routingFloat64Gauge{Float64Gauge: current}
	instrument.current = current
	instrument.bind = func(target metric.Meter) (metric.Float64Gauge, error) {
		return target.Float64Gauge(name, options...)
	}
	meter.instruments = append(meter.instruments, instrument)
	return instrument, nil
}

func (meter *routingMeter) Int64ObservableCounter(name string, options ...metric.Int64ObservableCounterOption) (metric.Int64ObservableCounter, error) {
	meter.mu.Lock()
	defer meter.mu.Unlock()
	current, err := meter.target.Int64ObservableCounter(name, options...)
	if err != nil {
		return nil, err
	}
	instrument := &routingInt64ObservableCounter{Int64ObservableCounter: current, owner: meter}
	instrument.current = current
	instrument.bind = func(target metric.Meter) (metric.Int64ObservableCounter, error) {
		return target.Int64ObservableCounter(name, options...)
	}
	meter.instruments = append(meter.instruments, instrument)
	return instrument, nil
}

func (meter *routingMeter) Int64ObservableUpDownCounter(name string, options ...metric.Int64ObservableUpDownCounterOption) (metric.Int64ObservableUpDownCounter, error) {
	meter.mu.Lock()
	defer meter.mu.Unlock()
	current, err := meter.target.Int64ObservableUpDownCounter(name, options...)
	if err != nil {
		return nil, err
	}
	instrument := &routingInt64ObservableUpDownCounter{Int64ObservableUpDownCounter: current, owner: meter}
	instrument.current = current
	instrument.bind = func(target metric.Meter) (metric.Int64ObservableUpDownCounter, error) {
		return target.Int64ObservableUpDownCounter(name, options...)
	}
	meter.instruments = append(meter.instruments, instrument)
	return instrument, nil
}

func (meter *routingMeter) Int64ObservableGauge(name string, options ...metric.Int64ObservableGaugeOption) (metric.Int64ObservableGauge, error) {
	meter.mu.Lock()
	defer meter.mu.Unlock()
	current, err := meter.target.Int64ObservableGauge(name, options...)
	if err != nil {
		return nil, err
	}
	instrument := &routingInt64ObservableGauge{Int64ObservableGauge: current, owner: meter}
	instrument.current = current
	instrument.bind = func(target metric.Meter) (metric.Int64ObservableGauge, error) {
		return target.Int64ObservableGauge(name, options...)
	}
	meter.instruments = append(meter.instruments, instrument)
	return instrument, nil
}

func (meter *routingMeter) Float64ObservableCounter(name string, options ...metric.Float64ObservableCounterOption) (metric.Float64ObservableCounter, error) {
	meter.mu.Lock()
	defer meter.mu.Unlock()
	current, err := meter.target.Float64ObservableCounter(name, options...)
	if err != nil {
		return nil, err
	}
	instrument := &routingFloat64ObservableCounter{Float64ObservableCounter: current, owner: meter}
	instrument.current = current
	instrument.bind = func(target metric.Meter) (metric.Float64ObservableCounter, error) {
		return target.Float64ObservableCounter(name, options...)
	}
	meter.instruments = append(meter.instruments, instrument)
	return instrument, nil
}

func (meter *routingMeter) Float64ObservableUpDownCounter(name string, options ...metric.Float64ObservableUpDownCounterOption) (metric.Float64ObservableUpDownCounter, error) {
	meter.mu.Lock()
	defer meter.mu.Unlock()
	current, err := meter.target.Float64ObservableUpDownCounter(name, options...)
	if err != nil {
		return nil, err
	}
	instrument := &routingFloat64ObservableUpDownCounter{Float64ObservableUpDownCounter: current, owner: meter}
	instrument.current = current
	instrument.bind = func(target metric.Meter) (metric.Float64ObservableUpDownCounter, error) {
		return target.Float64ObservableUpDownCounter(name, options...)
	}
	meter.instruments = append(meter.instruments, instrument)
	return instrument, nil
}

func (meter *routingMeter) Float64ObservableGauge(name string, options ...metric.Float64ObservableGaugeOption) (metric.Float64ObservableGauge, error) {
	meter.mu.Lock()
	defer meter.mu.Unlock()
	current, err := meter.target.Float64ObservableGauge(name, options...)
	if err != nil {
		return nil, err
	}
	instrument := &routingFloat64ObservableGauge{Float64ObservableGauge: current, owner: meter}
	instrument.current = current
	instrument.bind = func(target metric.Meter) (metric.Float64ObservableGauge, error) {
		return target.Float64ObservableGauge(name, options...)
	}
	meter.instruments = append(meter.instruments, instrument)
	return instrument, nil
}

func (meter *routingMeter) RegisterCallback(callback metric.Callback, instruments ...metric.Observable) (metric.Registration, error) {
	meter.mu.Lock()
	defer meter.mu.Unlock()
	for _, instrument := range instruments {
		routed, ok := instrument.(routedObservable)
		if !ok || routed.ownerMeter() != meter {
			return nil, errors.New("otelmetrics: callback instruments must come from the same routed meter")
		}
	}
	registration := &routingRegistration{
		callback:    callback,
		instruments: append([]metric.Observable(nil), instruments...),
		active:      true,
	}
	if err := registration.rebind(meter.target); err != nil {
		return nil, err
	}
	meter.registrations = append(meter.registrations, registration)
	return registration, nil
}

type routingRegistration struct {
	metric.Registration

	mu          sync.Mutex
	current     metric.Registration
	callback    metric.Callback
	instruments []metric.Observable
	active      bool
}

func (registration *routingRegistration) rebind(meter metric.Meter) error {
	registration.mu.Lock()
	defer registration.mu.Unlock()
	if !registration.active {
		return nil
	}
	unwrapped := make([]metric.Observable, len(registration.instruments))
	observables := make(map[routedObservable]metric.Observable, len(registration.instruments))
	for index, instrument := range registration.instruments {
		routed := instrument.(routedObservable)
		unwrapped[index] = routed.currentObservable()
		observables[routed] = unwrapped[index]
	}
	next, err := meter.RegisterCallback(routeCallback(registration.callback, observables), unwrapped...)
	if err != nil {
		return err
	}
	previous := registration.current
	registration.current = next
	registration.Registration = next
	if previous != nil {
		return previous.Unregister()
	}
	return nil
}

func (registration *routingRegistration) Unregister() error {
	registration.mu.Lock()
	defer registration.mu.Unlock()
	if !registration.active {
		return nil
	}
	registration.active = false
	if registration.current == nil {
		return nil
	}
	err := registration.current.Unregister()
	registration.current = nil
	return err
}

func routeCallback(callback metric.Callback, observables map[routedObservable]metric.Observable) metric.Callback {
	return func(ctx context.Context, observer metric.Observer) error {
		return callback(ctx, routingObserver{Observer: observer, observables: observables})
	}
}

type routingObserver struct {
	metric.Observer
	observables map[routedObservable]metric.Observable
}

func (observer routingObserver) ObserveInt64(instrument metric.Int64Observable, value int64, options ...metric.ObserveOption) {
	if routed, ok := instrument.(routedObservable); ok {
		if current, exists := observer.observables[routed]; exists {
			instrument = current.(metric.Int64Observable)
		} else if current, ok := instrument.(interface{ currentInt64Observable() metric.Int64Observable }); ok {
			instrument = current.currentInt64Observable()
		}
	}
	observer.Observer.ObserveInt64(instrument, value, options...)
}

func (observer routingObserver) ObserveFloat64(instrument metric.Float64Observable, value float64, options ...metric.ObserveOption) {
	if routed, ok := instrument.(routedObservable); ok {
		if current, exists := observer.observables[routed]; exists {
			instrument = current.(metric.Float64Observable)
		} else if current, ok := instrument.(interface {
			currentFloat64Observable() metric.Float64Observable
		}); ok {
			instrument = current.currentFloat64Observable()
		}
	}
	observer.Observer.ObserveFloat64(instrument, value, options...)
}
