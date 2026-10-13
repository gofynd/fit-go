package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gofynd/fit-go/kafka"
)

// This fake implements the original public consumer boundary, including an
// idle Consume that exits only after Close. No broker or runtime changes are used.
type demoConsumer struct {
	closed     chan struct{}
	consumeErr error
	closeErr   error
	closeCalls atomic.Int32
}

var _ kafka.KafkaConsumer = (*demoConsumer)(nil)

func (c *demoConsumer) Connect([]kafka.TopicConfig) error { return nil }

func (c *demoConsumer) Consume(kafka.MessageHandler, kafka.ConsumerOptions) error {
	if c.consumeErr != nil {
		return c.consumeErr
	}
	<-c.closed
	return nil
}

func (*demoConsumer) ConsumeBatch(kafka.BatchHandler, kafka.ConsumerOptions) error {
	return errors.New("batch consumption is not used by this example")
}

func (c *demoConsumer) Close() error {
	if c.closeCalls.Add(1) == 1 {
		close(c.closed)
	}
	return c.closeErr
}

func TestConsumeForClosesIdleConsumer(t *testing.T) {
	c := &demoConsumer{closed: make(chan struct{})}
	done := make(chan error, 1)
	go func() {
		done <- consumeFor(c, func(context.Context, kafka.MessagePayload) error {
			t.Error("idle consumer must not call the handler")
			return nil
		}, 10*time.Millisecond)
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("idle consumption did not stop after timer-triggered Close")
	}
	if got := c.closeCalls.Load(); got != 1 {
		t.Fatalf("Close calls = %d, want 1", got)
	}
}

func TestConsumeForEarlyErrorClosesOnce(t *testing.T) {
	want := errors.New("consume failed")
	c := &demoConsumer{closed: make(chan struct{}), consumeErr: want}
	if err := consumeFor(c, nil, time.Hour); !errors.Is(err, want) {
		t.Fatalf("error = %v, want consume failure", err)
	}
	if got := c.closeCalls.Load(); got != 1 {
		t.Fatalf("Close calls = %d, want 1", got)
	}
}

func TestConsumeForPreservesCloseError(t *testing.T) {
	wantConsume := errors.New("consume failed")
	wantClose := errors.New("close failed")
	c := &demoConsumer{closed: make(chan struct{}), consumeErr: wantConsume, closeErr: wantClose}
	err := consumeFor(c, nil, time.Hour)
	if !errors.Is(err, wantConsume) || !errors.Is(err, wantClose) {
		t.Fatalf("error = %v, want both consume and close failures", err)
	}
	if got := c.closeCalls.Load(); got != 1 {
		t.Fatalf("Close calls = %d, want 1", got)
	}
}

func TestConsumeForImmediateTimerClosesOnce(t *testing.T) {
	for range 100 {
		c := &demoConsumer{closed: make(chan struct{})}
		if err := consumeFor(c, nil, 0); err != nil {
			t.Fatal(err)
		}
		if got := c.closeCalls.Load(); got != 1 {
			t.Fatalf("Close calls = %d, want 1", got)
		}
	}
}
