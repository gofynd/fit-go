// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package tracing

import (
	"context"
	"testing"
)

// newOTLPExporter must construct cleanly for both protocols when the endpoint is
// a full URL (scheme+host+port) — the case that broke export with WithEndpoint
// (it produced "http://http://..."). WithEndpointURL parses it correctly.
func TestNewOTLPExporter_URLEndpointBothProtocols(t *testing.T) {
	for _, proto := range []string{"", "http/protobuf", "grpc"} {
		exp, err := newOTLPExporter(context.Background(), Options{
			Endpoint: "http://collector.example.svc.cluster.local:4318",
			Protocol: proto,
		})
		if err != nil {
			t.Fatalf("protocol %q: newOTLPExporter returned error: %v", proto, err)
		}
		_ = exp.Shutdown(context.Background())
	}
}
