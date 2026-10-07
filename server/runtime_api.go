// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package server

import "github.com/gin-gonic/gin"

// RuntimeConfig configures the complete HTTP server runtime while Config keeps
// its original released layout.
type RuntimeConfig = AdvancedConfig

// AccessLogConfig configures redacted request/response access logging.
type AccessLogConfig = LogRequestResponseAdvancedConfig

// JWTVerificationOptions configures algorithms, claims, keys, and clock skew.
type JWTVerificationOptions = JWTAdvancedOptions

// OTelMiddlewareOptions configures HTTP span enrichment and response propagation.
type OTelMiddlewareOptions = OTelMiddlewareAdvancedConfig

// NewRuntime creates a server with complete runtime controls.
func NewRuntime(cfg RuntimeConfig) *Server {
	return NewAdvanced(cfg)
}

// AccessLog returns the configurable redacted request/response access-log middleware.
func AccessLog(cfg AccessLogConfig) gin.HandlerFunc {
	return LogRequestResponseAdvanced(cfg)
}

// GinAccessLog returns the Gin-compatible configurable access-log middleware.
func GinAccessLog(cfg AccessLogConfig) gin.HandlerFunc {
	return AccessLog(cfg)
}

// AuthorizeJWTTokenWithOptions validates JWTs using explicit verification options.
func AuthorizeJWTTokenWithOptions(opts JWTVerificationOptions) gin.HandlerFunc {
	return AuthorizeJWTTokenAdvanced(opts)
}

// RequestIDWithHeaderPropagation writes generated request IDs to the request
// header as well as response and request context.
func RequestIDWithHeaderPropagation() gin.HandlerFunc {
	return RequestIDAdvanced()
}

// OTelMiddlewareWithOptions returns HTTP tracing middleware using explicit options.
func OTelMiddlewareWithOptions(opts OTelMiddlewareOptions) gin.HandlerFunc {
	return OTelMiddlewareWithAdvancedConfig(opts)
}
