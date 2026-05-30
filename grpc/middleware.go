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

// Package grpc middleware provides gRPC interceptors for the fit.go framework.
//
// It provides:
// - JWT authorization interceptor (authorize-jwt-token.ts)
// - Health check handler (health.methods.ts)
// - Interceptor type definitions for unary and stream RPCs
package grpc

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/golang-jwt/jwt/v5"

	"github.com/gofynd/fit-go/health"
)

// ---------------------------------------------------------------------------
// Interceptor types
// ---------------------------------------------------------------------------

// UnaryInterceptor is a function that intercepts unary RPCs.
// It receives the call info and a handler, and can modify the request,
// response, or short-circuit the call.
type UnaryInterceptor func(call *CallInfo, callback Callback, next NextFunc)

// StreamInterceptor is a function that intercepts streaming RPCs.
type StreamInterceptor func(call *CallInfo, callback Callback, next NextFunc)

// ---------------------------------------------------------------------------
// JWT Authorization
// ---------------------------------------------------------------------------

// JWTConfig holds configuration for the JWT authorization interceptor.
type JWTConfig struct {
	// Payload is the expected JWT payload to match against. If nil, the
	// interceptor extracts {company_id} from the request automatically.
	Payload map[string]interface{}

	// Secret is the JWT signing secret. If empty, falls back to the
	// JWT_SECRET_DELETE_ENTITY environment variable.
	Secret string

	// RSAPublicKeyPEM is the PEM-encoded RSA public key for RS256 verification.
	// Only used when the token's alg header is RS256.
	RSAPublicKeyPEM string

	// AllowedAlgorithms specifies which signing algorithms are accepted.
	// Supported: HS256, HS384, HS512, RS256. If empty, defaults to [HS256].
	AllowedAlgorithms []string

	// AllowedClockSkew is the maximum allowed clock skew for token
	// expiration validation. Default: 0 (no skew allowed).
	AllowedClockSkew time.Duration
}

// defaultAlgorithms is the default set of allowed JWT algorithms.
var defaultAlgorithms = []string{"HS256"}

// AuthorizeJWTToken returns a gRPC middleware HandlerFunc that validates
// JWT tokens from the "authorization" metadata header.
//
// Behavior:
// - Extracts Bearer token from the "authorization" metadata key
// - Verifies the token signature using the configured algorithm(s)
// - Compares the decoded payload against the expected payload
// - Sets call.Decoded with the decoded token on success
// - Calls callback with Unauthenticated error on failure
func AuthorizeJWTToken(cfg JWTConfig) HandlerFunc {
	return func(call *CallInfo, callback Callback, next NextFunc) {
		secret := cfg.Secret
		if secret == "" {
			secret = os.Getenv("JWT_SECRET_DELETE_ENTITY")
		}

		// Extract Bearer token from metadata.
		authHeader := call.Metadata.Get("authorization")
		token := strings.TrimPrefix(authHeader, "Bearer ")
		token = strings.TrimSpace(token)

		if token == "" || token == authHeader {
			callback(&RPCError{Code: Unauthenticated, Message: "Unauthorized"}, nil)
			return
		}

		// Determine allowed algorithms.
		allowedAlgs := cfg.AllowedAlgorithms
		if len(allowedAlgs) == 0 {
			allowedAlgs = defaultAlgorithms
		}

		// Parse and verify the JWT token.
		decoded, err := verifyJWTToken(token, secret, cfg.RSAPublicKeyPEM, allowedAlgs, cfg.AllowedClockSkew)
		if err != nil {
			callback(&RPCError{Code: Unauthenticated, Message: "Unauthorized"}, nil)
			return
		}

		// Determine expected payload.
		expectedPayload := cfg.Payload
		if expectedPayload == nil {
			// Default: extract company_id from request (matching JS behavior).
			if companyID, ok := call.Request["company_id"]; ok {
				expectedPayload = map[string]interface{}{
					"company_id": companyID,
				}
			}
		}

		// Compare payloads (excluding standard JWT claims).
		if expectedPayload != nil {
			authorized := comparePayloads(expectedPayload, decoded)
			if !authorized {
				callback(&RPCError{Code: Unauthenticated, Message: "Unauthorized"}, nil)
				return
			}
		}

		// Attach decoded token to call.
		call.Decoded = decoded
		next(nil)
	}
}

// verifyJWTToken parses and verifies a JWT token using the golang-jwt library.
// It supports HS256, HS384, HS512, and RS256 algorithms.
func verifyJWTToken(tokenStr, secret, rsaPubKeyPEM string, allowedAlgs []string, clockSkew time.Duration) (map[string]interface{}, error) {
	// Build the set of allowed signing methods.
	allowedMethodSet := make(map[string]bool, len(allowedAlgs))
	for _, alg := range allowedAlgs {
		allowedMethodSet[alg] = true
	}

	// Set parser options.
	parserOpts := []jwt.ParserOption{
		jwt.WithValidMethods(allowedAlgs),
	}
	if clockSkew > 0 {
		parserOpts = append(parserOpts, jwt.WithLeeway(clockSkew))
	}

	// Parse the token with a key function that selects the appropriate key
	// based on the algorithm.
	parsedToken, err := jwt.Parse(tokenStr, func(t *jwt.Token) (interface{}, error) {
		switch t.Method.Alg() {
		case "HS256", "HS384", "HS512":
			if secret == "" {
				return nil, fmt.Errorf("no HMAC secret configured for algorithm %s", t.Method.Alg())
			}
			return []byte(secret), nil
		case "RS256":
			if rsaPubKeyPEM == "" {
				return nil, fmt.Errorf("no RSA public key configured for RS256")
			}
			pubKey, err := jwt.ParseRSAPublicKeyFromPEM([]byte(rsaPubKeyPEM))
			if err != nil {
				return nil, fmt.Errorf("failed to parse RSA public key: %w", err)
			}
			return pubKey, nil
		default:
			return nil, fmt.Errorf("unsupported signing algorithm: %s", t.Method.Alg())
		}
	}, parserOpts...)

	if err != nil {
		return nil, fmt.Errorf("token verification failed: %w", err)
	}

	if !parsedToken.Valid {
		return nil, fmt.Errorf("token is not valid")
	}

	// Extract claims as a map.
	claims, ok := parsedToken.Claims.(jwt.MapClaims)
	if !ok {
		return nil, fmt.Errorf("unexpected claims type")
	}

	// Convert to plain map[string]interface{}.
	result := make(map[string]interface{}, len(claims))
	for k, v := range claims {
		result[k] = v
	}

	return result, nil
}

// standardJWTClaims are the claims excluded when comparing payloads.
// Matches the JS omit(decoded, ["iat", "exp", "nbf", "aud", "sub", "iss"]).
var standardJWTClaims = map[string]bool{
	"iat": true,
	"exp": true,
	"nbf": true,
	"aud": true,
	"sub": true,
	"iss": true,
}

// comparePayloads checks if the expected payload matches the decoded token
// payload, ignoring standard JWT claims.
func comparePayloads(expected, decoded map[string]interface{}) bool {
	// Build actual payload without standard claims.
	actual := make(map[string]interface{})
	for k, v := range decoded {
		if !standardJWTClaims[k] {
			actual[k] = v
		}
	}

	if len(expected) != len(actual) {
		return false
	}

	for k, ev := range expected {
		av, ok := actual[k]
		if !ok {
			return false
		}
		// Compare JSON representations for deep equality.
		ej, _ := json.Marshal(ev)
		aj, _ := json.Marshal(av)
		if string(ej) != string(aj) {
			return false
		}
	}
	return true
}

// ---------------------------------------------------------------------------
// Health check handler
// ---------------------------------------------------------------------------

// HealthCheckHandler returns a HandlerFunc that implements the gRPC Health
// Check protocol (grpc.health.v1.Health/Check and Watch).
//
// Returns:
// - {"status": "SERVING"} when all health checks pass
// - {"status": "NOT_SERVING", "meta": {"error_messages": "..."}} on failure
func HealthCheckHandler(checker *health.Checker) HandlerFunc {
	return func(call *CallInfo, callback Callback, next NextFunc) {
		errs := checker.Check()
		if len(errs) > 0 {
			callback(nil, map[string]interface{}{
				"status": "NOT_SERVING",
				"meta": map[string]interface{}{
					"error_messages": strings.Join(errs, ", "),
				},
			})
		} else {
			callback(nil, map[string]interface{}{
				"status": "SERVING",
			})
		}
	}
}

// ---------------------------------------------------------------------------
// Chain helpers
// ---------------------------------------------------------------------------

// ChainUnaryInterceptors chains multiple unary interceptors into a single
// HandlerFunc. Each interceptor must call next() to proceed to the next one.
func ChainUnaryInterceptors(interceptors ...UnaryInterceptor) HandlerFunc {
	return func(call *CallInfo, callback Callback, next NextFunc) {
		if len(interceptors) == 0 {
			next(nil)
			return
		}

		// Build the chain from the last interceptor backwards.
		var chainNext NextFunc
		chainNext = next

		for i := len(interceptors) - 1; i >= 0; i-- {
			interceptor := interceptors[i]
			currentNext := chainNext
			chainNext = func(err error) {
				if err != nil {
					next(err)
					return
				}
				interceptor(call, callback, currentNext)
			}
		}

		chainNext(nil)
	}
}
