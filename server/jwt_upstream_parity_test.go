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

package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/sha512"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// ---------------------------------------------------------------------------
// Reference oracle: verbatim copy of upstream main (df96a28) server/middleware.go
// AuthorizeJWTToken + verifyHS256. Only change: time.Now is replaced by an
// injected clock so the comparison is deterministic.
// ---------------------------------------------------------------------------

func upstreamAuthorizeJWTToken(opts JWTOptions, now func() time.Time) gin.HandlerFunc {
	return func(c *gin.Context) {
		secret := opts.Secret
		if secret == "" {
			secret = envGet("JWT_SECRET_DELETE_ENTITY", "")
		}
		authHeader := c.GetHeader("Authorization")
		token := strings.TrimPrefix(authHeader, "Bearer ")
		token = strings.TrimSpace(token)
		if token == "" || token == authHeader {
			c.AbortWithStatusJSON(http.StatusUnauthorized, map[string]string{"message": "Unauthorized"})
			return
		}
		claims, err := upstreamVerifyHS256(token, secret, now)
		if err != nil {
			c.AbortWithStatusJSON(http.StatusUnauthorized, map[string]string{"message": "Unauthorized"})
			return
		}
		c.Set(ginKeyDecodedToken, claims)
		ctx := context.WithValue(c.Request.Context(), ctxKeyDecodedToken, claims)
		c.Request = c.Request.WithContext(ctx)
		expected := opts.ExpectedPayload
		if expected == nil {
			companyID := c.Param("company_id")
			if companyID != "" {
				expected = map[string]interface{}{"company_id": companyID}
			}
		}
		if expected != nil {
			actual := make(map[string]interface{})
			for k, v := range claims {
				if _, skip := standardClaims[k]; !skip {
					actual[k] = v
				}
			}
			if !jsonEqual(expected, actual) {
				c.AbortWithStatusJSON(http.StatusUnauthorized, map[string]string{"message": "Unauthorized"})
				return
			}
		}
		c.Next()
	}
}

func upstreamVerifyHS256(tokenStr, secret string, nowFn func() time.Time) (map[string]interface{}, error) {
	parts := strings.Split(tokenStr, ".")
	if len(parts) != 3 {
		return nil, fmt.Errorf("jwt: invalid token format")
	}
	headerBytes, err := upstreamBase64URLDecode(parts[0])
	if err != nil {
		return nil, fmt.Errorf("jwt: invalid header encoding: %w", err)
	}
	var header map[string]interface{}
	if err := json.Unmarshal(headerBytes, &header); err != nil {
		return nil, fmt.Errorf("jwt: invalid header JSON: %w", err)
	}
	if alg, _ := header["alg"].(string); alg != "HS256" {
		return nil, fmt.Errorf("jwt: unsupported algorithm %q", alg)
	}
	signingInput := parts[0] + "." + parts[1]
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(signingInput))
	expectedSig := mac.Sum(nil)
	actualSig, err := upstreamBase64URLDecode(parts[2])
	if err != nil {
		return nil, fmt.Errorf("jwt: invalid signature encoding: %w", err)
	}
	if !hmac.Equal(expectedSig, actualSig) {
		return nil, fmt.Errorf("jwt: signature verification failed")
	}
	payloadBytes, err := upstreamBase64URLDecode(parts[1])
	if err != nil {
		return nil, fmt.Errorf("jwt: invalid payload encoding: %w", err)
	}
	var claims map[string]interface{}
	if err := json.Unmarshal(payloadBytes, &claims); err != nil {
		return nil, fmt.Errorf("jwt: invalid payload JSON: %w", err)
	}
	now := float64(nowFn().Unix())
	if exp, ok := claims["exp"].(float64); ok && now > exp {
		return nil, fmt.Errorf("jwt: token expired")
	}
	if nbf, ok := claims["nbf"].(float64); ok && now < nbf {
		return nil, fmt.Errorf("jwt: token not yet valid")
	}
	return claims, nil
}

func upstreamBase64URLDecode(s string) ([]byte, error) {
	switch len(s) % 4 {
	case 2:
		s += "=="
	case 3:
		s += "="
	}
	return base64.URLEncoding.DecodeString(s)
}

// ---------------------------------------------------------------------------
// Token generation helpers
// ---------------------------------------------------------------------------

func parityEncode(raw []byte, padded bool) string {
	if padded {
		return base64.URLEncoding.EncodeToString(raw)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

func paritySign(header, payload, secret string, h func() hash.Hash, padded bool) string {
	input := parityEncode([]byte(header), padded) + "." + parityEncode([]byte(payload), padded)
	mac := hmac.New(h, []byte(secret))
	mac.Write([]byte(input))
	return input + "." + parityEncode(mac.Sum(nil), padded)
}

type jwtParityResult struct {
	status        int
	body          string
	handlerCalled bool
	ginValue      interface{}
	ginExists     bool
	ctxValue      interface{}
	decoded       map[string]interface{}
}

func runJWTParity(t *testing.T, mw gin.HandlerFunc, route, path, authHeader string, setHeader bool) jwtParityResult {
	t.Helper()
	var res jwtParityResult
	engine := gin.New()
	engine.Use(mw)
	engine.GET(route, func(c *gin.Context) {
		res.handlerCalled = true
		res.ginValue, res.ginExists = c.Get(ginKeyDecodedToken)
		res.ctxValue = c.Request.Context().Value(ctxKeyDecodedToken)
		res.decoded = DecodedTokenFromContext(c.Request.Context())
		c.Status(http.StatusNoContent)
	})
	req := httptest.NewRequest(http.MethodGet, path, nil)
	if setHeader {
		req.Header.Set("Authorization", authHeader)
	}
	rec := httptest.NewRecorder()
	engine.ServeHTTP(rec, req)
	res.status = rec.Code
	res.body = rec.Body.String()
	return res
}

// TestAuthorizeJWTTokenMatchesUpstreamMain asserts that AuthorizeJWTToken
// produces exactly the same accept/reject decision, response status/body and
// context values as the upstream main implementation for a broad generated
// set of tokens, and pins the explicitly expected main decisions.
func TestAuthorizeJWTTokenMatchesUpstreamMain(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Setenv("JWT_SECRET_DELETE_ENTITY", "")

	fixedNow := time.Unix(1_800_000_000, 0)
	nowFn := func() time.Time { return fixedNow }
	prev := legacyJWTNow
	legacyJWTNow = nowFn
	t.Cleanup(func() { legacyJWTNow = prev })

	const secret = "parity-secret"
	nowSec := fixedNow.Unix()
	hs256 := `{"alg":"HS256","typ":"JWT"}`
	past := fmt.Sprint(nowSec - 3600)
	future := fmt.Sprint(nowSec + 3600)
	nowStr := fmt.Sprint(nowSec)

	type tc struct {
		name       string
		secret     string // JWTOptions.Secret
		expected   map[string]interface{}
		route      string
		path       string
		header     string
		noHeader   bool
		wantAccept bool // main's decision, derived from df96a28 source
	}
	sign := func(h, p string) string { return paritySign(h, p, secret, sha256.New, false) }
	valid := sign(hs256, `{"company_id":"123","exp":`+future+`}`)

	cases := []tc{
		{name: "valid HS256", header: "Bearer " + valid, wantAccept: true},
		{name: "wrong signature", header: "Bearer " + paritySign(hs256, `{"company_id":"123"}`, "other", sha256.New, false)},
		{name: "alg none", header: "Bearer " + parityEncode([]byte(`{"alg":"none"}`), false) + "." + parityEncode([]byte(`{"a":1}`), false) + "."},
		{name: "alg RS256 header HMAC-signed", header: "Bearer " + sign(`{"alg":"RS256"}`, `{"a":1}`)},
		{name: "alg HS512 header HS512-signed", header: "Bearer " + paritySign(`{"alg":"HS512"}`, `{"a":1}`, secret, sha512.New, false)},
		{name: "alg HS512 header HS256-signed", header: "Bearer " + sign(`{"alg":"HS512"}`, `{"a":1}`)},
		{name: "alg missing", header: "Bearer " + sign(`{"typ":"JWT"}`, `{"a":1}`)},
		{name: "alg non-string", header: "Bearer " + sign(`{"alg":256}`, `{"a":1}`)},
		{name: "exp far past", header: "Bearer " + sign(hs256, `{"exp":`+past+`}`)},
		{name: "exp far future", header: "Bearer " + sign(hs256, `{"exp":`+future+`}`), wantAccept: true},
		{name: "exp zero rejected as expired", header: "Bearer " + sign(hs256, `{"exp":0}`)},
		{name: "exp equal now accepted", header: "Bearer " + sign(hs256, `{"exp":`+nowStr+`}`), wantAccept: true},
		{name: "exp now minus one rejected", header: "Bearer " + sign(hs256, `{"exp":`+fmt.Sprint(nowSec-1)+`}`)},
		{name: "exp fractional", header: "Bearer " + sign(hs256, `{"exp":`+nowStr+`.5}`), wantAccept: true},
		{name: "exp string ignored", header: "Bearer " + sign(hs256, `{"exp":"`+past+`"}`), wantAccept: true},
		{name: "exp null ignored", header: "Bearer " + sign(hs256, `{"exp":null}`), wantAccept: true},
		{name: "exp bool ignored", header: "Bearer " + sign(hs256, `{"exp":true}`), wantAccept: true},
		{name: "nbf future", header: "Bearer " + sign(hs256, `{"nbf":`+future+`}`)},
		{name: "nbf past", header: "Bearer " + sign(hs256, `{"nbf":`+past+`}`), wantAccept: true},
		{name: "nbf equal now", header: "Bearer " + sign(hs256, `{"nbf":`+nowStr+`}`), wantAccept: true},
		{name: "nbf string ignored", header: "Bearer " + sign(hs256, `{"nbf":"`+future+`"}`), wantAccept: true},
		{name: "missing exp", header: "Bearer " + sign(hs256, `{"company_id":"123"}`), wantAccept: true},
		{name: "empty payload object", header: "Bearer " + sign(hs256, `{}`), wantAccept: true},
		{name: "null payload accepted", header: "Bearer " + sign(hs256, `null`), wantAccept: true},
		{name: "string payload rejected", header: "Bearer " + sign(hs256, `"value"`)},
		{name: "array payload rejected", header: "Bearer " + sign(hs256, `[1]`)},
		{name: "invalid payload JSON", header: "Bearer " + sign(hs256, `{`)},
		{name: "invalid header JSON", header: "Bearer " + sign(`{`, `{}`)},
		{name: "empty secret with empty-key token", secret: "", header: "Bearer " + paritySign(hs256, `{"a":1}`, "", sha256.New, false), wantAccept: true},
		{name: "empty secret with keyed token", secret: "", header: "Bearer " + valid},
		{name: "two segments", header: "Bearer " + strings.Join(strings.Split(valid, ".")[:2], ".")},
		{name: "four segments", header: "Bearer " + valid + ".x"},
		{name: "empty signature", header: "Bearer " + strings.Join(strings.Split(valid, ".")[:2], ".") + "."},
		{name: "bad base64 header", header: "Bearer !!!." + strings.SplitN(valid, ".", 2)[1]},
		{name: "padded segments", header: "Bearer " + paritySign(hs256, `{"company_id":"123"}`, secret, sha256.New, true), wantAccept: true},
		{name: "std base64 signature chars", header: "Bearer " + strings.NewReplacer("-", "+", "_", "/").Replace(valid)},
		{name: "bearer lowercase", header: "bearer " + valid},
		{name: "no bearer prefix", header: valid},
		{name: "bearer only", header: "Bearer "},
		{name: "bearer double space", header: "Bearer  " + valid, wantAccept: true},
		{name: "bearer trailing space", header: "Bearer " + valid + "  ", wantAccept: true},
		{name: "missing header", noHeader: true},
		{name: "expected payload match", expected: map[string]interface{}{"company_id": "123"}, header: "Bearer " + valid, wantAccept: true},
		{name: "expected payload mismatch", expected: map[string]interface{}{"company_id": "999"}, header: "Bearer " + valid},
		{name: "company_id route match", route: "/c/:company_id", path: "/c/123", header: "Bearer " + valid, wantAccept: true},
		{name: "company_id route mismatch", route: "/c/:company_id", path: "/c/999", header: "Bearer " + valid},
		{name: "company_id route numeric claim", route: "/c/:company_id", path: "/c/123", header: "Bearer " + sign(hs256, `{"company_id":123}`)},
	}

	// Generated cross-product to widen oracle coverage.
	headers := []string{hs256, `{"alg":"HS256"}`, `{"alg":"hs256"}`, `{"alg":"HS384"}`, `{"alg":""}`, `null`, `[]`}
	payloads := []string{`{}`, `null`, `{"exp":0}`, `{"exp":-1}`, `{"exp":` + nowStr + `}`, `{"exp":` + future + `,"nbf":` + past + `}`,
		`{"exp":"x"}`, `{"nbf":null}`, `{"nbf":` + future + `}`, `{"exp":1e20}`, `{"company_id":"123","iat":1}`, `"str"`, `123`}
	keys := []string{secret, "", "other"}
	for hi, h := range headers {
		for pi, p := range payloads {
			for ki, k := range keys {
				for _, padded := range []bool{false, true} {
					cases = append(cases, tc{
						name:       fmt.Sprintf("gen/h%d/p%d/k%d/pad=%v", hi, pi, ki, padded),
						secret:     secret,
						header:     "Bearer " + paritySign(h, p, k, sha256.New, padded),
						wantAccept: false, // checked against oracle only
					})
				}
			}
		}
	}

	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			opts := JWTOptions{Secret: secret, ExpectedPayload: c.expected}
			if strings.HasPrefix(c.name, "empty secret") {
				opts.Secret = c.secret
			}
			route, path := c.route, c.path
			if route == "" {
				route, path = "/test", "/test"
			}
			got := runJWTParity(t, AuthorizeJWTToken(opts), route, path, c.header, !c.noHeader)
			want := runJWTParity(t, upstreamAuthorizeJWTToken(opts, nowFn), route, path, c.header, !c.noHeader)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("diverges from upstream main:\n got=%#v\nwant=%#v", got, want)
			}
			if !strings.HasPrefix(c.name, "gen/") && got.handlerCalled != c.wantAccept {
				t.Fatalf("accepted=%v, want %v (status=%d body=%s)", got.handlerCalled, c.wantAccept, got.status, got.body)
			}
			if !got.handlerCalled {
				if got.status != http.StatusUnauthorized || got.body != `{"message":"Unauthorized"}` {
					t.Fatalf("reject response = %d %s", got.status, got.body)
				}
			}
		})
	}
}
