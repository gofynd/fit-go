// Copyright 2026 Fynd (Shopsense Retail Technologies Pvt. Ltd.)
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

// Package utils provides HTTP client, string sanitization, and general utility
// functions for the fit.go framework.
package utils

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// RequestInterceptor is a function that can modify an HTTP request before it is
// sent. Returning an error cancels the request.
type RequestInterceptor func(req *http.Request) error

// ResponseInterceptor is a function that can inspect or modify an HTTP response
// after it is received. Returning an error causes Do to return that error.
type ResponseInterceptor func(resp *http.Response) error

// HTTPClientOptions configures an HTTPClient instance.
type HTTPClientOptions struct {
	// Timeout for individual requests. Default: 30s.
	Timeout time.Duration

	// BaseURL is prepended to relative request URLs.
	BaseURL string

	// Headers are default headers applied to every request.
	Headers map[string]string

	// Logger receives structured log entries. If nil, logging is disabled.
	Logger HTTPLogger

	// MetricsRecorder receives request duration metrics. If nil, metrics are
	// disabled.
	MetricsRecorder HTTPMetricsRecorder

	// ProxyURL overrides the HTTPS_PROXY environment variable.
	ProxyURL string

	// RequestInterceptors are applied to every request before it is sent.
	RequestInterceptors []RequestInterceptor

	// ResponseInterceptors are applied to every response after it is received.
	ResponseInterceptors []ResponseInterceptor
}

// HTTPLogger is the logging interface used by HTTPClient. Implementations
// should be thread-safe.
type HTTPLogger interface {
	Debug(msg string, kvs ...interface{})
	Info(msg string, kvs ...interface{})
	Error(msg string, kvs ...interface{})
}

// HTTPMetricsRecorder receives HTTP client metrics.
type HTTPMetricsRecorder interface {
	RecordHTTPClient(method, host, statusCode string, duration time.Duration)
}

// HTTPResponse wraps the standard http.Response with additional metadata
// collected during the request lifecycle.
type HTTPResponse struct {
	*http.Response

	// Duration is the wall-clock time spent executing the request.
	Duration time.Duration
}

// HTTPClient wraps net/http.Client with request/response logging, metrics
// collection, proxy support, and interceptors. Port
// modules/axios/index.ts.
//
// Usage:
//
//	client := utils.NewHTTPClient(utils.HTTPClientOptions{
//	 Timeout: 10 * time.Second,
//	 Logger: myLogger,
//	})
//	req, _ := http.NewRequest("GET", "https://api.example.com/data", nil)
//	resp, err := client.Do(req)
type HTTPClient struct {
	mu sync.RWMutex
	client *http.Client
	baseURL string
	defaultHeaders map[string]string
	logger HTTPLogger
	metricsRecorder HTTPMetricsRecorder
	proxyURL string
	forceProxyList []string
	noProxyList []string
	requestInterceptors []RequestInterceptor
	responseInterceptors []ResponseInterceptor
}

// Loggable content types that are safe to include in debug logs.
var whitelistedContentTypes = []string{
	"application/json",
	"text/plain",
	"text/html",
	"application/xml",
	"text/xml",
	"application/csv",
	"text/csv",
}

// NewHTTPClient creates a new HTTPClient with the given options.
func NewHTTPClient(opts HTTPClientOptions) *HTTPClient {
	if opts.Timeout == 0 {
		opts.Timeout = 30 * time.Second
	}

	proxyURL := opts.ProxyURL
	if proxyURL == "" {
		proxyURL = os.Getenv("HTTPS_PROXY")
	}

	var forceProxy []string
	if domains := os.Getenv("FORCE_PROXY_DOMAINS"); domains != "" {
		for _, d := range strings.Split(domains, ",") {
			d = strings.TrimSpace(d)
			if d != "" {
				forceProxy = append(forceProxy, d)
			}
		}
	}

	var noProxy []string
	if domains := os.Getenv("NO_PROXY"); domains != "" {
		for _, d := range strings.Split(domains, ",") {
			d = strings.TrimSpace(d)
			if d != "" {
				noProxy = append(noProxy, d)
			}
		}
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()

	// Configure proxy if set.
	if proxyURL != "" {
		proxyParsed, err := url.Parse(proxyURL)
		if err == nil {
			transport.Proxy = func(req *http.Request) (*url.URL, error) {
				hostname := req.URL.Hostname()

				// Check NO_PROXY list.
				for _, np := range noProxy {
					if strings.EqualFold(hostname, np) || strings.HasSuffix(hostname, "."+np) {
						return nil, nil // no proxy for this host
					}
				}

				// Force proxy for specific domains (ignores NO_PROXY).
				for _, fp := range forceProxy {
					if strings.EqualFold(hostname, fp) {
						return proxyParsed, nil
					}
				}

				// Standard proxy for HTTPS only.
				if req.URL.Scheme == "https" {
					return proxyParsed, nil
				}
				return nil, nil
			}
		}
	}

	return &HTTPClient{
		client: &http.Client{
			Timeout: opts.Timeout,
			Transport: transport,
		},
		baseURL: strings.TrimRight(opts.BaseURL, "/"),
		defaultHeaders: opts.Headers,
		logger: opts.Logger,
		metricsRecorder: opts.MetricsRecorder,
		proxyURL: proxyURL,
		forceProxyList: forceProxy,
		noProxyList: noProxy,
		requestInterceptors: opts.RequestInterceptors,
		responseInterceptors: opts.ResponseInterceptors,
	}
}

// Do executes an HTTP request with logging and metrics interception. It applies
// default headers, logs the request/response, records duration metrics, and
// returns the response.
//
// Port of the axios request/response interceptors.
func (c *HTTPClient) Do(req *http.Request) (*http.Response, error) {
	// Apply base URL if the request URL is relative.
	if c.baseURL != "" && !req.URL.IsAbs() {
		parsed, err := url.Parse(c.baseURL + req.URL.String())
		if err == nil {
			req.URL = parsed
		}
	}

	// Apply default headers.
	c.mu.RLock()
	for k, v := range c.defaultHeaders {
		if req.Header.Get(k) == "" {
			req.Header.Set(k, v)
		}
	}
	c.mu.RUnlock()

	// Run request interceptors.
	for _, interceptor := range c.requestInterceptors {
		if err := interceptor(req); err != nil {
			return nil, fmt.Errorf("request interceptor: %w", err)
		}
	}

	// Generate request ID for log correlation.
	requestID := generateRequestID()
	action := strings.ToUpper(req.Method)
	externalURL := req.URL.String()

	// Log request.
	if c.logger != nil {
		c.logger.Debug(fmt.Sprintf("[EXT] Request %s to %s with Request ID: %s", action, externalURL, requestID))
	}

	startTime := time.Now()

	// Execute request.
	resp, err := c.client.Do(req)

	duration := time.Since(startTime)
	host := extractHost(externalURL)

	if err != nil {
		// Record error metrics.
		if c.metricsRecorder != nil {
			c.metricsRecorder.RecordHTTPClient(action, host, "0", duration)
		}
		if c.logger != nil {
			c.logger.Error(fmt.Sprintf("[EXT] Failed %s to %s with %s", action, externalURL, requestID),
				"request_url", externalURL,
				"request_method", action,
				"error", err.Error(),
			)
		}
		return nil, err
	}

	statusCode := fmt.Sprintf("%d", resp.StatusCode)

	// Record success metrics.
	if c.metricsRecorder != nil {
		c.metricsRecorder.RecordHTTPClient(action, host, statusCode, duration)
	}

	// Log response.
	if c.logger != nil {
		c.logger.Info(fmt.Sprintf("[EXT] Successful %s to %s with Request ID: %s", action, externalURL, requestID),
			"request_url", externalURL,
			"request_method", action,
			"response_status", resp.StatusCode,
		)

		// Log response body for whitelisted content types at debug level.
		ct := resp.Header.Get("Content-Type")
		for _, allowed := range whitelistedContentTypes {
			if strings.Contains(ct, allowed) {
				c.logger.Debug(fmt.Sprintf("[EXT] Response Data of %s to %s with Request ID: %s", action, externalURL, requestID),
					"content_type", ct,
				)
				break
			}
		}
	}

	// Run response interceptors.
	for _, interceptor := range c.responseInterceptors {
		if err := interceptor(resp); err != nil {
			return resp, fmt.Errorf("response interceptor: %w", err)
		}
	}

	return resp, nil
}

// Get performs an HTTP GET request to the given URL.
func (c *HTTPClient) Get(url string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

// Post performs an HTTP POST request to the given URL with the provided body.
func (c *HTTPClient) Post(url string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPost, url, body)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

// Put performs an HTTP PUT request to the given URL with the provided body.
func (c *HTTPClient) Put(url string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPut, url, body)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

// Patch performs an HTTP PATCH request to the given URL with the provided body.
func (c *HTTPClient) Patch(url string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodPatch, url, body)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

// Delete performs an HTTP DELETE request to the given URL.
func (c *HTTPClient) Delete(url string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		return nil, err
	}
	return c.Do(req)
}

// SetDefaultHeader sets a default header that will be applied to all requests.
func (c *HTTPClient) SetDefaultHeader(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.defaultHeaders == nil {
		c.defaultHeaders = make(map[string]string)
	}
	c.defaultHeaders[key] = value
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// extractHost extracts the hostname from a URL string for metrics labels.
func extractHost(rawURL string) string {
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "unknown"
	}
	host := parsed.Hostname()
	if host == "" {
		return "unknown"
	}
	return host
}

// generateRequestID creates a short unique request ID for log correlation.
func generateRequestID() string {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		// Fallback: use timestamp-based ID.
		return fmt.Sprintf("req-%d", time.Now().UnixNano())
	}
	return hex.EncodeToString(b)
}
