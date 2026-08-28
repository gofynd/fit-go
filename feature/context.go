// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package feature

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// EvaluationContext provides request-scoped FeatureHub attributes. For
// client-evaluated keys, contexts share the immutable feature repository and
// evaluate independently. FeatureHub's server-evaluated protocol exposes one
// active context per edge stream, so Build replaces the parent client's active
// server context before waiting for its evaluated feature set.
type EvaluationContext struct {
	client     *Client
	mu         sync.RWMutex
	attributes map[string][]string
}

// NewContext returns an evaluation context initialized with the framework's
// service and platform-version attributes.
func (c *Client) NewContext() *EvaluationContext {
	if c == nil {
		return &EvaluationContext{}
	}
	c.mu.RLock()
	attributes := cloneAttributes(c.defaults)
	c.mu.RUnlock()
	return &EvaluationContext{client: c, attributes: attributes}
}

// UserKey sets the FeatureHub userkey attribute.
func (c *EvaluationContext) UserKey(value string) *EvaluationContext {
	return c.AttributeValues("userkey", []string{value})
}

// SessionKey sets the FeatureHub session attribute.
func (c *EvaluationContext) SessionKey(value string) *EvaluationContext {
	return c.AttributeValues("session", []string{value})
}

// Country sets the standard FeatureHub country attribute.
func (c *EvaluationContext) Country(value string) *EvaluationContext {
	return c.AttributeValues("country", []string{value})
}

// Device sets the standard FeatureHub device attribute.
func (c *EvaluationContext) Device(value string) *EvaluationContext {
	return c.AttributeValues("device", []string{value})
}

// Platform sets the standard FeatureHub platform attribute.
func (c *EvaluationContext) Platform(value string) *EvaluationContext {
	return c.AttributeValues("platform", []string{value})
}

// Version sets the standard FeatureHub semantic-version attribute.
func (c *EvaluationContext) Version(value string) *EvaluationContext {
	return c.AttributeValues("version", []string{value})
}

// Attribute sets one custom context value.
func (c *EvaluationContext) Attribute(key string, value interface{}) *EvaluationContext {
	return c.AttributeValues(key, []string{fmt.Sprint(value)})
}

// AttributeValues sets multiple custom context values. Passing an empty slice
// removes the attribute.
func (c *EvaluationContext) AttributeValues(key string, values []string) *EvaluationContext {
	if c == nil || strings.TrimSpace(key) == "" {
		return c
	}
	c.mu.Lock()
	if len(values) == 0 {
		delete(c.attributes, key)
	} else {
		c.attributes[key] = append([]string(nil), values...)
	}
	c.mu.Unlock()
	return c
}

// Clear removes request attributes and restores framework defaults.
func (c *EvaluationContext) Clear() *EvaluationContext {
	if c == nil || c.client == nil {
		return c
	}
	c.client.mu.RLock()
	defaults := cloneAttributes(c.client.defaults)
	c.client.mu.RUnlock()
	c.mu.Lock()
	c.attributes = defaults
	c.mu.Unlock()
	return c
}

// Build makes this context active. Client-evaluated contexts only wait for the
// shared repository. Server-evaluated contexts reconnect with the exact
// x-featurehub encoding used by the JavaScript SDK and wait for the refreshed
// full feature set.
func (c *EvaluationContext) Build(ctx context.Context) error {
	if c == nil || c.client == nil {
		return nil
	}
	c.mu.RLock()
	attributes := cloneAttributes(c.attributes)
	c.mu.RUnlock()
	if !c.client.clientEvaluated {
		c.client.replaceAttributes(attributes)
	}
	return c.client.WaitReady(ctx)
}

// IsEnabled evaluates a BOOLEAN feature in this context.
func (c *EvaluationContext) IsEnabled(key string) bool {
	value, valueType, ok := c.evaluatedValue(key)
	if !ok || valueType != featureTypeBoolean {
		return false
	}
	boolean, ok := castBoolean(value)
	return ok && boolean
}

// GetValue evaluates a feature and returns its typed value.
func (c *EvaluationContext) GetValue(key string) interface{} {
	value, valueType, ok := c.evaluatedValue(key)
	if !ok {
		return nil
	}
	return castFeatureValue(valueType, value, true)
}

// GetString returns a STRING feature value.
func (c *EvaluationContext) GetString(key string) (string, bool) {
	value, valueType, ok := c.evaluatedValue(key)
	if !ok || valueType != featureTypeString || value == nil {
		return "", false
	}
	return fmt.Sprint(value), true
}

// GetNumber returns a NUMBER feature value.
func (c *EvaluationContext) GetNumber(key string) (float64, bool) {
	value, valueType, ok := c.evaluatedValue(key)
	if !ok || valueType != featureTypeNumber {
		return 0, false
	}
	return castNumber(value)
}

// GetRawJSON returns a JSON feature value without decoding it.
func (c *EvaluationContext) GetRawJSON(key string) (string, bool) {
	value, valueType, ok := c.evaluatedValue(key)
	if !ok || valueType != featureTypeJSON || value == nil {
		return "", false
	}
	if raw, ok := value.(string); ok {
		return raw, true
	}
	encoded, err := json.Marshal(value)
	if err != nil {
		return "", false
	}
	return string(encoded), true
}

func (c *EvaluationContext) evaluatedValue(key string) (interface{}, string, bool) {
	if c == nil || c.client == nil {
		return nil, "", false
	}
	c.mu.RLock()
	attributes := cloneAttributes(c.attributes)
	c.mu.RUnlock()
	return c.client.evaluatedValueFor(key, attributes)
}

func featureHubContextHeader(attributes map[string][]string) string {
	entries := make([]string, 0, len(attributes))
	for key, values := range attributes {
		entries = append(entries, key+"="+encodeURIComponent(strings.Join(values, ",")))
	}
	sort.Strings(entries)
	return strings.Join(entries, ",")
}

// encodeURIComponent matches JavaScript's UTF-8 encodeURIComponent safe set.
func encodeURIComponent(value string) string {
	const hexadecimal = "0123456789ABCDEF"
	var encoded strings.Builder
	for _, current := range []byte(value) {
		if isURIComponentByte(current) {
			encoded.WriteByte(current)
			continue
		}
		encoded.WriteByte('%')
		encoded.WriteByte(hexadecimal[current>>4])
		encoded.WriteByte(hexadecimal[current&0x0f])
	}
	return encoded.String()
}

func isURIComponentByte(value byte) bool {
	return value >= 'a' && value <= 'z' ||
		value >= 'A' && value <= 'Z' ||
		value >= '0' && value <= '9' ||
		strings.ContainsRune("-_.!~*'()", rune(value))
}
