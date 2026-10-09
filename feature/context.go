// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package feature

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
)

// EvaluationContext provides request-scoped FeatureHub attributes. For
// client-evaluated keys, contexts share the immutable feature repository and
// evaluate independently. Server-evaluated contexts obtain their own bounded
// feature snapshot during Build so concurrent requests never replace the
// parent client's active stream context.
type EvaluationContext struct {
	client       *Client
	mu           sync.RWMutex
	attributes   map[string][]string
	features     map[string]*featureState
	legacyValues map[string]interface{}
	built        bool
	revision     uint64
}

// NewContext returns an evaluation context initialized with the framework's
// service and platform-version attributes.
func (c *Client) NewContext() *EvaluationContext {
	if c == nil {
		return &EvaluationContext{attributes: make(map[string][]string)}
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
	if c.attributes == nil {
		c.attributes = make(map[string][]string)
	}
	if len(values) == 0 {
		delete(c.attributes, key)
	} else {
		c.attributes[key] = append([]string(nil), values...)
	}
	c.features = nil
	c.legacyValues = nil
	c.built = false
	c.revision++
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
	c.features = nil
	c.legacyValues = nil
	c.built = false
	c.revision++
	c.mu.Unlock()
	return c
}

// Build makes this context active. Client-evaluated contexts only wait for the
// shared repository. Server-evaluated contexts request an isolated feature
// snapshot with the exact x-featurehub encoding used by the JavaScript SDK.
func (c *EvaluationContext) Build(ctx context.Context) error {
	if c == nil || c.client == nil {
		return nil
	}
	c.mu.RLock()
	attributes := cloneAttributes(c.attributes)
	revision := c.revision
	c.mu.RUnlock()
	if c.client.legacy != nil {
		values, err := c.client.legacy.snapshotFor(ctx, attributes)
		if err != nil {
			return err
		}
		c.mu.Lock()
		if c.revision != revision {
			c.mu.Unlock()
			return errors.New("feature: evaluation context changed during Build")
		}
		c.legacyValues = values
		c.features = nil
		c.built = true
		c.mu.Unlock()
		return nil
	}
	if c.client.clientEvaluated {
		if err := c.client.WaitReady(ctx); err != nil {
			return err
		}
		c.mu.Lock()
		if c.revision != revision {
			c.mu.Unlock()
			return errors.New("feature: evaluation context changed during Build")
		}
		c.built = true
		c.mu.Unlock()
		return nil
	}
	features, err := c.client.evaluatedFeaturesForContext(ctx, attributes)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.revision != revision {
		c.mu.Unlock()
		return errors.New("feature: evaluation context changed during Build")
	}
	c.features = features
	c.built = true
	c.mu.Unlock()
	return nil
}

// IsEnabled evaluates a BOOLEAN feature in this context.
func (c *EvaluationContext) IsEnabled(key string) bool {
	value, valueType, ok := c.evaluatedValue(key)
	if !ok {
		return false
	}
	if c != nil && c.client != nil && c.client.legacy != nil {
		return legacyBoolean(value)
	}
	if valueType != featureTypeBoolean {
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
	feature, contextFeature := c.features[key]
	legacyValue, legacyFeature := c.legacyValues[key]
	built := c.built
	c.mu.RUnlock()
	if c.client.legacy != nil {
		if !built {
			return nil, "", false
		}
		return legacyValue, featureValueType(legacyValue), legacyFeature && legacyValue != nil
	}
	if !c.client.clientEvaluated {
		if !built {
			return nil, "", false
		}
		if !contextFeature || feature == nil {
			return nil, "", false
		}
		return feature.Value, feature.Type, feature.Value != nil
	}
	c.mu.RLock()
	attributes := cloneAttributes(c.attributes)
	c.mu.RUnlock()
	return c.client.evaluatedValueFor(key, attributes)
}

func legacyBoolean(value interface{}) bool {
	switch typed := value.(type) {
	case bool:
		return typed
	case string:
		return strings.EqualFold(strings.TrimSpace(typed), "true")
	case float64:
		return typed != 0
	default:
		return false
	}
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
