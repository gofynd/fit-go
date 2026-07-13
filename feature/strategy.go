// Copyright 2026 Fynd (Shopsense Retail Technologies Limited)
// Licensed under the Apache License, Version 2.0.

package feature

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"math"
	"math/bits"
	"net"
	"regexp"
	"strconv"
	"strings"
	"time"
)

const (
	featureTypeBoolean = "BOOLEAN"
	featureTypeString  = "STRING"
	featureTypeNumber  = "NUMBER"
	featureTypeJSON    = "JSON"
	maxPercentage      = 1_000_000
)

var (
	javascriptIntegerPrefix = regexp.MustCompile(`^[+-]?\d+`)
	javascriptFloatPrefix   = regexp.MustCompile(`^[+-]?(?:(?:\d+\.?\d*)|(?:\.\d+))(?:[eE][+-]?\d+)?`)
)

type featureState struct {
	ID         string            `json:"id"`
	Key        string            `json:"key"`
	Locked     bool              `json:"l,omitempty"`
	Version    *int64            `json:"version,omitempty"`
	Type       string            `json:"type,omitempty"`
	Value      interface{}       `json:"value"`
	Strategies []rolloutStrategy `json:"strategies,omitempty"`
}

type rolloutStrategy struct {
	ID                   string              `json:"id"`
	Percentage           float64             `json:"percentage,omitempty"`
	PercentageAttributes []string            `json:"percentageAttributes,omitempty"`
	Attributes           []strategyAttribute `json:"attributes,omitempty"`
	Value                interface{}         `json:"value"`
}

type strategyAttribute struct {
	Conditional string        `json:"conditional"`
	FieldName   string        `json:"fieldName"`
	Values      []interface{} `json:"values"`
	Type        string        `json:"type"`
}

func featureVersion(feature *featureState) int64 {
	if feature == nil || feature.Version == nil {
		return -1
	}
	return *feature.Version
}

// applyStrategies follows featurehub-javascript-core-sdk's ApplyFeature
// algorithm, including feature-ID salting and cumulative unqualified buckets.
func applyStrategies(feature *featureState, context map[string][]string, now time.Time) (interface{}, bool) {
	if feature == nil || len(feature.Strategies) == 0 {
		return nil, false
	}
	defaultPercentageKey := firstAttribute(context, "session", firstAttribute(context, "userkey", ""))
	basePercentages := make(map[string]float64)
	var percentage float64
	var percentageKey string
	percentageCalculated := false

	for _, strategy := range feature.Strategies {
		if strategy.Percentage != 0 && (defaultPercentageKey != "" || len(strategy.PercentageAttributes) > 0) {
			newPercentageKey := defaultPercentageKey
			if len(strategy.PercentageAttributes) > 0 {
				parts := make([]string, 0, len(strategy.PercentageAttributes))
				for _, attribute := range strategy.PercentageAttributes {
					parts = append(parts, firstAttribute(context, attribute, "<none>"))
				}
				newPercentageKey = strings.Join(parts, "$")
			}
			if !percentageCalculated || newPercentageKey != percentageKey {
				percentageKey = newPercentageKey
				percentage = determineClientPercentage(percentageKey, feature.ID)
				percentageCalculated = true
			}
			base := basePercentages[percentageKey]
			if len(strategy.Attributes) > 0 {
				base = 0
			}
			if percentage <= base+strategy.Percentage && (len(strategy.Attributes) == 0 || matchAttributes(context, strategy.Attributes, now)) {
				return strategy.Value, true
			}
			if len(strategy.Attributes) == 0 {
				basePercentages[percentageKey] += strategy.Percentage
			}
		}
		if strategy.Percentage == 0 && len(strategy.Attributes) > 0 && matchAttributes(context, strategy.Attributes, now) {
			return strategy.Value, true
		}
	}
	return nil, false
}

func determineClientPercentage(percentageKey, featureID string) float64 {
	hash := murmurHash3([]byte(percentageKey+featureID), 0)
	return math.Floor(float64(hash) / math.Pow(2, 32) * maxPercentage)
}

// murmurHash3 implements the x86 32-bit MurmurHash3 variant used by the
// FeatureHub JavaScript SDK without unsafe pointer arithmetic.
func murmurHash3(data []byte, seed uint32) uint32 {
	const (
		c1 uint32 = 0xcc9e2d51
		c2 uint32 = 0x1b873593
	)

	hash := seed
	blockCount := len(data) / 4
	for block := 0; block < blockCount; block++ {
		value := binary.LittleEndian.Uint32(data[block*4:])
		value *= c1
		value = bits.RotateLeft32(value, 15)
		value *= c2

		hash ^= value
		hash = bits.RotateLeft32(hash, 13)
		hash = hash*5 + 0xe6546b64
	}

	tail := data[blockCount*4:]
	var value uint32
	switch len(tail) {
	case 3:
		value ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		value ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		value ^= uint32(tail[0])
		value *= c1
		value = bits.RotateLeft32(value, 15)
		value *= c2
		hash ^= value
	}

	hash ^= uint32(len(data))
	hash ^= hash >> 16
	hash *= 0x85ebca6b
	hash ^= hash >> 13
	hash *= 0xc2b2ae35
	hash ^= hash >> 16
	return hash
}

func matchAttributes(context map[string][]string, attributes []strategyAttribute, now time.Time) bool {
	for _, attribute := range attributes {
		supplied := context[attribute.FieldName]
		if len(supplied) == 0 && strings.EqualFold(attribute.FieldName, "now") {
			switch attribute.Type {
			case "DATE":
				supplied = []string{now.UTC().Format("2006-01-02")}
			case "DATETIME":
				supplied = []string{now.UTC().Format(time.RFC3339Nano)}
			}
		}
		if attribute.Values == nil && len(supplied) == 0 {
			if attribute.Conditional != "EQUALS" {
				return false
			}
			continue
		}
		if attribute.Values == nil || len(supplied) == 0 {
			return false
		}
		matched := false
		for _, value := range supplied {
			if matchAttributeValue(value, attribute) {
				matched = true
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
}

func matchAttributeValue(value string, attribute strategyAttribute) bool {
	switch attribute.Type {
	case "BOOLEAN":
		if len(attribute.Values) == 0 {
			return false
		}
		expected := fmt.Sprint(attribute.Values[0]) == "true"
		actual := value == "true"
		if attribute.Conditional == "EQUALS" {
			return actual == expected
		}
		if attribute.Conditional == "NOT_EQUALS" {
			return actual != expected
		}
		return false
	case "NUMBER":
		return matchNumber(value, attribute)
	case "DATE":
		parsed, ok := parseFeatureDate(value)
		if !ok {
			return false
		}
		return matchString(parsed.Format("2006-01-02"), attribute)
	case "DATETIME":
		parsed, ok := parseFeatureDate(value)
		if !ok {
			return false
		}
		return matchString(parsed.UTC().Format("2006-01-02T15:04:05Z"), attribute)
	case "SEMANTIC_VERSION":
		return matchSemanticVersion(value, attribute)
	case "IP_ADDRESS":
		return matchIPAddress(value, attribute)
	case "STRING":
		return matchString(value, attribute)
	default:
		return false
	}
}

func matchString(value string, attribute strategyAttribute) bool {
	values := attributeStrings(attribute.Values)
	for _, candidate := range values {
		switch attribute.Conditional {
		case "EQUALS":
			if value == candidate {
				return true
			}
		case "ENDS_WITH":
			if strings.HasSuffix(value, candidate) {
				return true
			}
		case "STARTS_WITH":
			if strings.HasPrefix(value, candidate) {
				return true
			}
		case "GREATER":
			if value > candidate {
				return true
			}
		case "GREATER_EQUALS":
			if value >= candidate {
				return true
			}
		case "LESS":
			if value < candidate {
				return true
			}
		case "LESS_EQUALS":
			if value <= candidate {
				return true
			}
		case "INCLUDES":
			if strings.Contains(value, candidate) {
				return true
			}
		case "REGEX":
			if expression, err := regexp.Compile(candidate); err == nil && expression.MatchString(value) {
				return true
			}
		}
	}
	if attribute.Conditional == "NOT_EQUALS" {
		for _, candidate := range values {
			if value == candidate {
				return false
			}
		}
		return true
	}
	if attribute.Conditional == "EXCLUDES" {
		for _, candidate := range values {
			if strings.Contains(value, candidate) {
				return false
			}
		}
		return true
	}
	return false
}

func matchNumber(value string, attribute strategyAttribute) bool {
	floatMode := strings.Contains(value, ".")
	actual, ok := parseJavaScriptNumber(value, floatMode)
	if !ok {
		return false
	}
	matchedEqual := false
	matchedContains := false
	for _, raw := range attribute.Values {
		candidateText := fmt.Sprint(raw)
		candidate, ok := parseJavaScriptNumber(candidateText, floatMode)
		if !ok {
			continue
		}
		matchedEqual = matchedEqual || actual == candidate
		matchedContains = matchedContains || strings.Contains(value, candidateText)
		switch attribute.Conditional {
		case "EQUALS":
			if actual == candidate {
				return true
			}
		case "GREATER":
			if actual > candidate {
				return true
			}
		case "GREATER_EQUALS":
			if actual >= candidate {
				return true
			}
		case "LESS":
			if actual < candidate {
				return true
			}
		case "LESS_EQUALS":
			if actual <= candidate {
				return true
			}
		case "STARTS_WITH":
			if strings.HasPrefix(value, candidateText) {
				return true
			}
		case "ENDS_WITH":
			if strings.HasSuffix(value, candidateText) {
				return true
			}
		case "INCLUDES":
			if strings.Contains(value, candidateText) {
				return true
			}
		case "REGEX":
			if expression, err := regexp.Compile(candidateText); err == nil && expression.MatchString(value) {
				return true
			}
		}
	}
	if attribute.Conditional == "NOT_EQUALS" {
		return !matchedEqual
	}
	if attribute.Conditional == "EXCLUDES" {
		return !matchedContains
	}
	return false
}

// parseJavaScriptNumber preserves the installed FeatureHub JavaScript SDK's
// parseInt/parseFloat split. The SDK selects parseFloat only when the supplied
// context value contains a decimal point and both parsers accept numeric
// prefixes rather than requiring the entire string to be numeric.
func parseJavaScriptNumber(value string, floatMode bool) (float64, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0, false
	}
	matcher := javascriptIntegerPrefix
	if floatMode {
		matcher = javascriptFloatPrefix
	}
	prefix := matcher.FindString(trimmed)
	if prefix == "" {
		return 0, false
	}
	parsed, err := strconv.ParseFloat(prefix, 64)
	return parsed, err == nil
}

func matchSemanticVersion(value string, attribute strategyAttribute) bool {
	for _, candidate := range attributeStrings(attribute.Values) {
		comparison := compareSemanticVersion(value, candidate)
		switch attribute.Conditional {
		case "EQUALS", "INCLUDES":
			if comparison == 0 {
				return true
			}
		case "GREATER":
			if comparison > 0 {
				return true
			}
		case "GREATER_EQUALS":
			if comparison >= 0 {
				return true
			}
		case "LESS":
			if comparison < 0 {
				return true
			}
		case "LESS_EQUALS":
			if comparison <= 0 {
				return true
			}
		case "NOT_EQUALS", "EXCLUDES":
			if comparison != 0 {
				return true
			}
		}
	}
	return false
}

func compareSemanticVersion(left, right string) int {
	leftParts := strings.Split(left, ".")
	rightParts := strings.Split(right, ".")
	for index := 0; index < 3; index++ {
		leftNumber, leftNumeric := semanticPart(leftParts, index)
		rightNumber, rightNumeric := semanticPart(rightParts, index)
		switch {
		case leftNumeric && rightNumeric && leftNumber > rightNumber:
			return 1
		case leftNumeric && rightNumeric && leftNumber < rightNumber:
			return -1
		case leftNumeric && !rightNumeric:
			return 1
		case !leftNumeric && rightNumeric:
			return -1
		}
	}
	return 0
}

func semanticPart(parts []string, index int) (float64, bool) {
	if index >= len(parts) {
		return 0, false
	}
	value, err := strconv.ParseFloat(parts[index], 64)
	return value, err == nil
}

func matchIPAddress(value string, attribute strategyAttribute) bool {
	ip := net.ParseIP(value)
	if ip == nil {
		return false
	}
	contained := false
	for _, candidate := range attributeStrings(attribute.Values) {
		if !strings.Contains(candidate, "/") {
			if parsed := net.ParseIP(candidate); parsed != nil && parsed.Equal(ip) {
				contained = true
				break
			}
			continue
		}
		_, network, err := net.ParseCIDR(candidate)
		if err == nil && network.Contains(ip) {
			contained = true
			break
		}
	}
	switch attribute.Conditional {
	case "EQUALS", "INCLUDES":
		return contained
	case "NOT_EQUALS", "EXCLUDES":
		return !contained
	default:
		return false
	}
}

func parseFeatureDate(value string) (time.Time, bool) {
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}

func firstAttribute(context map[string][]string, key, fallback string) string {
	if values := context[key]; len(values) > 0 {
		return values[0]
	}
	return fallback
}

func attributeStrings(values []interface{}) []string {
	result := make([]string, 0, len(values))
	for _, value := range values {
		if value != nil {
			result = append(result, fmt.Sprint(value))
		}
	}
	return result
}

func castBoolean(value interface{}) (bool, bool) {
	switch typed := value.(type) {
	case bool:
		return typed, true
	case string:
		return typed == "true", true
	default:
		return fmt.Sprint(value) == "true", value != nil
	}
}

func castNumber(value interface{}) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, true
	case float32:
		return float64(typed), true
	case int:
		return float64(typed), true
	case int64:
		return float64(typed), true
	case json.Number:
		parsed, err := typed.Float64()
		return parsed, err == nil
	default:
		parsed, err := strconv.ParseFloat(fmt.Sprint(value), 64)
		return parsed, err == nil
	}
}

func castFeatureValue(valueType string, value interface{}, parseJSON bool) interface{} {
	if value == nil {
		return nil
	}
	switch valueType {
	case featureTypeBoolean:
		boolean, _ := castBoolean(value)
		return boolean
	case featureTypeString:
		return fmt.Sprint(value)
	case featureTypeNumber:
		number, ok := castNumber(value)
		if !ok {
			return nil
		}
		return number
	case featureTypeJSON:
		if !parseJSON {
			return fmt.Sprint(value)
		}
		if raw, ok := value.(string); ok {
			var decoded interface{}
			if json.Unmarshal([]byte(raw), &decoded) == nil {
				return decoded
			}
			return map[string]interface{}{}
		}
		return value
	default:
		return nil
	}
}
