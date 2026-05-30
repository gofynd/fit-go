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

package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/base64"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
)

// Default LRU cache settings.
const (
	defaultCacheMax     = 2000
	defaultCacheMaxSize = 100000
)

// Manager provides AES-256-GCM encryption and decryption with LRU caching.
// Go implementation of EncryptionManager (modules/encryption/index.ts).
//
// Usage:
//
//	mgr := encryption.NewManager()
//	if err := mgr.Init(); err != nil { ... }
//	encrypted, err := mgr.Encrypt("sensitive data")
//	decrypted, err := mgr.Decrypt(encrypted)
type Manager struct {
	mu              sync.RWMutex
	initialized     bool
	dek             []byte
	iv              []byte
	encryptionCache *lruCache
	decryptionCache *lruCache
}

// NewManager creates a new uninitialized Manager.
func NewManager() *Manager {
	return &Manager{}
}

// Init initializes the encryption manager. It reads the provider from
// ENCRYPTION_PROVIDER ("vault" or "gcp-kms", default "vault"), loads the DEK
// and IV either from environment variables (PII_DEK_BASE64, PII_IV_BASE64) or
// from the configured provider, and initializes the LRU caches.
func (m *Manager) Init() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if m.initialized {
		return nil
	}

	// Try inline DEK/IV from environment first.
	dekEnv := os.Getenv("PII_DEK_BASE64")
	ivEnv := os.Getenv("PII_IV_BASE64")

	if dekEnv != "" && ivEnv != "" {
		dek, err := base64.StdEncoding.DecodeString(dekEnv)
		if err != nil {
			return fmt.Errorf("encryption: failed to decode PII_DEK_BASE64: %w", err)
		}
		iv, err := base64.StdEncoding.DecodeString(ivEnv)
		if err != nil {
			return fmt.Errorf("encryption: failed to decode PII_IV_BASE64: %w", err)
		}
		m.dek = dek
		m.iv = iv
	} else {
		// Use provider.
		provider, err := m.createProvider()
		if err != nil {
			return err
		}
		dek, iv, err := provider.Init()
		if err != nil {
			return fmt.Errorf("encryption: provider init failed: %w", err)
		}
		m.dek = dek
		m.iv = iv
	}

	// Validate key sizes.
	if len(m.dek) != 32 {
		return fmt.Errorf("encryption: DEK must be 32 bytes (AES-256), got %d", len(m.dek))
	}
	if len(m.iv) != 12 {
		return fmt.Errorf("encryption: IV must be 12 bytes (GCM nonce), got %d", len(m.iv))
	}

	// Initialize LRU caches.
	cacheMax := envInt("ENCRYPTION_CACHE_MAX", defaultCacheMax)
	cacheMaxSize := envInt("ENCRYPTION_CACHE_MAX_SIZE", defaultCacheMaxSize)

	m.encryptionCache = newLRUCache(cacheMax, cacheMaxSize)
	m.decryptionCache = newLRUCache(cacheMax, cacheMaxSize)

	m.initialized = true
	return nil
}

// createProvider instantiates the appropriate encryption provider based on the
// ENCRYPTION_PROVIDER environment variable.
func (m *Manager) createProvider() (Provider, error) {
	mode := os.Getenv("ENCRYPTION_PROVIDER")
	if mode == "" {
		mode = "vault"
	}

	switch strings.ToLower(mode) {
	case "gcp-kms":
		return NewGCPProvider(), nil
	case "vault":
		return NewVaultProvider(), nil
	default:
		return nil, fmt.Errorf("encryption: unknown provider %q (expected 'vault' or 'gcp-kms')", mode)
	}
}

// Encrypt encrypts a plaintext message using AES-256-GCM and returns the
// result as "base64(ciphertext).base64(tag)". Results are cached in the
// encryption LRU cache.
func (m *Manager) Encrypt(message string) (string, error) {
	m.mu.RLock()
	if !m.initialized {
		m.mu.RUnlock()
		return "", fmt.Errorf("encryption: module is not initialized")
	}
	dek := m.dek
	iv := m.iv
	encCache := m.encryptionCache
	decCache := m.decryptionCache
	m.mu.RUnlock()

	// Check cache.
	if cached, ok := encCache.Get(message); ok {
		return cached, nil
	}

	block, err := aes.NewCipher(dek)
	if err != nil {
		return "", fmt.Errorf("encryption: cipher init failed: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("encryption: GCM init failed: %w", err)
	}

	// Seal appends the ciphertext + authentication tag.
	sealed := gcm.Seal(nil, iv, []byte(message), nil)

	// Split into ciphertext and tag. GCM tag is the last gcm.Overhead() bytes.
	tagSize := gcm.Overhead() // 16 bytes for AES-GCM
	ciphertextBytes := sealed[:len(sealed)-tagSize]
	tagBytes := sealed[len(sealed)-tagSize:]

	ciphertextB64 := base64.StdEncoding.EncodeToString(ciphertextBytes)
	tagB64 := base64.StdEncoding.EncodeToString(tagBytes)

	encrypted := ciphertextB64 + "." + tagB64

	// Populate both caches.
	encCache.Set(message, encrypted)
	decCache.Set(encrypted, message)

	return encrypted, nil
}

// Decrypt decrypts a string in the format "base64(ciphertext).base64(tag)"
// using AES-256-GCM and returns the plaintext. Results are cached in the
// decryption LRU cache.
func (m *Manager) Decrypt(encrypted string) (string, error) {
	m.mu.RLock()
	if !m.initialized {
		m.mu.RUnlock()
		return "", fmt.Errorf("encryption: module is not initialized")
	}
	dek := m.dek
	iv := m.iv
	encCache := m.encryptionCache
	decCache := m.decryptionCache
	m.mu.RUnlock()

	// Check cache.
	if cached, ok := decCache.Get(encrypted); ok {
		return cached, nil
	}

	parts := strings.SplitN(encrypted, ".", 2)
	if len(parts) != 2 {
		return "", fmt.Errorf("encryption: invalid encrypted format, expected 'ciphertext.tag'")
	}

	ciphertextBytes, err := base64.StdEncoding.DecodeString(parts[0])
	if err != nil {
		return "", fmt.Errorf("encryption: failed to decode ciphertext: %w", err)
	}

	tagBytes, err := base64.StdEncoding.DecodeString(parts[1])
	if err != nil {
		return "", fmt.Errorf("encryption: failed to decode tag: %w", err)
	}

	block, err := aes.NewCipher(dek)
	if err != nil {
		return "", fmt.Errorf("encryption: cipher init failed: %w", err)
	}

	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return "", fmt.Errorf("encryption: GCM init failed: %w", err)
	}

	// Reassemble sealed data: ciphertext + tag.
	sealed := make([]byte, len(ciphertextBytes)+len(tagBytes))
	copy(sealed, ciphertextBytes)
	copy(sealed[len(ciphertextBytes):], tagBytes)

	plaintext, err := gcm.Open(nil, iv, sealed, nil)
	if err != nil {
		return "", fmt.Errorf("encryption: decryption failed: %w", err)
	}

	decrypted := string(plaintext)

	// Populate both caches.
	decCache.Set(encrypted, decrypted)
	encCache.Set(decrypted, encrypted)

	return decrypted, nil
}

// IsInitialized reports whether the encryption manager has been successfully
// initialized.
func (m *Manager) IsInitialized() bool {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.initialized
}

// ---------------------------------------------------------------------------
// LRU Cache - simple thread-safe LRU cache using a doubly-linked list + map.
// Port of the LRU cache behavior (lru-cache npm package).
// ---------------------------------------------------------------------------

type lruCache struct {
	mu      sync.Mutex
	max     int // max number of entries
	maxSize int // max total size in bytes
	size    int // current total size
	items   map[string]*lruNode
	head    *lruNode // most recently used
	tail    *lruNode // least recently used
}

type lruNode struct {
	key   string
	value string
	size  int
	prev  *lruNode
	next  *lruNode
}

func newLRUCache(max, maxSize int) *lruCache {
	return &lruCache{
		max:     max,
		maxSize: maxSize,
		items:   make(map[string]*lruNode),
	}
}

// Get retrieves a value from the cache and promotes it to the head (MRU).
func (c *lruCache) Get(key string) (string, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	node, ok := c.items[key]
	if !ok {
		return "", false
	}
	c.moveToHead(node)
	return node.value, true
}

// Set adds or updates a value in the cache, evicting LRU entries as needed.
func (c *lruCache) Set(key, value string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	entrySize := len(key) + len(value)

	if node, ok := c.items[key]; ok {
		// Update existing entry.
		c.size -= node.size
		node.value = value
		node.size = entrySize
		c.size += entrySize
		c.moveToHead(node)
	} else {
		// Add new entry.
		node := &lruNode{
			key:   key,
			value: value,
			size:  entrySize,
		}
		c.items[key] = node
		c.addToHead(node)
		c.size += entrySize
	}

	// Evict until within limits.
	for (len(c.items) > c.max || c.size > c.maxSize) && c.tail != nil {
		c.evictTail()
	}
}

func (c *lruCache) addToHead(node *lruNode) {
	node.prev = nil
	node.next = c.head
	if c.head != nil {
		c.head.prev = node
	}
	c.head = node
	if c.tail == nil {
		c.tail = node
	}
}

func (c *lruCache) moveToHead(node *lruNode) {
	if node == c.head {
		return
	}
	c.removeNode(node)
	c.addToHead(node)
}

func (c *lruCache) removeNode(node *lruNode) {
	if node.prev != nil {
		node.prev.next = node.next
	} else {
		c.head = node.next
	}
	if node.next != nil {
		node.next.prev = node.prev
	} else {
		c.tail = node.prev
	}
}

func (c *lruCache) evictTail() {
	if c.tail == nil {
		return
	}
	node := c.tail
	c.removeNode(node)
	c.size -= node.size
	delete(c.items, node.key)
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// envInt reads an integer from the environment, returning defaultVal on missing
// or unparseable values.
func envInt(key string, defaultVal int) int {
	v := os.Getenv(key)
	if v == "" {
		return defaultVal
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return defaultVal
	}
	return n
}
