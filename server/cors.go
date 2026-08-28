package server

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

// CORSOptions configures callback-based, credentialed CORS.
type CORSOptions struct {
	// AllowOrigin decides whether to allow a given non-empty request Origin. It
	// receives the gin context for context-aware lookups and the
	// raw Origin header; return true to reflect it with credentials. Required — a nil
	// AllowOrigin allows nothing.
	AllowOrigin func(c *gin.Context, origin string) bool
	// AllowHeaders is the Access-Control-Allow-Headers value emitted on a preflight.
	AllowHeaders string
	// AllowMethods is the Access-Control-Allow-Methods value on a preflight.
	// Defaults to "GET,HEAD,PUT,PATCH,POST,DELETE".
	AllowMethods string
	// MaxAgeSeconds is the preflight cache TTL (Access-Control-Max-Age). Defaults to 86400.
	MaxAgeSeconds int
	// SkipHeader bypasses CORS when its request value is "true".
	SkipHeader string
	// PassThroughDisallowedPreflight lets an OPTIONS request with an absent or
	// disallowed Origin continue to the configured route/no-route handler. The
	// zero value preserves fit-go's existing behavior of terminating every
	// preflight with 204. Allowed preflights always terminate with 204.
	PassThroughDisallowedPreflight bool
}

// DynamicCORS reflects allowed origins and terminates allowed preflights with
// HTTP 204. Install it at engine level when no-route preflights must be handled.
func DynamicCORS(opts CORSOptions) gin.HandlerFunc {
	methods := opts.AllowMethods
	if methods == "" {
		methods = "GET,HEAD,PUT,PATCH,POST,DELETE"
	}
	maxAge := opts.MaxAgeSeconds
	if maxAge <= 0 {
		maxAge = 86400
	}
	maxAgeStr := strconv.Itoa(maxAge)
	return func(c *gin.Context) {
		if opts.SkipHeader != "" && strings.EqualFold(c.GetHeader(opts.SkipHeader), "true") {
			c.Next()
			return
		}
		origin := c.GetHeader("Origin")
		allowed := origin != "" && opts.AllowOrigin != nil && opts.AllowOrigin(c, origin)
		if allowed {
			c.Header("Access-Control-Allow-Origin", origin)
			c.Header("Access-Control-Allow-Credentials", "true")
			c.Header("Vary", "Origin")
		}
		if c.Request.Method == http.MethodOptions {
			if allowed {
				c.Header("Access-Control-Allow-Headers", opts.AllowHeaders)
				c.Header("Access-Control-Allow-Methods", methods)
				c.Header("Access-Control-Max-Age", maxAgeStr)
			} else if opts.PassThroughDisallowedPreflight {
				c.Next()
				return
			}
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}
