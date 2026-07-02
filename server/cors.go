package server

import (
	"net/http"
	"strconv"
	"strings"

	"github.com/gin-gonic/gin"
)

// CORSOptions configures the dynamic CORS middleware. It is deliberately
// callback-based: the CALLER owns the per-Origin allow decision (e.g. a dynamic
// allowlist of *.fynd.com / *.addsale.com plus registered application domains, as
// legacy Sentinel did), while fit-go owns the MECHANISM — reflecting the Origin with
// credentials, answering the preflight, and honoring a skip header. A nil
// Config.CORS disables the middleware entirely (matching a service whose
// config.enable_cors is off).
type CORSOptions struct {
	// AllowOrigin decides whether to allow a given non-empty request Origin. It
	// receives the gin context (for context-aware lookups, e.g. a Redis check) and the
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
	// SkipHeader, when non-empty and present on the request with value "true", bypasses
	// CORS for that request (legacy Sentinel's X-Skip-Cors).
	SkipHeader string
}

// CORS returns a gin middleware implementing dynamic, credentialed CORS. An allowed
// cross-origin response reflects the request Origin (credentials mode forbids "*") and
// sets Allow-Credentials + Vary:Origin; a preflight OPTIONS short-circuits 204 with the
// configured headers/methods/max-age. Installed engine-level (see Server.Init), it also
// runs on gin's no-route path, so a preflight to a GET/POST-only path is answered here
// rather than 404/405'd.
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
			}
			c.AbortWithStatus(http.StatusNoContent)
			return
		}
		c.Next()
	}
}
