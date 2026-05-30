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

package server

import (
	"encoding/json"
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/gofynd/fit-go/errors"
)

// JSON writes a JSON response with the given status code (net/http version).
func JSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(status)
	if data != nil {
		_ = json.NewEncoder(w).Encode(data)
	}
}

// GinJSON writes a JSON response using gin's context.
func GinJSON(c *gin.Context, status int, data interface{}) {
	c.JSON(status, data)
}

// Success writes a 200 JSON response with the given data.
func Success(w http.ResponseWriter, data interface{}) {
	JSON(w, http.StatusOK, data)
}

// GinSuccess writes a 200 JSON response using gin's context.
func GinSuccess(c *gin.Context, data interface{}) {
	c.JSON(http.StatusOK, data)
}

// Error writes a JSON error response. If the error is a *errors.FitError, the
// HTTP status code and structured error body from the FitError are used.
// Otherwise a generic 500 Internal Server Error is returned.
func Error(w http.ResponseWriter, err error) {
	if err == nil {
		JSON(w, http.StatusInternalServerError, map[string]interface{}{
			"error": map[string]interface{}{
				"message": "unknown error",
			},
		})
		return
	}

	if fe, ok := err.(*errors.FitError); ok {
		status := fe.HTTPStatusCode
		if status < 200 || status > 599 {
			status = http.StatusInternalServerError
		}
		JSON(w, status, map[string]interface{}{
			"error": map[string]interface{}{
				"code":    fe.Code,
				"message": fe.GetMessage(),
				"meta":    fe.Meta,
			},
		})
		return
	}

	JSON(w, http.StatusInternalServerError, map[string]interface{}{
		"error": map[string]interface{}{
			"message": err.Error(),
		},
	})
}

// GinError writes a JSON error response using gin's context.
func GinError(c *gin.Context, err error) {
	if err == nil {
		c.JSON(http.StatusInternalServerError, map[string]interface{}{
			"error": map[string]interface{}{
				"message": "unknown error",
			},
		})
		return
	}

	if fe, ok := err.(*errors.FitError); ok {
		status := fe.HTTPStatusCode
		if status < 200 || status > 599 {
			status = http.StatusInternalServerError
		}
		c.JSON(status, map[string]interface{}{
			"error": map[string]interface{}{
				"code":    fe.Code,
				"message": fe.GetMessage(),
				"meta":    fe.Meta,
			},
		})
		return
	}

	c.JSON(http.StatusInternalServerError, map[string]interface{}{
		"error": map[string]interface{}{
			"message": err.Error(),
		},
	})
}
