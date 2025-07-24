package middlewares

import (
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// FilteredLoggerConfig defines the configuration for the filtered logger
type FilteredLoggerConfig struct {
	// SkipPaths is a list of URL paths to skip logging
	SkipPaths []string
	// SkipMethods is a list of HTTP methods to skip logging
	SkipMethods []string
	// SkipPathPrefixes is a list of URL path prefixes to skip logging
	SkipPathPrefixes []string
	// Output is the writer where logs will be written
	Output io.Writer
}

// FilteredLoggerWithConfig returns a gin.HandlerFunc (middleware) that logs requests
// but skips logging for specified paths, methods, or path prefixes
func FilteredLoggerWithConfig(config FilteredLoggerConfig) gin.HandlerFunc {
	if config.Output == nil {
		config.Output = log.StandardLogger().Out
	}

	return gin.LoggerWithConfig(gin.LoggerConfig{
		Output: config.Output,
		SkipPaths: config.SkipPaths,
		Formatter: func(param gin.LogFormatterParams) string {
			// Skip logging for health check endpoints
			if shouldSkipLogging(param.Path, param.Method, config) {
				return ""
			}

			// Use a custom log format similar to Gin's default
			return defaultLogFormatter(param)
		},
	})
}

// FilteredLogger returns a gin.HandlerFunc (middleware) that logs requests
// but filters out health check and PROPFIND requests
func FilteredLogger() gin.HandlerFunc {
	config := FilteredLoggerConfig{
		SkipPaths: []string{
			"/ping",
		},
		SkipMethods: []string{
			"HEAD", // Skip HEAD requests for health checks
		},
		SkipPathPrefixes: []string{
			"/dav/", // Skip WebDAV PROPFIND requests
		},
		Output: log.StandardLogger().Out,
	}

	return FilteredLoggerWithConfig(config)
}

// shouldSkipLogging determines if a request should be skipped from logging
func shouldSkipLogging(path, method string, config FilteredLoggerConfig) bool {
	// Check if path should be skipped
	for _, skipPath := range config.SkipPaths {
		if path == skipPath {
			return true
		}
	}

	// Check if method should be skipped
	for _, skipMethod := range config.SkipMethods {
		if method == skipMethod {
			return true
		}
	}

	// Check if path prefix should be skipped
	for _, skipPrefix := range config.SkipPathPrefixes {
		if strings.HasPrefix(path, skipPrefix) {
			return true
		}
	}

	// Special case: Skip PROPFIND requests (common in WebDAV)
	if method == "PROPFIND" {
		return true
	}

	return false
}

// defaultLogFormatter provides a default log format similar to Gin's built-in formatter
func defaultLogFormatter(param gin.LogFormatterParams) string {
	var statusColor, methodColor, resetColor string
	if param.IsOutputColor() {
		statusColor = param.StatusCodeColor()
		methodColor = param.MethodColor()
		resetColor = param.ResetColor()
	}

	if param.Latency > time.Minute {
		param.Latency = param.Latency.Truncate(time.Second)
	}

	return fmt.Sprintf("[GIN] %v |%s %3d %s| %13v | %15s |%s %-7s %s %#v\n%s",
		param.TimeStamp.Format("2006/01/02 - 15:04:05"),
		statusColor, param.StatusCode, resetColor,
		param.Latency,
		param.ClientIP,
		methodColor, param.Method, resetColor,
		param.Path,
		param.ErrorMessage,
	)
}