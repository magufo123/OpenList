package middlewares

import (
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
)

// LogFilterConfig holds configuration for log filtering
type LogFilterConfig struct {
	// EnableFiltering controls whether log filtering is enabled
	EnableFiltering bool
	
	// FilterHealthChecks controls whether to filter health check requests
	FilterHealthChecks bool
	
	// FilterWebDAV controls whether to filter WebDAV requests
	FilterWebDAV bool
	
	// FilterHEADRequests controls whether to filter HEAD requests
	FilterHEADRequests bool
	
	// CustomSkipPaths allows adding custom paths to skip
	CustomSkipPaths []string
	
	// CustomSkipMethods allows adding custom methods to skip
	CustomSkipMethods []string
	
	// CustomSkipPrefixes allows adding custom path prefixes to skip
	CustomSkipPrefixes []string
}

// DefaultLogFilterConfig returns the default configuration
func DefaultLogFilterConfig() LogFilterConfig {
	return LogFilterConfig{
		EnableFiltering:    true,
		FilterHealthChecks: true,
		FilterWebDAV:       true,
		FilterHEADRequests: true,
		CustomSkipPaths:    []string{},
		CustomSkipMethods:  []string{},
		CustomSkipPrefixes: []string{},
	}
}

// LoadLogFilterConfigFromEnv loads configuration from environment variables
func LoadLogFilterConfigFromEnv() LogFilterConfig {
	config := DefaultLogFilterConfig()
	
	// Check if filtering is enabled
	if env := os.Getenv("OPENLIST_LOG_FILTER_ENABLED"); env != "" {
		config.EnableFiltering = strings.ToLower(env) == "true"
	}
	
	// Check individual filter options
	if env := os.Getenv("OPENLIST_LOG_FILTER_HEALTH_CHECKS"); env != "" {
		config.FilterHealthChecks = strings.ToLower(env) == "true"
	}
	
	if env := os.Getenv("OPENLIST_LOG_FILTER_WEBDAV"); env != "" {
		config.FilterWebDAV = strings.ToLower(env) == "true"
	}
	
	if env := os.Getenv("OPENLIST_LOG_FILTER_HEAD_REQUESTS"); env != "" {
		config.FilterHEADRequests = strings.ToLower(env) == "true"
	}
	
	// Load custom skip paths
	if env := os.Getenv("OPENLIST_LOG_FILTER_SKIP_PATHS"); env != "" {
		config.CustomSkipPaths = strings.Split(env, ",")
		for i, path := range config.CustomSkipPaths {
			config.CustomSkipPaths[i] = strings.TrimSpace(path)
		}
	}
	
	// Load custom skip methods
	if env := os.Getenv("OPENLIST_LOG_FILTER_SKIP_METHODS"); env != "" {
		config.CustomSkipMethods = strings.Split(env, ",")
		for i, method := range config.CustomSkipMethods {
			config.CustomSkipMethods[i] = strings.TrimSpace(strings.ToUpper(method))
		}
	}
	
	// Load custom skip prefixes
	if env := os.Getenv("OPENLIST_LOG_FILTER_SKIP_PREFIXES"); env != "" {
		config.CustomSkipPrefixes = strings.Split(env, ",")
		for i, prefix := range config.CustomSkipPrefixes {
			config.CustomSkipPrefixes[i] = strings.TrimSpace(prefix)
		}
	}
	
	return config
}

// ToFilteredLoggerConfig converts LogFilterConfig to FilteredLoggerConfig
func (c LogFilterConfig) ToFilteredLoggerConfig() FilteredLoggerConfig {
	if !c.EnableFiltering {
		// Return empty config to disable filtering
		return FilteredLoggerConfig{
			Output: log.StandardLogger().Out,
		}
	}
	
	config := FilteredLoggerConfig{
		Output: log.StandardLogger().Out,
	}
	
	// Add health check paths
	if c.FilterHealthChecks {
		config.SkipPaths = append(config.SkipPaths, "/ping")
	}
	
	// Add HEAD method filtering
	if c.FilterHEADRequests {
		config.SkipMethods = append(config.SkipMethods, "HEAD")
	}
	
	// Add WebDAV filtering
	if c.FilterWebDAV {
		config.SkipPathPrefixes = append(config.SkipPathPrefixes, "/dav/")
		config.SkipMethods = append(config.SkipMethods, "PROPFIND")
	}
	
	// Add custom configurations
	config.SkipPaths = append(config.SkipPaths, c.CustomSkipPaths...)
	config.SkipMethods = append(config.SkipMethods, c.CustomSkipMethods...)
	config.SkipPathPrefixes = append(config.SkipPathPrefixes, c.CustomSkipPrefixes...)
	
	return config
}

// ConfigurableFilteredLogger returns a filtered logger with configuration loaded from environment
func ConfigurableFilteredLogger() gin.HandlerFunc {
	config := LoadLogFilterConfigFromEnv()
	loggerConfig := config.ToFilteredLoggerConfig()
	
	if !config.EnableFiltering {
		// Return standard Gin logger if filtering is disabled
		return gin.LoggerWithWriter(log.StandardLogger().Out)
	}
	
	return FilteredLoggerWithConfig(loggerConfig)
}