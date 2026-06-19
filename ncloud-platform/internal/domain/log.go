package domain

import "time"

// LogType categorizes the origin of a log entry.
type LogType string

const (
	LogTypeBuild   LogType = "build"
	LogTypeDeploy  LogType = "deploy"
	LogTypeHTTP    LogType = "http"
	LogTypeNetwork LogType = "network"
	LogTypeVolume  LogType = "volume"
	LogTypeSystem  LogType = "system"
	LogTypeMetrics LogType = "metrics"
)

// LogLevel represents severity.
type LogLevel string

const (
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
	LogLevelDebug LogLevel = "debug"
)

// LogEntry is the core logging structure written to the database and streamed via SSE.
type LogEntry struct {
	ID           int64     `json:"id"`
	DeploymentID string    `json:"deployment_id"`
	LogType      LogType   `json:"log_type"`
	Level        LogLevel  `json:"level"`
	Message      string    `json:"message"`
	Timestamp    time.Time `json:"timestamp"`
}
