package domain

import "time"

// BuildLog represents a single log line emitted during a deployment build.
type BuildLog struct {
	ID           string    `json:"id"`
	DeploymentID string    `json:"deployment_id"`
	Timestamp    time.Time `json:"timestamp"`
	Message      string    `json:"message"`
}

// NewBuildLog creates a new log entry.
func NewBuildLog(id, deploymentID, message string) *BuildLog {
	return &BuildLog{
		ID:           id,
		DeploymentID: deploymentID,
		Timestamp:    time.Now().UTC(),
		Message:      message,
	}
}
