package domain

// EnvironmentVariable represents an env var injected into the project build/run.
type EnvironmentVariable struct {
	ID        string `json:"id"`
	ProjectID string `json:"project_id"`
	Key       string `json:"key"`
	Value     string `json:"value"` // Should be encrypted in DB
}

// NewEnvironmentVariable creates a new environment variable.
func NewEnvironmentVariable(id, projectID, key, value string) *EnvironmentVariable {
	return &EnvironmentVariable{
		ID:        id,
		ProjectID: projectID,
		Key:       key,
		Value:     value,
	}
}
