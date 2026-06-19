package domain

import (
	"time"
)

// Project represents a user's application on the platform.
type Project struct {
	ID        string    `json:"id"`
	OwnerID   string    `json:"owner_id"` // User ID (if personal project)
	TeamID    string    `json:"team_id"`  // Team ID (if team project)
	Name      string    `json:"name"`
	Framework string    `json:"framework"`
	CreatedAt time.Time `json:"created_at"`
}

// NewProject is a factory function to create a validated project.
func NewProject(id, ownerID, teamID, name, framework string) (*Project, error) {
	return &Project{
		ID:        id,
		OwnerID:   ownerID,
		TeamID:    teamID,
		Name:      name,
		Framework: framework,
		CreatedAt: time.Now().UTC(),
	}, nil
}
