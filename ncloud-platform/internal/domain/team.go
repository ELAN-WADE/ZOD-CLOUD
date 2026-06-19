package domain

import "time"

type TeamRole string

const (
	RoleOwner  TeamRole = "owner"
	RoleAdmin  TeamRole = "admin"
	RoleMember TeamRole = "member"
)

type Team struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	CreatedAt time.Time `json:"created_at"`
}

type TeamMember struct {
	TeamID    string    `json:"team_id"`
	UserID    string    `json:"user_id"`
	Role      TeamRole  `json:"role"`
	JoinedAt  time.Time `json:"joined_at"`
}

func NewTeam(id, name string) *Team {
	return &Team{
		ID:        id,
		Name:      name,
		CreatedAt: time.Now().UTC(),
	}
}
