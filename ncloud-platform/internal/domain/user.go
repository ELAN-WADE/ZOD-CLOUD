package domain

import "time"

// User represents a tenant/customer of ZOD CLOUD.
type User struct {
	ID           string    `json:"id"`
	Email        string    `json:"email"`
	PasswordHash string    `json:"-"`
	Tier         string    `json:"tier"` // beta, alpha, ultra
	CreatedAt    time.Time `json:"created_at"`
}

// NewUser creates a new user.
func NewUser(id, email, passwordHash, tier string) *User {
	return &User{
		ID:           id,
		Email:        email,
		PasswordHash: passwordHash,
		Tier:         tier,
		CreatedAt:    time.Now().UTC(),
	}
}
