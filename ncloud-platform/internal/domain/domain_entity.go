package domain

// Domain represents a custom domain linked to a project.
type Domain struct {
	ID         string `json:"id"`
	ProjectID  string `json:"project_id"`
	Hostname   string `json:"hostname"`
	SSLEnabled bool   `json:"ssl_enabled"`
}

// NewDomain creates a new Domain record.
func NewDomain(id, projectID, hostname string) *Domain {
	return &Domain{
		ID:         id,
		ProjectID:  projectID,
		Hostname:   hostname,
		SSLEnabled: true, // Defaulting to true for ZOD CLOUD domains
	}
}
