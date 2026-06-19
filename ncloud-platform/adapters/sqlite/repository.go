package sqlite

import (
	"context"
	"database/sql"

	"github.com/ncloud/platform/internal/domain"
)

// SQLiteUserRepository
type SQLiteUserRepository struct{ db *sql.DB }

func NewSQLiteUserRepository(db *sql.DB) *SQLiteUserRepository { return &SQLiteUserRepository{db: db} }

func (r *SQLiteUserRepository) Create(ctx context.Context, u *domain.User) error {
	query := `INSERT INTO users (id, email, password_hash, tier, created_at) VALUES (?, ?, ?, ?, ?)`
	_, err := r.db.ExecContext(ctx, query, u.ID, u.Email, u.PasswordHash, u.Tier, u.CreatedAt)
	return err
}
func (r *SQLiteUserRepository) GetByID(ctx context.Context, id string) (*domain.User, error) {
	row := r.db.QueryRowContext(ctx, "SELECT id, email, password_hash, tier, created_at FROM users WHERE id = ?", id)
	var u domain.User
	if err := row.Scan(&u.ID, &u.Email, &u.PasswordHash, &u.Tier, &u.CreatedAt); err != nil {
		if err == sql.ErrNoRows { return nil, domain.ErrNotFound }
		return nil, err
	}
	return &u, nil
}
func (r *SQLiteUserRepository) GetByEmail(ctx context.Context, email string) (*domain.User, error) {
	row := r.db.QueryRowContext(ctx, "SELECT id, email, password_hash, tier, created_at FROM users WHERE email = ?", email)
	var u domain.User
	if err := row.Scan(&u.ID, &u.Email, &u.PasswordHash, &u.Tier, &u.CreatedAt); err != nil {
		if err == sql.ErrNoRows { return nil, domain.ErrNotFound }
		return nil, err
	}
	return &u, nil
}

// SQLiteProjectRepository
type SQLiteProjectRepository struct{ db *sql.DB }

func NewSQLiteProjectRepository(db *sql.DB) *SQLiteProjectRepository { return &SQLiteProjectRepository{db: db} }

func (r *SQLiteProjectRepository) Create(ctx context.Context, p *domain.Project) error {
	query := `INSERT INTO projects (id, owner_id, team_id, name, framework, created_at) VALUES (?, ?, ?, ?, ?, ?)`
	_, err := r.db.ExecContext(ctx, query, p.ID, p.OwnerID, p.TeamID, p.Name, p.Framework, p.CreatedAt)
	return err
}
func (r *SQLiteProjectRepository) GetByID(ctx context.Context, id string) (*domain.Project, error) {
	row := r.db.QueryRowContext(ctx, "SELECT id, owner_id, team_id, name, framework, created_at FROM projects WHERE id = ?", id)
	var p domain.Project
	if err := row.Scan(&p.ID, &p.OwnerID, &p.TeamID, &p.Name, &p.Framework, &p.CreatedAt); err != nil {
		if err == sql.ErrNoRows { return nil, domain.ErrNotFound }
		return nil, err
	}
	return &p, nil
}
func (r *SQLiteProjectRepository) GetByOwner(ctx context.Context, ownerID string) ([]*domain.Project, error) {
	rows, err := r.db.QueryContext(ctx, "SELECT id, owner_id, team_id, name, framework, created_at FROM projects WHERE owner_id = ?", ownerID)
	if err != nil { return nil, err }
	defer rows.Close()
	var projects []*domain.Project
	for rows.Next() {
		var p domain.Project
		if err := rows.Scan(&p.ID, &p.OwnerID, &p.TeamID, &p.Name, &p.Framework, &p.CreatedAt); err != nil { return nil, err }
		projects = append(projects, &p)
	}
	return projects, nil
}
func (r *SQLiteProjectRepository) GetByTeam(ctx context.Context, teamID string) ([]*domain.Project, error) {
	rows, err := r.db.QueryContext(ctx, "SELECT id, owner_id, team_id, name, framework, created_at FROM projects WHERE team_id = ?", teamID)
	if err != nil { return nil, err }
	defer rows.Close()
	var projects []*domain.Project
	for rows.Next() {
		var p domain.Project
		if err := rows.Scan(&p.ID, &p.OwnerID, &p.TeamID, &p.Name, &p.Framework, &p.CreatedAt); err != nil { return nil, err }
		projects = append(projects, &p)
	}
	return projects, nil
}
func (r *SQLiteProjectRepository) Update(ctx context.Context, p *domain.Project) error {
	_, err := r.db.ExecContext(ctx, "UPDATE projects SET name = ?, framework = ? WHERE id = ?", p.Name, p.Framework, p.ID)
	return err
}
func (r *SQLiteProjectRepository) Delete(ctx context.Context, id string) error {
	_, err := r.db.ExecContext(ctx, "DELETE FROM projects WHERE id = ?", id)
	return err
}

// SQLiteDeploymentRepository
type SQLiteDeploymentRepository struct{ db *sql.DB }

func NewSQLiteDeploymentRepository(db *sql.DB) *SQLiteDeploymentRepository { return &SQLiteDeploymentRepository{db: db} }

func (r *SQLiteDeploymentRepository) Create(ctx context.Context, d *domain.Deployment) error {
	query := `INSERT INTO deployments (id, project_id, status, image_name, container_id, public_url, internal_url, tunnel_id, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
	_, err := r.db.ExecContext(ctx, query, d.ID, d.ProjectID, string(d.Status), d.ImageName, d.ContainerID, d.PublicURL, d.InternalURL, d.TunnelID, d.CreatedAt)
	return err
}
func (r *SQLiteDeploymentRepository) GetByID(ctx context.Context, id string) (*domain.Deployment, error) {
	row := r.db.QueryRowContext(ctx, "SELECT id, project_id, status, image_name, container_id, public_url, internal_url, tunnel_id, created_at FROM deployments WHERE id = ?", id)
	var d domain.Deployment
	var status string
	if err := row.Scan(&d.ID, &d.ProjectID, &status, &d.ImageName, &d.ContainerID, &d.PublicURL, &d.InternalURL, &d.TunnelID, &d.CreatedAt); err != nil {
		if err == sql.ErrNoRows { return nil, domain.ErrNotFound }
		return nil, err
	}
	d.Status = domain.DeploymentStatus(status)
	return &d, nil
}
func (r *SQLiteDeploymentRepository) UpdateStatus(ctx context.Context, id string, status domain.DeploymentStatus) error {
	_, err := r.db.ExecContext(ctx, "UPDATE deployments SET status = ? WHERE id = ?", string(status), id)
	return err
}
func (r *SQLiteDeploymentRepository) Update(ctx context.Context, d *domain.Deployment) error {
	_, err := r.db.ExecContext(ctx, "UPDATE deployments SET status = ?, image_name = ?, container_id = ?, public_url = ?, internal_url = ?, tunnel_id = ? WHERE id = ?", string(d.Status), d.ImageName, d.ContainerID, d.PublicURL, d.InternalURL, d.TunnelID, d.ID)
	return err
}

// SQLiteDomainRepository
type SQLiteDomainRepository struct{ db *sql.DB }
func NewSQLiteDomainRepository(db *sql.DB) *SQLiteDomainRepository { return &SQLiteDomainRepository{db: db} }
func (r *SQLiteDomainRepository) Create(ctx context.Context, d *domain.Domain) error { return nil }
func (r *SQLiteDomainRepository) GetByProjectID(ctx context.Context, projectID string) ([]*domain.Domain, error) { return nil, nil }
func (r *SQLiteDomainRepository) Delete(ctx context.Context, id string) error { return nil }

// SQLiteEnvironmentVariableRepository
type SQLiteEnvironmentVariableRepository struct{ db *sql.DB }
func NewSQLiteEnvironmentVariableRepository(db *sql.DB) *SQLiteEnvironmentVariableRepository { return &SQLiteEnvironmentVariableRepository{db: db} }
func (r *SQLiteEnvironmentVariableRepository) Create(ctx context.Context, e *domain.EnvironmentVariable) error { return nil }
func (r *SQLiteEnvironmentVariableRepository) GetByProjectID(ctx context.Context, projectID string) ([]*domain.EnvironmentVariable, error) { return nil, nil }
func (r *SQLiteEnvironmentVariableRepository) Delete(ctx context.Context, id string) error { return nil }

// SQLiteBuildLogRepository
type SQLiteBuildLogRepository struct{ db *sql.DB }
func NewSQLiteBuildLogRepository(db *sql.DB) *SQLiteBuildLogRepository { return &SQLiteBuildLogRepository{db: db} }
func (r *SQLiteBuildLogRepository) Create(ctx context.Context, b *domain.BuildLog) error { return nil }
func (r *SQLiteBuildLogRepository) GetByDeploymentID(ctx context.Context, deploymentID string) ([]*domain.BuildLog, error) { return nil, nil }
