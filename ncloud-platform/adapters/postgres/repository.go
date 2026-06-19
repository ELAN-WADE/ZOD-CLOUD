package postgres

import (
	"context"
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
	"github.com/ncloud/platform/internal/domain"
)

// PostgresProjectRepository implements ProjectRepository using PostgreSQL.
// This is our Phase 2 upgrade from SQLite, offering ACID compliance and scalability.
type PostgresProjectRepository struct {
	db *sql.DB
}

func NewPostgresProjectRepository(db *sql.DB) *PostgresProjectRepository {
	return &PostgresProjectRepository{db: db}
}

func (r *PostgresProjectRepository) Create(ctx context.Context, p *domain.Project) error {
	// Postgres uses $1, $2 instead of ?
	query := `INSERT INTO projects (id, name, owner_id, team_id, framework, created_at) 
	          VALUES ($1, $2, $3, $4, $5, $6)`
	_, err := r.db.ExecContext(ctx, query, p.ID, p.Name, p.OwnerID, p.TeamID, p.Framework, p.CreatedAt)
	if err != nil {
		return fmt.Errorf("failed to insert project into postgres: %w", err)
	}
	return nil
}

func (r *PostgresProjectRepository) GetByID(ctx context.Context, id string) (*domain.Project, error) {
	query := `SELECT id, name, owner_id, team_id, framework, created_at FROM projects WHERE id = $1`
	row := r.db.QueryRowContext(ctx, query, id)

	var p domain.Project
	err := row.Scan(&p.ID, &p.Name, &p.OwnerID, &p.TeamID, &p.Framework, &p.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	return &p, nil
}

func (r *PostgresProjectRepository) GetByOwner(ctx context.Context, ownerID string) ([]*domain.Project, error) {
	// Querying multiple rows
	query := `SELECT id, name, owner_id, team_id, framework, created_at FROM projects WHERE owner_id = $1`
	rows, err := r.db.QueryContext(ctx, query, ownerID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var projects []*domain.Project
	for rows.Next() {
		var p domain.Project
		if err := rows.Scan(&p.ID, &p.Name, &p.OwnerID, &p.TeamID, &p.Framework, &p.CreatedAt); err != nil {
			return nil, err
		}
		projects = append(projects, &p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return projects, nil
}

func (r *PostgresProjectRepository) GetByTeam(ctx context.Context, teamID string) ([]*domain.Project, error) {
	// Querying multiple rows
	query := `SELECT id, name, owner_id, team_id, framework, created_at FROM projects WHERE team_id = $1`
	rows, err := r.db.QueryContext(ctx, query, teamID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var projects []*domain.Project
	for rows.Next() {
		var p domain.Project
		if err := rows.Scan(&p.ID, &p.Name, &p.OwnerID, &p.TeamID, &p.Framework, &p.CreatedAt); err != nil {
			return nil, err
		}
		projects = append(projects, &p)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return projects, nil
}

func (r *PostgresProjectRepository) Update(ctx context.Context, p *domain.Project) error {
	query := `UPDATE projects SET name = $1, framework = $2 WHERE id = $3`
	_, err := r.db.ExecContext(ctx, query, p.Name, p.Framework, p.ID)
	return err
}

func (r *PostgresProjectRepository) Delete(ctx context.Context, id string) error {
	query := `DELETE FROM projects WHERE id = $1`
	_, err := r.db.ExecContext(ctx, query, id)
	return err
}
