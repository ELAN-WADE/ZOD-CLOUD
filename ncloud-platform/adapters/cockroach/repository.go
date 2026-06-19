package cockroach

import (
	"context"
	"database/sql"
	"fmt"

	// CockroachDB specific retry helper
	"github.com/cockroachdb/cockroach-go/v2/crdb"
	_ "github.com/lib/pq"
	"github.com/ncloud/platform/internal/domain"
)

// CockroachProjectRepository implements ProjectRepository for Phase 3.
// It handles distributed database nuances like SERIALIZABLE isolation retries.
type CockroachProjectRepository struct {
	db *sql.DB
}

func NewCockroachProjectRepository(db *sql.DB) *CockroachProjectRepository {
	return &CockroachProjectRepository{db: db}
}

func (r *CockroachProjectRepository) Create(ctx context.Context, p *domain.Project) error {
	// ExecuteTx automatically retries the transaction if a serialization error occurs.
	// This is critical for CockroachDB's SERIALIZABLE isolation level.
	err := crdb.ExecuteTx(ctx, r.db, nil, func(tx *sql.Tx) error {
		query := `INSERT INTO projects (id, name, owner_id, framework, created_at) 
		          VALUES ($1, $2, $3, $4, $5)`
		_, err := tx.ExecContext(ctx, query, p.ID, p.Name, p.OwnerID, p.Framework, p.CreatedAt)
		return err
	})

	if err != nil {
		return fmt.Errorf("failed to insert project into cockroachdb: %w", err)
	}
	return nil
}

func (r *CockroachProjectRepository) GetByID(ctx context.Context, id string) (*domain.Project, error) {
	// A simple read doesn't strictly need a retry block if it's a single statement,
	// but it's good practice in highly contested distributed environments.
	var p domain.Project
	err := crdb.ExecuteTx(ctx, r.db, nil, func(tx *sql.Tx) error {
		query := `SELECT id, name, owner_id, framework, created_at FROM projects WHERE id = $1`
		row := tx.QueryRowContext(ctx, query, id)
		return row.Scan(&p.ID, &p.Name, &p.OwnerID, &p.Framework, &p.CreatedAt)
	})

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, domain.ErrNotFound
		}
		return nil, err
	}
	return &p, nil
}

// ... GetByTeam, Update, Delete implementations omitted for brevity but follow the same crdb.ExecuteTx pattern ...
