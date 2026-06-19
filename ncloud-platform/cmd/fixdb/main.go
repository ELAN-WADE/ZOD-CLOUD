package main
import (
	"database/sql"
	"fmt"
	_ "modernc.org/sqlite"
)
func main() {
	db, err := sql.Open("sqlite", "./ncloud.db")
	if err != nil { panic(err) }
	
	// Add missing columns if they don't exist
	queries := []string{
		"ALTER TABLE deployments RENAME COLUMN state TO status;",
		"ALTER TABLE deployments ADD COLUMN image_name TEXT;",
		"ALTER TABLE deployments ADD COLUMN container_id TEXT;",
		"ALTER TABLE deployments ADD COLUMN public_url TEXT;",
		"ALTER TABLE deployments ADD COLUMN internal_url TEXT;",
		"ALTER TABLE deployments ADD COLUMN tunnel_id TEXT;",
		"ALTER TABLE deployments ADD COLUMN project_id TEXT;",
		"ALTER TABLE projects ADD COLUMN owner_id TEXT;",
		"ALTER TABLE projects ADD COLUMN team_id TEXT;",
		"ALTER TABLE projects ADD COLUMN framework TEXT;",
	}
	
	for _, q := range queries {
		_, err := db.Exec(q)
		if err != nil {
			fmt.Println("Warning or error on:", q, "->", err)
		} else {
			fmt.Println("Success:", q)
		}
	}
}
