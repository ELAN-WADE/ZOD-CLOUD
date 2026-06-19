package main
import (
	"database/sql"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
)
func main() {
	db, err := sql.Open("sqlite3", "./ncloud.db")
	if err != nil { panic(err) }
	var c int
	db.QueryRow("SELECT COUNT(*) FROM logs").Scan(&c)
	fmt.Printf("Total logs: %d\n", c)
}
