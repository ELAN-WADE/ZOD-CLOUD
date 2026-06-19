package main

import (
	"database/sql"
	"fmt"
	"log"

	_ "github.com/mattn/go-sqlite3"
)

func main() {
	db, err := sql.Open("sqlite3", "ncloud.db")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	rows, err := db.Query("SELECT message FROM logs WHERE deployment_id = 'dep_1781874234121085500' AND log_type = 'build' ORDER BY timestamp ASC")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	for rows.Next() {
		var msg string
		if err := rows.Scan(&msg); err != nil {
			log.Fatal(err)
		}
		fmt.Println(msg)
	}
}
