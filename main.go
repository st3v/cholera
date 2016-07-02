package main

import (
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/pborman/uuid"

	"github.com/st3v/cfkit/env"
)

func main() {
	service, err := env.ServiceWithTag("mysql")
	if err != nil {
		log.Fatal("Must bind mysql service")
	}

	db, err := sql.Open("mysql", service.Credentials["uri"].(string))
	if err != nil {
		log.Fatal("Error connecting to db:", err)
	}
	defer db.Close()

	if _, err = db.Exec(`
		CREATE TABLE IF NOT EXISTS foo (
			id INT(11) NOT NULL AUTO_INCREMENT,
			ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			count INT(11),
			uuid VARCHAR(32)
		);
	`); err != nil {
		log.Fatal("Error creating table:", err)
	}

	stmt, err := db.Prepare("INSERT INTO foo (count, uuid) VALUES(?, ?)")
	if err != nil {
		log.Fatal("Error preparing statement:", err)
	}
	defer stmt.Close()

	go func() {
		i := 0
		for {
			<-time.After(time.Second)

			if _, err := stmt.Exec(i, uuid.New()); err != nil {
				log.Println("Error inserting row:", err)
				continue
			}

			i++
		}
	}()

	http.HandleFunc("/node1", getHandler(db))
	http.ListenAndServe(env.Addr(), nil)
}

func getHandler(db *sql.DB) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var id, ts, count, uuid string

		err := db.QueryRow("SELECT id, ts, count, uuid FROM foo ORDER BY id DESC LIMIT 1").Scan(&id, &ts, &count, &uuid)
		switch {
		case err == sql.ErrNoRows:
			fmt.Fprint(w, "No rows")
		case err != nil:
			fmt.Fprintf(w, "Error: %s", err)
		default:
			fmt.Fprint(w, strings.Join([]string{id, ts, count, uuid}, ", "))
		}
	}
}
