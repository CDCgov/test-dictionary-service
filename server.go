package main

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/gin-gonic/gin"
	pq "github.com/lib/pq"
)

func main() {
	s := gin.Default()

	var err error
	var db *sql.DB

	if os.Getenv("POSTGRES_USER") != "" {
		connInfo := fmt.Sprintf(
			"user=%s dbname=%s password=%s host=%s port=%s sslmode=disable",
			os.Getenv("POSTGRES_USER"),
			os.Getenv("POSTGRES_DATABASE"),
			os.Getenv("POSTGRES_PASSWORD"),
			os.Getenv("POSTGRES_PORT_5432_TCP_ADDR"),
			os.Getenv("POSTGRES_PORT_5432_TCP_PORT"),
		)
		db, err = sql.Open("postgres", connInfo)
	} else {
		db, err = sql.Open("postgres", "user=testservice dbname=test_service sslmode=disable")
	}

	if err != nil {
		fmt.Printf("Couldn't connect to postgres: %v", err)
	}
	defer db.Close()

	loadCodes("sample_codes.csv", db)

	s.GET("/codes", func(c *gin.Context) {
		queryString := c.Query("query")
		resultCodes := queryCodes(queryString, db)
		c.JSON(200, resultCodes)
	})

	s.Run(":8080")
}

func queryCodes(queryString string, db *sql.DB) [][]string {
	rows, err := db.Query("SELECT * FROM codes WHERE code LIKE '%' || $1 || '%' OR description LIKE '%' || $1 || '%'", queryString)
	if err != nil {
		fmt.Printf("Error querying postgres: %v", err)
	}
	var results [][]string
	defer rows.Close()
	for rows.Next() {
		var code string
		var desc string
		err = rows.Scan(&code, &desc)
		if err != nil {
			fmt.Printf("Error scanning result rows: %v", err)
		}
		results = append(results, []string{code, desc})
	}
	err = rows.Err()
	if err != nil {
		fmt.Printf("Error during result iteration: %v", err)
	}
	return results
}

func loadCodes(filepath string, db *sql.DB) {
	rows, err := db.Query("SELECT count(1) FROM codes")
	if err != nil {
		fmt.Printf("Error querying postgres: %v", err)
	}
	for rows.Next() {
		var count int
		err = rows.Scan(&count)

		if count > 0 {
			fmt.Println("there are existing codes in the database.  Skipping code loading.")
			return
		}
	}

	txn, err := db.Begin()
	if err != nil {
		fmt.Printf("Couldn't create transaction: %v", err)
	}

	stmt, err := txn.Prepare(pq.CopyIn("codes", "code", "description"))
	if err != nil {
		fmt.Printf("Couldn't create transaction statement: %v", err)
	}

	codeFile, err := os.Open(filepath)
	if err != nil {
		fmt.Printf("Couldn't open code csv: %v", err)
	}

	r := bufio.NewReader(codeFile)
	line, isPrefix, err := r.ReadLine()
	for err == nil && !isPrefix {
		split := strings.Split(string(line), ",")
		fmt.Println(split)
		_, err = stmt.Exec(split[0], split[1])
		line, isPrefix, err = r.ReadLine()
	}
	if isPrefix {
		fmt.Printf("buffer size to small")
		return
	}

	_, err = stmt.Exec()
	if err != nil {
		fmt.Printf("Couldn't execute transaction statement: %v", err)
	}

	err = stmt.Close()
	if err != nil {
		fmt.Printf("Couldn't close transaction statement: %v", err)
	}

	err = txn.Commit()
	if err != nil {
		fmt.Printf("Couldn't commit transaction: %v", err)
	}

}
