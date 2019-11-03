package csql

import (
	"database/sql"
	"log"

	// Import the MySQL SQL driver.
	_ "github.com/go-sql-driver/mysql"
)

func initDB(dsn string) *sql.DB {
	var err error
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		log.Fatalf("Could not open db: %v", err)
	}
	// Only allow 1 connection to the database to avoid overloading it.
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)
	return db
}

// Options options for csql
type Options struct {
	IsPartitioned  bool
	PartitionField string
}

// Write writes the interface into BQ
func Write(dsn string, obj interface{}) (err error) {
	db := initDB(dsn)
	defer db.Close()
	err = db.Ping()
	return err
}

// // MySQLDemo is an example of making a MySQL database query.
// func MySQLDemo(w http.ResponseWriter, r *http.Request) {
//         rows, err := db.Query("SELECT NOW() as now")
//         if err != nil {
//                 log.Printf("db.Query: %v", err)
//                 http.Error(w, "Error querying database", http.StatusInternalServerError)
//                 return
//         }
//         defer rows.Close()

//         now := ""
//         rows.Next()
//         if err := rows.Scan(&now); err != nil {
//                 log.Printf("rows.Scan: %v", err)
//                 http.Error(w, "Error scanning database", http.StatusInternalServerError)
//                 return
//         }
//         fmt.Fprintf(w, "Now: %v", now)
// }
