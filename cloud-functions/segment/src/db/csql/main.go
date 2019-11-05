package csql

import (
	"fmt"
	"segment/models"
	"segment/utils/logger"

	// Import the MySQL SQL driver.
	_ "github.com/go-sql-driver/mysql"

	dbr "github.com/gocraft/dbr/v2"
)

const (
	tblDecodeSuffix     = "_decode"
	tblDecodeCreateStmt = `CREATE TABLE IF NOT EXISTS segment.%s (
		signature VARCHAR(255) NOT NULL,
		people_id VARCHAR(255) NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (signature))`
	tblDecodeInsert = `INSERT INTO decode(signature,people_id) VALUES (?,?)`
)

func initDB(dsn string) *dbr.Session {
	// create a connection (e.g. "postgres", "mysql", or "sqlite3")
	logger.Info(dsn)
	logger.InfoFmt("hey: %s", dsn)
	conn, _ := dbr.Open("mysql", dsn, nil)
	conn.SetMaxOpenConns(1)

	// create a session for each business unit of execution (e.g. a web request or goworkers job)
	return conn.NewSession(nil)
}

// Write writes the interface into BQ
func Write(dsn string, r models.Record) (err error) {
	sess := initDB(dsn)
	tx, err := sess.Begin()
	if err != nil {
		return logger.Err(err)
	}
	defer tx.RollbackUnlessCommitted()
	tblName := r.GetStrOwnerID() + tblDecodeSuffix
	_, err = tx.Exec(fmt.Sprintf(tblDecodeCreateStmt, tblName))
	if err != nil {
		return logger.Err(err)
	}
	is := tx.InsertInto(tblName).Record(&r).Columns("signature", "people_id")
	res, err := is.Exec()
	if err != nil {
		return logger.ErrFmt("[csql.Write.Exec] %v#", err)
	}
	ra, _ := res.RowsAffected()
	lid, _ := res.LastInsertId()
	logger.InfoFmt("rows affected: %d - last inserted id: %d", ra, lid)
	tx.Commit()
	// sess.InsertInto(models.CSQL)
	return err
}
