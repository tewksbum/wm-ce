package csql

import (
	"database/sql"
	"fmt"
	"segment/models"
	"segment/utils/logger"
	"segment/wemade"
	"strings"

	// Import the MySQL SQL driver.
	_ "github.com/go-sql-driver/mysql"

	dbr "github.com/gocraft/dbr/v2"
)

const (
	dialect             = "mysql"
	tblDecodeSuffix     = "_decode"
	tblDecodeCreateStmt = `CREATE TABLE IF NOT EXISTS %s.%s (
		signature VARCHAR(255) NOT NULL,
		people_id VARCHAR(255) NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (signature))`
	tblDecodeInsert = `INSERT INTO decode(signature,people_id) VALUES (?,?)`
)

func initDB(dsn string) *dbr.Session {
	// create a connection (e.g. "postgres", "mysql", or "sqlite3")
	conn, _ := dbr.Open(dialect, dsn, nil)
	conn.SetMaxOpenConns(1)

	// create a session for each business unit of execution (e.g. a web request or goworkers job)
	return conn.NewSession(nil)
}

// Write the interface into CSQL
func Write(dsn string, r models.Record) (updated bool, err error) {
	sess := initDB(dsn)
	tx, err := sess.Begin()
	if err != nil {
		return updated, logger.Err(err)
	}
	defer tx.RollbackUnlessCommitted()
	rdbOpts := r.GetDBOptions()
	tblName := rdbOpts.TableName
	if rdbOpts.IsTableNameSuffix {
		tblName = r.GetStrOwnerID() + r.GetTableNameAsSuffix()
	}
	_, err = tx.Exec(fmt.Sprintf(tblDecodeCreateStmt, rdbOpts.SchemaName, tblName))
	if err != nil {
		return updated, logger.Err(err)
	}
	var res sql.Result
	rIDField := r.GetIDField()
	rmap := r.GetMap()
	stmt := tx.Select(rIDField).From(tblName).Where(rIDField+" = ?", rmap[rIDField])
	buf := dbr.NewBuffer()
	_ = stmt.Build(stmt.Dialect, buf)
	logger.Info(buf.String())
	exists, err := stmt.ReturnString()
	if err != nil {
		if !strings.Contains(err.Error(), "1146") && !strings.Contains(err.Error(), "not found") {
			return updated, logger.ErrFmt("[csql.Write.selectStmt] %#v", err)
		}
	}
	logger.Info(exists)
	if len(exists) < 1 {
		is := tx.InsertInto(tblName).Columns(r.GetColumnList()...).Record(r)
		buf := dbr.NewBuffer()
		_ = is.Build(is.Dialect, buf)
		logger.Info(buf.String())
		res, err = is.Exec()
	} else {
		us := tx.Update(tblName).
			Where(rIDField+" = ?", rmap[rIDField]).
			SetMap(rmap)
		buf := dbr.NewBuffer()
		_ = us.Build(us.Dialect, buf)
		logger.Info(buf.String())
		res, err = us.Exec()
		updated = true
	}
	// Logging of the created insert command
	if err != nil {
		return updated, logger.ErrFmt("[csql.Write.Exec] %v#", err)
	}
	ra, _ := res.RowsAffected()
	lid, _ := res.LastInsertId()
	logger.InfoFmt("[csql.Write] rows affected: %d - last inserted id: %d", ra, lid)
	err = tx.Commit()
	if err != nil {
		return updated, logger.ErrFmt("[csql.Write.Commit] %v#", err)
	}
	return updated, err
}

// Read the interface from CSQL
func Read(dsn string, r models.Record) (or wemade.OutputRecords, err error) {
	for _, filter := range r.GetDBOptions().Filters {
		logger.InfoFmt("filter: %#v", filter)
	}
	return or, err
}

// Delete the interface from CSQL
func Delete(dsn string, r models.Record) error {
	for _, filter := range r.GetDBOptions().Filters {
		logger.InfoFmt("filter: %#v", filter)
	}
	return nil
}
