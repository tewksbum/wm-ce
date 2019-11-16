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
	tblDecodeCreateStmt = `CREATE TABLE IF NOT EXISTS %s.%s (
		signature VARCHAR(255) NOT NULL,
		people_id VARCHAR(255) NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (signature))`
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
	opts := r.GetDBOptions()
	tblName := opts.Tablename
	if opts.HasTablenamePrefix {
		tblName = r.GetTablenamePrefix() + tblName
	}
	if opts.HasTablenameSuffix {
		tblName += r.GetTablenameSuffix()
	}
	_, err = tx.Exec(fmt.Sprintf(tblDecodeCreateStmt, opts.SchemaName, tblName))
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
	logger.InfoFmt("%s = %s", rmap[rIDField], exists)
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
func Read(dsn string, r models.Record) (or wemade.OutputRecord, err error) {
	opts := r.GetDBOptions()
	tblName := opts.Tablename
	if opts.HasTablenamePrefix {
		tblName = r.GetTablenamePrefix() + tblName
	}
	if opts.HasTablenameSuffix {
		tblName += r.GetTablenameSuffix()
	}
	// querystr := "SELECT " + strings.Join(r.GetColumnList(), ",") + " FROM " + tblName
	sess := initDB(dsn)
	tx, err := sess.Begin()
	if err != nil {
		return or, logger.Err(err)
	}
	defer tx.RollbackUnlessCommitted()
	stmt := tx.Select(r.GetColumnList()...).From(tblName)
	if len(opts.Filters) > 0 {
		pfs, err := models.ParseFilters(opts.Filters, false, "", "record")
		if err != nil {
			return or, logger.ErrFmt("[csql.Read.ParsingFilters]: %#v", err)
		}
		if len(pfs) > 0 {
			for _, pf := range pfs {
				for i := 0; i < len(pf.ParamNames); i++ {
					var v interface{}
					if pf.Values != nil {
						v = pf.Values[i] // converInterfaceBQ(pf.Values[i])
						switch t := v.(type) {
						case []interface{}:
							tmp := []string{}
							for _, vv := range v.([]interface{}) {
								tmp = append(tmp, fmt.Sprint(vv))
							}
							v = strings.Join(tmp, ",")
							logger.InfoFmt("param: %q - type: %T", v, t)
						default:
							logger.InfoFmt("param: %q - type: %T", v, t)
						}
					} else {
						v = nil
					}
					if v == nil {
						stmt = stmt.Where(pf.ParsedCondition)
					} else {
						stmt = stmt.Where(pf.ParsedCondition, v)
					}
				}
			}
		}
		oBy := models.ParseOrderBy(opts.Filters, false)
		if oBy != "" {
			for _, o := range strings.Split(oBy, ",") {
				stmt = stmt.OrderBy(o)
			}
		}
	}
	buf := dbr.NewBuffer()
	_ = stmt.Build(stmt.Dialect, buf)
	logger.InfoFmt("Query: %s", buf.String())
	return loadRows(stmt, r.GetEntityType(), r.GetColumnBlackList())
}

// Delete the interface from CSQL
func Delete(dsn string, r models.Record) error {
	for _, filter := range r.GetDBOptions().Filters {
		logger.InfoFmt("filter: %#v", filter)
	}
	return nil
}
