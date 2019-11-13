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
	opts := r.GetDBOptions()
	tblName := opts.TableName
	if opts.HasTableNamePrefix {
		tblName = r.GetTablenamePrefix() + tblName
	}
	if opts.HasTableNameSuffix {
		tblName += r.GetTablenameAsSuffix()
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
	dbOpts := r.GetDBOptions()
	querystr := "" //"SELECT record.* from `" + projectID + "." + datasetID + "`." + tableID
	if len(dbOpts.Filters) > 0 {
		querystr += " WHERE "
		pfs, err := models.ParseFilters(dbOpts.Filters, true, "", "record")
		if err != nil {
			return or, logger.ErrFmt("[csql.Read.ParsingFilters]: %#v", err)
		}
		for _, pf := range pfs {
			querystr += pf.ParsedCondition
			for i := 0; i < len(pf.ParamNames); i++ {
				v := pf.Values[i] // converInterfaceBQ(pf.Values[i])
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
				// params = append(params, bigquery.QueryParameter{
				// 	// Converting pfValues[i] which is interface{} to .(*interface{})
				// 	// then assign Value the *value instead of the pointer.
				// 	Name: pf.ParamNames[i], Value: v,
				// })
			}
		}
		querystr += models.ParseOrderBy(dbOpts.Filters)
	}
	logger.InfoFmt("Query: %s", querystr)
	// ctx := context.Background()
	// bqClient, err := bigquery.NewClient(ctx, projectID)
	// if err != nil {
	// 	return or, logger.Err(err)
	// }
	// q := bqClient.Query(querystr)
	// q.Parameters = params
	// ri, err := q.Read(ctx)
	// if err != nil {
	// 	return or, logger.ErrFmt("[bq.Read.Query.Read]: %#v", err)
	// }
	// totalrows := int(ri.TotalRows)
	// logger.InfoFmt("Total records: %d", totalrows)
	// rec := models.GetRecordType(obj.GetEntityType())
	// logger.InfoFmt("rec: %#v", rec)
	// or.Count = totalrows
	// for i := 1; i <= totalrows; i++ {
	// 	ri.Next(rec)
	// 	or.List = append(or.List, utils.StructToMap(rec, nil))
	// }
	return or, err
}

// Delete the interface from CSQL
func Delete(dsn string, r models.Record) error {
	for _, filter := range r.GetDBOptions().Filters {
		logger.InfoFmt("filter: %#v", filter)
	}
	return nil
}
