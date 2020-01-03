package csql

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"segment/models"
	"segment/utils"
	"segment/utils/logger"
	"segment/wemade"
	"strings"

	// Import the MySQL SQL driver.
	_ "github.com/go-sql-driver/mysql"

	dbr "github.com/gocraft/dbr/v2"
)

const (
	dialect = "mysql"
)

var (
	_conn                *dbr.Connection = nil
	errUnableToConnect                   = errors.New("Unable to instantiate a connection. err: -108")
	errDeleteNotPossible                 = errors.New("Unable to delete with no conditionals")
)

func initDB(dsn string, method string) (sess *dbr.Session, err error) {
	// create a connection with `dialect` (e.g. "postgres", "mysql", or "sqlite3")
	if _conn == nil {
		logger.InfoFmt("[csql.%s.initDB] Using a new connection.", method)
		_conn, err = dbr.Open(dialect, dsn, nil)
		if err != nil {
			return nil, logger.Err(err)
		}
		_conn.SetMaxOpenConns(1)
	} else {
		logger.InfoFmt("[csql.%s.initDB] Using an already instantiated connection.", method)
	}
	// create a session for each business unit of execution (e.g. a web request or goworkers job)
	return _conn.NewSession(nil), nil
}

// Write the interface into CSQL
func Write(dsn string, r models.Record) (updated bool, err error) {
	sess, err := initDB(dsn, "Write")
	if err != nil {
		return false, logger.Err(errUnableToConnect)
	}
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
	tblNameTick := fmt.Sprintf(tblNameFormatTick, tblName)
	createTbl := getCreateTableStatement(r.GetEntityType(), tblNameTick, opts.IgnoreUniqueFields)
	_, err = tx.Exec(createTbl)
	if err != nil {
		return updated, logger.Err(err)
	}
	var res sql.Result
	compositeID := []string{}
	rIDField := r.GetIDField()
	rmap := r.GetMap()
	rIDFieldValue := rmap[rIDField]
	stmt := tx.Select(rIDField).From(tblNameTick).Where(rIDField+" = ?", rmap[rIDField])
	if strings.Contains(rIDField, ".") {
		compositeID = strings.Split(rIDField, ".")
		if rmap[compositeID[0]] != nil {
			rIDFieldValue = rmap[compositeID[0]].(map[string]interface{})[compositeID[1]]
			stmt = tx.Select(compositeID[1]).From(tblNameTick).Where(compositeID[1]+" = ?", rIDFieldValue)
		}
	}
	buf := dbr.NewBuffer()
	_ = stmt.Build(stmt.Dialect, buf)
	exists, err := stmt.ReturnString()
	logger.InfoFmt("[SELECT]: %s - %s: %s = %s", buf.String(), rIDField, rIDFieldValue, exists)
	if err != nil {
		if !strings.Contains(err.Error(), "1146") && !strings.Contains(err.Error(), "not found") {
			return updated, logger.ErrFmt("[csql.Write.selectStmt] %#v", err)
		}
	}
	if len(exists) < 1 {
		is := tx.InsertInto(tblName)
		switch r.GetEntityType() {
		case models.TypeDecode:
			is = is.Columns(r.GetColumnList()...).Record(r)
		default:
			sigs := `[`
			for _, s := range r.GetSignatures() {
				sigs += `"` + s + `",`
			}
			if sigs != "[" {
				sigs = sigs[:len(sigs)-1] + `]`
				is = is.Pair("signatures", sigs)
			} else {
				sigs = "[]"
			}
			if len(r.GetPassthrough()) > 0 {
				is = is.Pair("passthrough", r.GetPassthrough())
			}
			// if len(r.GetAttributes()) > 0 {
			// 	is = is.Pair("Attributes", r.GetAttributes())
			// }
			rec := utils.StructToMap(r, r.GetColumnBlackList())
			j, _ := json.Marshal(rec["record"])
			// logger.InfoFmt("value: %#v", string(j))
			is = is.Pair("record", string(j))
		}
		buf := dbr.NewBuffer()
		_ = is.Build(is.Dialect, buf)
		logger.InfoFmt("[INSERT]: %s", buf.String())
		res, err = is.Exec()
	} else {
		us := tx.Update(tblName).Where(rIDField+" = ?", rmap[rIDField])
		if len(compositeID) > 0 {
			us = tx.Update(tblName).Where(compositeID[1]+" = ?", rIDFieldValue)
		}
		switch r.GetEntityType() {
		case models.TypeDecode:
			us = us.SetMap(rmap)
		default:
			sigs := `[`
			for _, s := range r.GetSignatures() {
				sigs += `"` + s + `",`
			}
			if sigs != "[" {
				sigs = sigs[:len(sigs)-1] + `]`
				us = us.Set("signatures", sigs)
			} else {
				sigs = "[]"
			}
			if len(r.GetPassthrough()) > 0 {
				us = us.Set("passthrough", r.GetPassthrough())
			}
			// if len(r.GetAttributes()) > 0 {
			// 	us = us.Set("Attributes", r.GetAttributes())
			// }
			rec := utils.StructToMap(r, r.GetColumnBlackList())
			j, _ := json.Marshal(rec["record"])
			// logger.InfoFmt("value: %#v", string(j))
			us = us.Set("record", string(j))
		}
		buf := dbr.NewBuffer()
		_ = us.Build(us.Dialect, buf)
		logger.InfoFmt("[UPDATE]: %s", buf.String())
		res, err = us.Exec()
		if err == nil {
			updated = true
		}
	}
	// Logging of the created insert command
	if err != nil {
		errorito := logger.ErrFmt("[csql.Write.Exec] %#v", err)
		// TODO: remove this conditional for HH
		if r.GetTablename() != models.TblHousehold {
			return updated, errorito
		}
		return true, nil
	}
	ra, _ := res.RowsAffected()
	lid, _ := res.LastInsertId()
	logger.InfoFmt("[csql.Write] rows affected: %d - last inserted id: %d", ra, lid)
	err = tx.Commit()
	if err != nil {
		return updated, logger.ErrFmt("[csql.Write.Commit] %#v", err)
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
	tblName = fmt.Sprintf(tblNameFormatTick, tblName)
	sess, err := initDB(dsn, "Read")
	if err != nil {
		return or, logger.Err(errUnableToConnect)
	}
	tx, err := sess.Begin()
	if err != nil {
		return or, logger.Err(err)
	}
	defer tx.RollbackUnlessCommitted()
	stmt := tx.Select(r.GetSelectColumnList()...).From(tblName)
	if len(opts.Joins) > 0 {
		// TODO : Optimize joins, sprintf the tablename and the j.Tablename into `On`
		for _, j := range opts.Joins {
			switch j.Type {
			case "left":
				stmt = stmt.LeftJoin(j.Table, j.On)
			case "right":
				stmt = stmt.RightJoin(j.Table, j.On)
			case "full":
				stmt = stmt.FullJoin(j.Table, j.On)
			default:
				stmt = stmt.Join(j.Table, j.On)
			}
		}
	}
	if len(opts.Filters) > 0 {
		pfs, err := models.ParseFilters(opts.Filters, false, "", "record")
		if err != nil {
			return or, logger.ErrFmt("[csql.Read.ParsingFilters]: %#v", err)
		}
		if len(pfs) > 0 {
			var vals []interface{}
			buf := dbr.NewBuffer()
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
							switch pf.Op {
							case models.OperationBetween:
								dbr.Expr(pf.ParsedCondition, tmp[0], tmp[1]).Build(stmt.Dialect, buf)
							default:
								v = strings.Join(tmp, ",")
								dbr.Expr(pf.ParsedCondition, v).Build(stmt.Dialect, buf)
							}
							logger.InfoFmt("read param array: [%s] %#v - type: %T", pf.ParamNames[i], v, t)
						default:
							dbr.Expr(pf.ParsedCondition, v).Build(stmt.Dialect, buf)
							logger.InfoFmt("read param [%s] %#v - type: %T\n", pf.ParamNames[i], v, t)
						}
					} else {
						v = nil
					}
					if v == nil {
						dbr.Expr(pf.ParsedCondition).Build(stmt.Dialect, buf)
					} else {
						vals = append(vals, v)
					}
				}
			}
			stmt = stmt.Where(buf.String(), vals...)
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
	opts := r.GetDBOptions()
	tblName := opts.Tablename
	if opts.HasTablenamePrefix {
		tblName = r.GetTablenamePrefix() + tblName
	}
	if opts.HasTablenameSuffix {
		tblName += r.GetTablenameSuffix()
	}
	sess, err := initDB(dsn, "Delete")
	if err != nil {
		return logger.Err(errUnableToConnect)
	}
	tx, err := sess.Begin()
	if err != nil {
		return logger.Err(err)
	}
	defer tx.RollbackUnlessCommitted()
	tblNameTick := fmt.Sprintf(tblNameFormatTick, tblName)
	createTbl := getCreateTableStatement(r.GetEntityType(), tblNameTick, opts.IgnoreUniqueFields)
	_, err = tx.Exec(createTbl)
	if err != nil {
		return logger.ErrFmt("[csql.Delete.createTbl]: %#v", err)
	}
	stmt := tx.DeleteFrom(tblName)

	if len(opts.Filters) > 0 {
		pfs, err := models.ParseFilters(opts.Filters, false, "", "record")
		if err != nil {
			return logger.ErrFmt("[csql.Delete.ParsingFilters]: %#v", err)
		}
		if len(pfs) > 0 {
			var vals []interface{}
			buf := dbr.NewBuffer()
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
							switch pf.Op {
							case models.OperationBetween:
								dbr.Expr(pf.ParsedCondition, tmp[0], tmp[1]).Build(stmt.Dialect, buf)
							default:
								v = strings.Join(tmp, ",")
								dbr.Expr(pf.ParsedCondition, v).Build(stmt.Dialect, buf)
							}
							logger.InfoFmt("delete param array [%s]: %#v - type: %T", pf.ParamNames[i], v, t)
						default:
							dbr.Expr(pf.ParsedCondition, v).Build(stmt.Dialect, buf)
							logger.InfoFmt("delete param [%s]: %#v - type: %T", pf.ParamNames[i], v, t)
						}
					} else {
						v = nil
					}
					if v == nil {
						dbr.Expr(pf.ParsedCondition).Build(stmt.Dialect, buf)
					} else {
						vals = append(vals, v)
					}
				}
			}
			stmt = stmt.Where(buf.String(), vals...)
		}
	} else {
		return errDeleteNotPossible
	}
	buf := dbr.NewBuffer()
	_ = stmt.Build(stmt.Dialect, buf)
	logger.InfoFmt("[DELETE]: %s", buf.String())
	res, err := stmt.Exec()
	if err != nil {
		tx.Rollback()
		return err
	}
	ra, _ := res.RowsAffected()
	lid, _ := res.LastInsertId()
	logger.InfoFmt("[csql.Delete] rows affected: %d - last inserted id: %d", ra, lid)
	err = tx.Commit()
	if err != nil {
		return logger.ErrFmt("[csql.Delete.Commit] %v#", err)
	}
	return nil
}
