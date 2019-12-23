package csql

import (
	"segment/models"
	"segment/utils"
	"segment/utils/logger"
	"segment/wemade"

	"github.com/gocraft/dbr/v2"
)

const (
	tblNameFormatTick   = "`%s`"
	tblDecodeCreateStmt = `CREATE TABLE IF NOT EXISTS %s(
		signature VARCHAR(255) NOT NULL,
		people_id VARCHAR(255) NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		PRIMARY KEY (signature))`
	tblCreateStmt = `CREATE TABLE IF NOT EXISTS %s(
		id serial PRIMARY KEY,
		signatures JSON NULL,
		passthrough JSON NULL,
		record JSON NULL,
		timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	)`
)

func loadRows(stmt *dbr.SelectStmt, entityType string, blacklist []string) (or wemade.OutputRecord, err error) {
	totalrows := 0
	switch entityType {
	case models.TypeCampaign:
		rows := []models.Campaign{}
		totalrows, err = stmt.Load(&rows)
		or.List = appendList(rows, totalrows, entityType, blacklist)
	case models.TypeEvent:
		rows := []models.Event{}
		totalrows, err = stmt.Load(&rows)
		or.List = appendList(rows, totalrows, entityType, blacklist)
	case models.TypeHousehold:
		rows := []models.Household{}
		totalrows, err = stmt.Load(&rows)
		or.List = appendList(rows, totalrows, entityType, blacklist)
	case models.TypeOrderHeader:
		rows := []models.OrderHeader{}
		totalrows, err = stmt.Load(&rows)
		or.List = appendList(rows, totalrows, entityType, blacklist)
	case models.TypeOrderConsignment:
		rows := []models.OrderConsignment{}
		totalrows, err = stmt.Load(&rows)
		or.List = appendList(rows, totalrows, entityType, blacklist)
	case models.TypeOrderDetail:
		rows := []models.OrderDetail{}
		totalrows, err = stmt.Load(&rows)
		or.List = appendList(rows, totalrows, entityType, blacklist)
	case models.TypePeople:
		rows := []models.People{}
		totalrows, err = stmt.Load(&rows)
		or.List = appendList(rows, totalrows, entityType, blacklist)
	case models.TypeProduct:
		rows := []models.Product{}
		totalrows, err = stmt.Load(&rows)
		or.List = appendList(rows, totalrows, entityType, blacklist)
	case models.TypeDecode:
		rows := []models.DecodeRecord{}
		totalrows, err = stmt.Load(&rows)
		or.List = appendList(rows, totalrows, entityType, blacklist)
	default:
		rows := []models.FallbackData{}
		totalrows, err = stmt.Load(&rows)
		or.List = appendList(rows, totalrows, entityType, blacklist)
	}
	logger.InfoFmt("Total returned rows: %#v", totalrows)
	if err != nil {
		return or, logger.ErrFmt("[csql.loadRows]: %q", err)
	}
	or.Count = totalrows
	return or, nil
}

func appendList(rows interface{}, totalrows int, entityType string, blacklist []string) []interface{} {
	var list []interface{}
	for i := 0; i < totalrows; i++ {
		list = append(list, utils.StructToMap(models.GetRecordFromSlice(entityType, rows, i), blacklist))
	}
	return list
}

func getCreateTableStatement(entityType string) string {
	switch entityType {
	case models.TypeDecode:
		return tblDecodeCreateStmt
	default:
		return tblCreateStmt
	}
}
