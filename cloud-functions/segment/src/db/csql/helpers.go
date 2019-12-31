package csql

import (
	"fmt"
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
		household_id VARCHAR(255) NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		INDEX(people_id),
		INDEX(household_id),
		PRIMARY KEY (signature));`
	tblCreateStmt = `CREATE TABLE IF NOT EXISTS %s(
		id serial PRIMARY KEY,
		signatures JSON NULL,
		passthrough JSON NULL,
		attributes JSON NULL,
		record JSON NULL,
		%s
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
		%s);`
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

func getCreateTableStatement(entityType string, tblName string) string {
	switch entityType {
	case models.TypeDecode:
		return fmt.Sprintf(tblDecodeCreateStmt, tblName)
	case models.TypePeople:
		cols := `
			people_id    VARCHAR(255) AS (JSON_UNQUOTE(record->'$.peopleId')),
			salutation   VARCHAR(255) AS (JSON_UNQUOTE(record->'$.salutation')),
			firstName    VARCHAR(255) AS (JSON_UNQUOTE(record->'$.firstName')),
			lastName     VARCHAR(255) AS (JSON_UNQUOTE(record->'$.lastName')),
			gender       VARCHAR(255) AS (JSON_UNQUOTE(record->'$.gender')),
			age          VARCHAR(255) AS (JSON_UNQUOTE(record->'$.age')),
			organization VARCHAR(255) AS (JSON_UNQUOTE(record->'$.organization')),
			title        VARCHAR(255) AS (JSON_UNQUOTE(record->'$.title')),
			role         VARCHAR(255) AS (JSON_UNQUOTE(record->'$.role')),
			ad1          VARCHAR(255) AS (JSON_UNQUOTE(record->'$.ad1')),
			ad2          VARCHAR(255) AS (JSON_UNQUOTE(record->'$.ad2')),
			city         VARCHAR(255) AS (JSON_UNQUOTE(record->'$.city')),
			state        VARCHAR(255) AS (JSON_UNQUOTE(record->'$.state')),
			zip          VARCHAR(255) AS (JSON_UNQUOTE(record->'$.zip')),
			country      VARCHAR(255) AS (JSON_UNQUOTE(record->'$.country')),
			adCorrect    VARCHAR(255) AS (JSON_UNQUOTE(record->'$.adCorrect')),
			emails       JSON AS (record->'$.emails'),
			phones       JSON AS (record->'$.phones'),
		`
		idxs := `,
			INDEX(people_id),
			INDEX(firstName),
			INDEX(gender),
			INDEX(age),
			INDEX(organization),
			INDEX(title),
			INDEX(role),
			INDEX(ad1),
			INDEX(ad2),
			INDEX(city),
			INDEX(state),
			INDEX(zip)
		`
		return fmt.Sprintf(tblCreateStmt, tblName, cols, idxs)
	case models.TypeHousehold:
		cols := `
			household_id VARCHAR(255) AS (JSON_UNQUOTE(record->'$.householdId')),
			lastName     VARCHAR(64)  AS (JSON_UNQUOTE(record->'$.lastName')),
			address1     VARCHAR(255) AS (JSON_UNQUOTE(record->'$.address1')),
			address2     VARCHAR(255) AS (JSON_UNQUOTE(record->'$.address2')),
			address3     VARCHAR(255) AS (JSON_UNQUOTE(record->'$.address3')),
			adCorrect    VARCHAR(255) AS (JSON_UNQUOTE(record->'$.adCorrect')),
			city         VARCHAR(64)  AS (JSON_UNQUOTE(record->'$.city')),
			state        VARCHAR(64)  AS (JSON_UNQUOTE(record->'$.state')),
			zip          VARCHAR(64)  AS (JSON_UNQUOTE(record->'$.zip')),
			country      VARCHAR(32)  AS (JSON_UNQUOTE(record->'$.country')),
		`
		idxs := `,
			INDEX(household_id),
			INDEX(city),
			INDEX(state),
			INDEX(zip)
		`
		return fmt.Sprintf(tblCreateStmt, tblName, cols, idxs)
	default:
		return fmt.Sprintf(tblCreateStmt, tblName, "", "")
	}
}
