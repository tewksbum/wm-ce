package csql

import (
	"encoding/json"
	"fmt"
	"segment/models"
	"segment/utils"
	"segment/utils/logger"
	"segment/wemade"

	dbr "github.com/gocraft/dbr/v2"
)

const (
	tblNameFormatTick   = "`%s`"
	tblDecodeCreateStmt = `CREATE TABLE IF NOT EXISTS %s(
		signature VARCHAR(255) NOT NULL,
		peopleId VARCHAR(255) NULL,
		householdId VARCHAR(255) NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
		INDEX(peopleId),
		INDEX(householdId),
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
	tmprows := [][]byte{}
	switch entityType {
	case models.TypeCampaign:
		rows := []models.Campaign{}
		row := models.Campaign{}
		totalrows, err = stmt.Load(&tmprows)
		for _, tr := range tmprows {
			json.Unmarshal(tr, &row)
			rows = append(rows, row)
		}
		or.List = appendList(rows, totalrows, entityType, blacklist)
	case models.TypeEvent:
		rows := []models.Event{}
		row := models.Event{}
		totalrows, err = stmt.Load(&tmprows)
		for _, tr := range tmprows {
			json.Unmarshal(tr, &row)
			rows = append(rows, row)
		}
		or.List = appendList(rows, totalrows, entityType, blacklist)
	case models.TypeHousehold:
		rows := []models.Household{}
		row := models.Household{}
		totalrows, err = stmt.Load(&tmprows)
		for _, tr := range tmprows {
			json.Unmarshal(tr, &row)
			rows = append(rows, row)
		}
		or.List = appendList(rows, totalrows, entityType, blacklist)
	case models.TypeOrderHeader:
		rows := []models.OrderHeader{}
		row := models.OrderHeader{}
		totalrows, err = stmt.Load(&tmprows)
		for _, tr := range tmprows {
			json.Unmarshal(tr, &row)
			rows = append(rows, row)
		}
		or.List = appendList(rows, totalrows, entityType, blacklist)
	case models.TypeOrderConsignment:
		rows := []models.OrderConsignment{}
		row := models.OrderConsignment{}
		totalrows, err = stmt.Load(&tmprows)
		for _, tr := range tmprows {
			json.Unmarshal(tr, &row)
			rows = append(rows, row)
		}
		or.List = appendList(rows, totalrows, entityType, blacklist)
	case models.TypeOrderDetail:
		rows := []models.OrderDetail{}
		row := models.OrderDetail{}
		totalrows, err = stmt.Load(&tmprows)
		for _, tr := range tmprows {
			json.Unmarshal(tr, &row)
			rows = append(rows, row)
		}
		or.List = appendList(rows, totalrows, entityType, blacklist)
	case models.TypePeople:
		rows := []models.People{}
		row := models.People{}
		totalrows, err = stmt.Load(&tmprows)
		for _, tr := range tmprows {
			json.Unmarshal(tr, &row)
			rows = append(rows, row)
		}
		or.List = appendList(rows, totalrows, entityType, blacklist)
	case models.TypeProduct:
		rows := []models.Product{}
		row := models.Product{}
		totalrows, err = stmt.Load(&tmprows)
		for _, tr := range tmprows {
			json.Unmarshal(tr, &row)
			rows = append(rows, row)
		}
		or.List = appendList(rows, totalrows, entityType, blacklist)
	case models.TypeDecode:
		rows := []models.DecodeRecord{}
		row := models.DecodeRecord{}
		totalrows, err = stmt.Load(&tmprows)
		for _, tr := range tmprows {
			json.Unmarshal(tr, &row)
			rows = append(rows, row)
		}
		or.List = appendList(rows, totalrows, entityType, blacklist)
	default:
		rows := []models.FallbackData{}
		row := models.FallbackData{}
		totalrows, err = stmt.Load(&tmprows)
		for _, tr := range tmprows {
			json.Unmarshal(tr, &row)
			rows = append(rows, row)
		}
		or.List = appendList(rows, totalrows, entityType, blacklist)
	}
	logger.DebugFmt("Total returned rows: %#v", totalrows)
	// logger.DebugFmt("Rows: %#v", or.List)
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

func getCreateTableStatement(entityType string, tblName string, ignoreUniqueFields bool) string {
	switch entityType {
	case models.TypeDecode:
		return fmt.Sprintf(tblDecodeCreateStmt, tblName)
	case models.TypePeople:
		cols := `
			peopleId     VARCHAR(255) AS (record->>'$.peopleId')     STORED,
			salutation   VARCHAR(255) AS (record->>'$.salutation')   STORED,
			firstName    VARCHAR(255) AS (record->>'$.firstName')    STORED,
			lastName     VARCHAR(255) AS (record->>'$.lastName')     STORED,
			gender       VARCHAR(255) AS (record->>'$.gender')       STORED,
			age          VARCHAR(255) AS (record->>'$.age')          STORED,
			organization VARCHAR(255) AS (record->>'$.organization') STORED,
			title        VARCHAR(255) AS (record->>'$.title')        STORED,
			role         VARCHAR(255) AS (record->>'$.role')         STORED,
			address1     VARCHAR(255) AS (record->>'$.address1')     STORED,
			address2     VARCHAR(255) AS (record->>'$.address2')     STORED,
			address3     VARCHAR(255) AS (record->>'$.address3')     STORED,
			city         VARCHAR(255) AS (record->>'$.city')         STORED,
			state        VARCHAR(255) AS (record->>'$.state')        STORED,
			zip          VARCHAR(255) AS (record->>'$.zip')          STORED,
			country      VARCHAR(255) AS (record->>'$.country')      STORED,
			perme        VARCHAR(255) AS (record->>'$.perme')        STORED,
			permm        VARCHAR(255) AS (record->>'$.permm')        STORED,
			perms        VARCHAR(255) AS (record->>'$.perms')        STORED,
			adcorrect    VARCHAR(255) AS (record->>'$.adcorrect')    STORED,
			adtype       VARCHAR(255) AS (record->>'$.adtype')       STORED,
			advalid      VARCHAR(255) AS (record->>'$.advalid')      STORED,
			adbook       VARCHAR(255) AS (record->>'$.adbook')       STORED,
			adparser     VARCHAR(255) AS (record->>'$.adparser')     STORED,
			ziptype      VARCHAR(255) AS (record->>'$.ziptype')      STORED,
			recordtype   VARCHAR(255) AS (record->>'$.recordtype')   STORED,
			dob          VARCHAR(255) AS (record->>'$.dob')          STORED,
			status       VARCHAR(255) AS (record->>'$.status')       STORED,
			emails               JSON AS (record->'$.emails')        STORED,
			phones               JSON AS (record->'$.phones')        STORED,
		`
		key := "INDEX(peopleId),"
		if !ignoreUniqueFields {
			key = "UNIQUE KEY(peopleId),"
		}
		idxs := fmt.Sprintf(`,
			%s
			UNIQUE KEY(peopleId),
			INDEX(firstName),
			INDEX(lastName),
			INDEX(gender),
			INDEX(age),
			INDEX(organization),
			INDEX(title),
			INDEX(role),
			INDEX(city),
			INDEX(state),
			INDEX(zip),
			INDEX(perme),
			INDEX(permm),
			INDEX(perms),
			INDEX(adcorrect),
			INDEX(adtype),
			INDEX(advalid),
			INDEX(adbook),
			INDEX(adparser),
			INDEX(ziptype),
			INDEX(recordtype),
			INDEX(dob),
			INDEX(status)
			`, key)
		return fmt.Sprintf(tblCreateStmt, tblName, cols, idxs)
	case models.TypeHousehold:
		cols := `
			householdId  VARCHAR(255) AS (record->>'$.householdId') STORED,
			lastName     VARCHAR(255) AS (record->>'$.lastName')    STORED,
			address1     VARCHAR(255) AS (record->>'$.address1')    STORED,
			address2     VARCHAR(255) AS (record->>'$.address2')    STORED,
			address3     VARCHAR(255) AS (record->>'$.address3')    STORED,
			adcorrect    VARCHAR(255) AS (record->>'$.adcorrect')   STORED,
			city         VARCHAR(255) AS (record->>'$.city')        STORED,
			state        VARCHAR(255) AS (record->>'$.state')       STORED,
			zip          VARCHAR(255) AS (record->>'$.zip')         STORED,
			country      VARCHAR(255) AS (record->>'$.country')     STORED,
			`
		key := "INDEX(householdId),"
		if !ignoreUniqueFields {
			key = "UNIQUE KEY(householdId),"
		}
		idxs := fmt.Sprintf(`,
			%s
			UNIQUE KEY(householdId),
			INDEX(lastName),
			INDEX(city),
			INDEX(state),
			INDEX(zip)
			`, key)
		return fmt.Sprintf(tblCreateStmt, tblName, cols, idxs)
	default:
		return fmt.Sprintf(tblCreateStmt, tblName, "", "")
	}
}
