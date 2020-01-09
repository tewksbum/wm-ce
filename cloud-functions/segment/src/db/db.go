package db

import (
	"os"
	"segment/db/bq"
	"segment/db/csql"
	"segment/models"
	"segment/utils/logger"
	"segment/wemade"
)

var appEnv string = os.Getenv("APP_ENV")

const (
	recordSignature  string = `JSON_CONTAINS('["%s"]', JSON_ARRAY(signature))`
	recordDistinctC  string = `DISTINCT JSON_OBJECT("%s", %s)`
	peopleIDField    string = "peopleId"
	householdIDField string = "householdId"
)

// Write decides where the data will be stored, and does so.
func Write(projectID string, csqlDSN string, r models.Record) (updated bool, err error) {
	// var one interface{} = 1
	var pid interface{}
	switch r.GetEntityType() {
	case models.TypeOrderHeader:
		break
	case models.TypeOrderConsignment:
		break
	case models.TypeOrderDetail:
		break
	case models.TypeHousehold:
		if (r.(*models.HouseholdRecord)).Record.HouseholdID == "" && appEnv == "dev" {
			logger.Info("[db.Write.HouseholdSignatureUpsert]: householdId is empty, skipping")
		}
		// Cleanup
		// recopy := *r.(*models.HouseholdRecord)
		// rrec := buildHouseholdDecode(r.(*models.HouseholdRecord), "")
		// rrec.SetSelectColumnList([]string{fmt.Sprintf(recordDistinctC, householdIDField, householdIDField)})
		// rrec.AddDBFilter(
		// 	models.QueryFilter{
		// 		Field: fmt.Sprintf(recordSignature, strings.Join(r.GetSignatures(), `", "`)),
		// 		Op:    models.OperationEquals,
		// 		Value: &one,
		// 	},
		// )
		// or, err := csql.Read(csqlDSN, rrec)
		// if err != nil {
		// 	logger.ErrFmt("[db.Write.HouseholdSignatureUpsert.Cleanup.Read]: %#v", err)
		// }
		// for _, cr := range or.List {
		// 	jr := utils.StructToMap(cr, nil)
		// 	pid := jr[householdIDField]
		// 	recopy.AddDBFilter(
		// 		models.QueryFilter{
		// 			Field: householdIDField,
		// 			Op:    models.OperationEquals,
		// 			Value: &pid,
		// 		},
		// 	)
		// 	err = csql.Delete(csqlDSN, &recopy)
		// 	if err != nil {
		// 		logger.ErrFmt("[db.Write.HouseholdSignatureUpsert.Cleanup]: %#v", err)
		// 	}
		// }
		// Cleanup end
		// Cleanup by ID
		pid = (r.(*models.HouseholdRecord)).Record.HouseholdID
		(r.(*models.HouseholdRecord)).AddDBFilter(
			models.QueryFilter{
				Field: householdIDField,
				Op:    models.OperationEquals,
				Value: &pid,
			},
		)
		err = csql.Delete(csqlDSN, (r.(*models.HouseholdRecord)))
		if err != nil {
			logger.ErrFmt("[db.Write.HouseholdSignatureUpsert.Cleanup]: %q", err)
		}
		for _, sig := range r.GetSignatures() {
			rs := buildHouseholdDecode(r.(*models.HouseholdRecord), sig)
			if _, err := csql.Write(csqlDSN, rs); err != nil {
				logger.ErrFmt("[db.Write.HouseholdSignatureUpsert]: %#v", err)
			}
		}
	case models.TypePeople:
		// Cleanup
		for _, s := range r.GetExpiredSets() {
			pid = s
			recopy := *r.(*models.PeopleRecord)
			recopy.AddDBFilter(
				models.QueryFilter{
					Field: peopleIDField,
					Op:    models.OperationEquals,
					Value: &pid,
				},
			)
			err = csql.Delete(csqlDSN, &recopy)
			if err != nil {
				logger.ErrFmt("[db.Write.PeopleSignatureUpsert.Cleanup]: %#v", err)
			}
		}
		// recopy := *r.(*models.PeopleRecord)
		// rrec.SetSelectColumnList([]string{fmt.Sprintf(recordDistinctC, peopleIDField, peopleIDField)})
		// lop := models.OperationLinkAnd
		// pop := models.OperationLinkOr
		// rrec.AddDBFilter(
		// 	models.QueryFilter{
		// 		Field: fmt.Sprintf(recordSignature, strings.Join(r.GetSignatures(), `", "`)),
		// 		Op:    models.OperationEquals,
		// 		Value: &one,
		// 	},
		// )
		// rrec.AddDBFilter(
		// 	models.QueryFilter{
		// 		OpLink: &lop,
		// 		Field:  peopleIDField,
		// 		Op:     models.OperationIsNotNull,
		// 	},
		// )
		// rrec.AddDBFilter(
		// 	models.QueryFilter{
		// 		OpLink: &pop,
		// 		Field:  peopleIDField,
		// 		Op:     models.OperationIsNotNull,
		// 	},
		// )
		// or, err := csql.Read(csqlDSN, rrec)
		// if err != nil {
		// 	logger.ErrFmt("[db.Write.PeopleSignatureUpsert.Cleanup.Read]: %#v", err)
		// }
		// for _, cr := range or.List {
		// 	jr := utils.StructToMap(cr, nil)
		// 	pid := jr[peopleIDField]
		// 	recopy.AddDBFilter(
		// 		models.QueryFilter{
		// 			Field: peopleIDField,
		// 			Op:    models.OperationEquals,
		// 			Value: &pid,
		// 		},
		// 	)
		// 	err = csql.Delete(csqlDSN, &recopy)
		// 	if err != nil {
		// 		logger.ErrFmt("[db.Write.PeopleSignatureUpsert.Cleanup]: %#v", err)
		// 	}
		// }
		// Cleanup end
		//Cleanup by ID
		pid = (r.(*models.PeopleRecord)).Record.PeopleID
		(r.(*models.PeopleRecord)).AddDBFilter(
			models.QueryFilter{
				Field: peopleIDField,
				Op:    models.OperationEquals,
				Value: &pid,
			},
		)
		err = csql.Delete(csqlDSN, (r.(*models.PeopleRecord)))
		if err != nil {
			logger.ErrFmt("[db.Write.PeopleSignatureUpsert.Cleanup]: %q", err)
		}
		(r.(*models.PeopleRecord)).IDField = peopleIDField
		for _, sig := range r.GetSignatures() {
			rs := buildPeopleDecode(r.(*models.PeopleRecord), sig)
			if _, err := csql.Write(csqlDSN, rs); err != nil {
				logger.ErrFmt("[db.Write.PeopleSignatureUpsert]: %#v", err)
			}
		}
	}
	if err == nil {
		switch r.GetDBType() {
		case models.CSQL:
			updated, err = csql.Write(csqlDSN, r)
		case models.BQ:
			updated, err = bq.Write(projectID, r)
		}
	}
	return updated, err
}

// Read gets where the data is stored, reads and returns it acording to filters
func Read(projectID string, csqlDSN string, r models.Record) (or wemade.OutputRecord, err error) {
	switch r.GetDBType() {
	case models.CSQL:
		or, err = csql.Read(csqlDSN, r)
	case models.BQ:
		or, err = bq.Read(projectID, r)
	}
	return or, err
}

// Delete gets where the data is stored and deletes it acording to filters
func Delete(projectID string, csqlDSN string, r models.Record) (err error) {
	switch r.GetDBType() {
	case models.CSQL:
		err = csql.Delete(csqlDSN, r)
	case models.BQ:
		err = bq.Delete(projectID, r)
	}
	return err
}
