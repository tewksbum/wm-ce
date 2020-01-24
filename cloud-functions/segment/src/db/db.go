package db

import (
	"os"
	"segment/db/bq"
	"segment/db/csql"
	"segment/models"
	"segment/utils/logger"
	"segment/wemade"
)

var (
	appEnv         string = os.Getenv("APP_ENV")
	cleanupEnabled bool   = os.Getenv("ENABLE_CLEANUP") == "yes"
)

const (
	recordSignature string = `JSON_CONTAINS('["%s"]', JSON_ARRAY(signature))`
	recordDistinctC string = `DISTINCT JSON_OBJECT("%s", %s)`
)

// Write decides where the data will be stored, and does so.
func Write(projectID string, csqlDSN string, r models.Record) (updated bool, err error) {
	skipUpdate := false
	// var one interface{} = 1
	var pid interface{}
	switch r.GetEntityType() {
	case models.TypeExpiredSet:
		skipUpdate = true
	case models.TypeOrderHeader:
		break
	case models.TypeOrderConsignment:
		break
	case models.TypeOrderDetail:
		break
	case models.TypeHousehold:
		skipUpdate = true
		if cleanupEnabled {
			if (r.(*models.HouseholdRecord)).Record.HouseholdID == "" && appEnv == "dev" {
				logger.Info("[db.Write.HouseholdSignatureUpsert]: householdId is empty, skipping")
			}
			pid = (r.(*models.HouseholdRecord)).Record.HouseholdID
			(r.(*models.HouseholdRecord)).AddDBFilter(
				models.QueryFilter{
					Field: models.HouseholdIDField,
					Op:    models.OperationEquals,
					Value: &pid,
				},
			)
			err = csql.Delete(csqlDSN, (r.(*models.HouseholdRecord)))
			if err != nil {
				logger.ErrFmt("[db.Write.HouseholdSignatureUpsert.Cleanup]: %q", err)
			}
		}
		for _, sig := range r.GetSignatures() {
			rs := buildHouseholdDecode(r.(*models.HouseholdRecord), sig)
			if _, err := csql.Write(csqlDSN, rs, !skipUpdate); err != nil {
				logger.ErrFmt("[db.Write.HouseholdSignatureUpsert]: %#v", err)
			}
		}
	case models.TypePeople:
		skipUpdate = true
		// Cleanup
		for _, s := range r.GetExpiredSets() {
			csql.Write(csqlDSN, buildExpiredSet(r.(*models.PeopleRecord), s, models.TblPeople), skipUpdate)
		}
		if cleanupEnabled {
			pid = (r.(*models.PeopleRecord)).Record.PeopleID
			(r.(*models.PeopleRecord)).AddDBFilter(
				models.QueryFilter{
					Field: models.PeopleIDField,
					Op:    models.OperationEquals,
					Value: &pid,
				},
			)
			err = csql.Delete(csqlDSN, (r.(*models.PeopleRecord)))
			if err != nil {
				logger.ErrFmt("[db.Write.PeopleSignatureUpsert.Cleanup]: %q", err)
			} else {
				logger.DebugFmt("[db.Write.PeopleSignatureUpsert.Cleanup]: Deleted peopleId: %s", pid.(string))
			}
			(r.(*models.PeopleRecord)).IDField = models.PeopleIDField
		}
		for _, sig := range r.GetSignatures() {
			rs := buildPeopleDecode(r.(*models.PeopleRecord), sig)
			if _, err := csql.Write(csqlDSN, rs, !skipUpdate); err != nil {
				logger.ErrFmt("[db.Write.PeopleSignatureUpsert]: %#v", err)
			}
		}
	}
	// if err == nil {
	switch r.GetDBType() {
	case models.CSQL:
		updated, err = csql.Write(csqlDSN, r, skipUpdate)
	case models.BQ:
		updated, err = bq.Write(projectID, r)
	}
	// }
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

// SweepExpiredSets gets where the data is stored and deletes it acording to filters
func SweepExpiredSets(projectID string, csqlDSN string, entityType string, entityBlacklist []string) (err error) {
	return csql.SweepExpiredSets(csqlDSN, entityType, entityBlacklist)
	// switch r.GetDBType() {
	// case models.CSQL:
	// err = csql.SweepExpiredSets(csqlDSN, entityType)
	// case models.BQ:
	// 	err = bq.SweepExpiredSets(projectID, r)
	// }
	// return err
}
