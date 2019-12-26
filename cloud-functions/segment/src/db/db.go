package db

import (
	"segment/db/bq"
	"segment/db/csql"
	"segment/models"
	"segment/utils/logger"
	"segment/wemade"
)

const (
	recordPeopleID    string = "record->'$.peopleId'"
	recordHouseholdID string = "record->'$.householdId'"
)

// Write decides where the data will be stored, and does so.
func Write(projectID string, csqlDSN string, r models.Record) (updated bool, err error) {
	switch r.GetEntityType() {
	case models.TypeOrderHeader:
		fallthrough
	case models.TypeOrderConsignment:
		fallthrough
	case models.TypeOrderDetail:
		fallthrough
	case models.TypeHousehold:
		var pid interface{}
		pid = (r.(*models.HouseholdRecord)).Record.HouseholdID
		(r.(*models.HouseholdRecord)).AddDBFilter(
			models.QueryFilter{
				Field: recordHouseholdID,
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
				logger.ErrFmt("[db.Write.HouseholdSignatureUpsert]: %q", err)
			}
		}
	case models.TypePeople:
		// logger.InfoFmt("signatures: %q", r.GetSignatures())
		var pid interface{}
		pid = (r.(*models.PeopleRecord)).Record.PeopleID
		(r.(*models.PeopleRecord)).AddDBFilter(
			models.QueryFilter{
				Field: recordPeopleID,
				Op:    models.OperationEquals,
				Value: &pid,
			},
		)
		err = csql.Delete(csqlDSN, (r.(*models.PeopleRecord)))
		if err != nil {
			logger.ErrFmt("[db.Write.PeopleSignatureUpsert.Cleanup]: %q", err)
		}
		for _, sig := range r.GetSignatures() {
			rs := buildPeopleDecode(r.(*models.PeopleRecord), sig)
			if _, err := csql.Write(csqlDSN, rs); err != nil {
				logger.ErrFmt("[db.Write.PeopleSignatureUpsert]: %q", err)
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
