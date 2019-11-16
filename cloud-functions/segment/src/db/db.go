package db

import (
	"segment/db/bq"
	"segment/db/csql"
	"segment/models"
	"segment/utils/logger"
	"segment/wemade"
)

// Write decides where the data will be stored, and does so.
func Write(projectID string, csqlDSN string, r models.Record) (updated bool, err error) {
	switch r.GetDBType() {
	case models.CSQL:
		updated, err = csql.Write(csqlDSN, r)
	case models.BQ:
		updated, err = bq.Write(projectID, r)
		if err == nil {
			if r.GetEntityType() == models.TypePeople {
				logger.InfoFmt("signatures: %q", r.GetSignatures())
				for _, sig := range r.GetSignatures() {
					rs := buildDecode(r.(*models.PeopleRecord), sig)
					if _, err := csql.Write(csqlDSN, rs); err != nil {
						logger.ErrFmt("[db.Write.SignatureUpsert]: %q", err)
					}
				}
			}
		}
	}
	return updated, err
}

// Read gets where the data is stored, reads and returns it acording to filters
func Read(projectID string, csqlDSN string, r models.Record) (or wemade.OutputRecords, err error) {
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
