package db

import (
	"segment/db/bq"
	"segment/db/csql"
	"segment/models"
	"segment/wemade"
)

// Write decides where the data will be stored, and does so.
func Write(projectID string, csqlDSN string, r models.Record) (updated bool, err error) {
	opts := r.GetDBOptions()
	switch opts.Type {
	case models.CSQL:
		updated, err = csql.Write(csqlDSN, r)
	case models.BQ:
		updated, err = bq.Write(projectID, r.GetStrOwnerID(), r.GetTableName(),
			opts.IsPartitioned, opts.PartitionField, r)
	}
	return updated, err
}

// Read gets where the data is stored, reads and returns it acording to filters
func Read(projectID string, csqlDSN string, r models.Record) (or wemade.OutputRecords, err error) {
	switch r.GetDBOptions().Type {
	case models.CSQL:
		or, err = csql.Read(csqlDSN, r)
	case models.BQ:
		or, err = bq.Read(projectID, r.GetStrOwnerID(), r.GetTableName(), r)
	}
	return or, err
}

// Delete gets where the data is stored and deletes it acording to filters
func Delete(projectID string, csqlDSN string, r models.Record) (err error) {
	switch r.GetDBOptions().Type {
	case models.CSQL:
		err = csql.Delete(csqlDSN, r)
	case models.BQ:
		err = bq.Delete(projectID, r.GetStrOwnerID(), r.GetTableName(), r)
	}
	return err
}
