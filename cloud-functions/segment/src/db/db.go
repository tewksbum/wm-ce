package db

import (
	"segment/db/bq"
	"segment/db/csql"
	"segment/models"
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
func Read(projectID string, csqlDSN string, r models.Record) (err error) {
	opts := r.GetDBOptions()
	switch opts.Type {
	case models.CSQL:
		err = csql.Read(csqlDSN, opts.Filters)
	case models.BQ:
		err = bq.Read(projectID, r.GetStrOwnerID(), r.GetTableName(), opts.Filters)
	}
	return err
}

// Delete gets where the data is stored and deletes it acording to filters
func Delete(projectID string, csqlDSN string, r models.Record) (err error) {
	opts := r.GetDBOptions()
	switch opts.Type {
	case models.CSQL:
		err = csql.Delete(csqlDSN, opts.Filters)
	case models.BQ:
		err = bq.Delete(projectID, r.GetStrOwnerID(), r.GetTableName(),
			opts.Filters)
	}
	return err
}
