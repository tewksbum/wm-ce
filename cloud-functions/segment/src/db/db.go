package db

import (
	"segment/db/bq"
	"segment/db/csql"
	"segment/models"
)

// Write decides where the data will be stored
func Write(projectID string, csqlDSN string, r models.Record) (err error) {
	opts := r.GetDBOptions()
	switch opts.Type {
	case models.CSQL:
		err = csql.Write(csqlDSN, r)
	case models.BQ:
		err = bq.Write(projectID, r.GetStrOwnerID(), r.GetEntityType(),
			opts.IsPartitioned, opts.PartitionField, r)
	}
	return err
}
