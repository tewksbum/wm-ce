package db

import (
	"segment/bq"
)

// "segment/wemade"

// DB Types
const (
	CSQL string = "csql"
	BQ   string = "bq"
)

// Write decides where the data will be stored
func Write(projectID string, csqlDSN string, r Record) (err error) {
	switch r.GetDBOptions().Type {
	case CSQL:
		// csql.Write()
	case BQ:
		err = bq.Write(projectID, r.GetStrCustomerID(), r.GetEntityType(), r.GetDBOptions().BQOptions, r)
	}
	return err
}
