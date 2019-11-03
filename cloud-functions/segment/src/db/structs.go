package db

import (
	"segment/bq"
	"segment/csql"
)

// Options db options for entities
type Options struct {
	Type        string // csql or bq
	BQOptions   bq.Options
	CSQLOptions csql.Options
}

//Record interface
type Record interface {
	GetStrCustomerID() string
	GetEntityType() string
	GetStorageType() string
	GetDBOptions() Options
}
