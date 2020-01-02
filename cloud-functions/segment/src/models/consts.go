package models

const (
	// CSQL CloudSQL instance - MySQL|PostgreSQL
	CSQL = "csql"
	// BQ Bigquery instance
	BQ = "bq"
)

// Table and type names
var (
	TypeDecode           = "decode"
	TypeEvent            = "event"
	TypeOrderHeader      = "order"
	TypeOrderDetail      = "orderdetail"
	TypeOrderConsignment = "orderconsignment"
	TypeHousehold        = "household"
	TypeProduct          = "product"
	TypePeople           = "people"
	TypeCampaign         = "campaigns"
	TblDecode            = "decode"
	TblnamePrefix        = "seg_"
	TblEvent             = "events"
	TblOrderHeader       = "orders"
	TblOrderDetail       = "orderdetails"
	TblOrderConsignment  = "orderconsignments"
	TblHousehold         = "households"
	TblProduct           = "products"
	TblPeople            = "people"
	TblShed              = "shed"
	TblCampaign          = "campaigns"
	DefPartitionField    = "timestamp"
	DsTblCustomers       = "Customer"
	DsFilterCustomers    = "AccessKey = "
)

// Default variables
var (
	IDField                 = "id"
	DefaultSelectColumnList = []string{"record"}
	DefaultColumnList       = []string{"signatures", "passthrough", "attributes"}
	DecodeIDField           = "signature"
	DecodeColumnList        = []string{"signature", "peopleId", "householdId"}
	DecodeBlackList         = []string{"source", "passthrough", "attributes",
		"ownerId", "owner", "entityType", "timestamp", "signatures"}
)

// Operation types
const (
	OperationEquals           = "eq"
	OperationNotEquals        = "noteq"
	OperationLessThan         = "lt"
	OperationLessThanEqual    = "ltq"
	OperationGreaterThan      = "gt"
	OperationGreaterThanEqual = "gte"
	OperationIs               = "is"
	OperationIsNull           = "isnull"
	OperationIsNotNull        = "isnotnull"
	OperationIn               = "in"
	OperationNotIn            = "notin"
	OperationLike             = "like"
	OperationILike            = "ilike"
	OperationNotLike          = "notlike"
	OperationBetween          = "between"
	OperationLinkAnd          = "and"
	OperationLinkOr           = "or"
	OperationTypeFilter       = "filter"
	OperationTypeOrderBy      = "order"
	OperationOrderByAsc       = "ASC"
	OperationOrderByDesc      = "DESC"
)
