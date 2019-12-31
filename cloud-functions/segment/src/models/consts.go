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

// "Decode" specific variables
var (
	IDField          = "id"
	ColumnList       = []string{"signatures", "passthrough", "attributes"}
	DecodeIDField    = "signature"
	DecodeColumnList = []string{"signature", "people_id", "household_id"}
	DecodeBlackList  = []string{"source", "passthrough", "attributes",
		"owner_id", "owner", "entity_type", "timestamp", "signatures"}
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
