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
	TypeOrderHeader      = "orderheader"
	TypeOrderDetail      = "orderdetail"
	TypeOrderConsignment = "orderconsignment"
	TypeHousehold        = "household"
	TypeProduct          = "product"
	TypePeople           = "people"
	TypeCampaign         = "campaigns"
	TblDecode            = "decode"
	TblnamePrefix        = "seg_"
	TblEvent             = "events"
	TblOrderHeader       = "orderheaders"
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
	DecodeIDField    = "signature"
	DecodeColumnList = []string{"signature", "people_id"}
	DecodeBlackList  = []string{"passthrough", "attributes", "source",
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
