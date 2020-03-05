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
	TypeExpiredSet       = "expiredsets"
	TypeEvent            = "event"
	TypeOrderHeader      = "order"
	TypeOrderDetail      = "orderdetail"
	TypeOrderConsignment = "orderconsignment"
	TypeHousehold        = "household"
	TypeProduct          = "product"
	TypePeople           = "people"
	TypeCampaign         = "campaigns"
	TblDecode            = "decode"
	TblExpiredSet        = "expiredsets"
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
	PeopleIDField           = "peopleId"
	HouseholdIDField        = "householdId"
	IDField                 = "id"
	DefaultSelectColumnList = []string{"record", "eventId"}
	DefaultColumnList       = []string{"signatures", "eventId", "passthrough", "attributes"}
	DecodeIDField           = "signature"
	DecodeColumnList        = []string{"signature", "peopleId", "householdId"}
	DecodeBlackList         = []string{"source", "passthrough", "attributes", "expiredSets",
		"ownerId", "owner", "entityType", "timestamp", "signatures"}
	ExpiredSetIDField    = "expiredId"
	ExpiredSetColumnList = []string{"expiredId", "entity"}
	ExpiredSetBlackList  = []string{"source", "passthrough", "attributes", "expiredSets",
		"ownerId", "owner", "entityType", "timestamp", "signatures"}
)

// Operation types
const (
	OperationEquals           string = "eq"
	OperationNotEquals        string = "noteq"
	OperationLessThan         string = "lt"
	OperationLessThanEqual    string = "ltq"
	OperationGreaterThan      string = "gt"
	OperationGreaterThanEqual string = "gte"
	OperationIs               string = "is"
	OperationIsNull           string = "isnull"
	OperationIsNotNull        string = "isnotnull"
	OperationIn               string = "in"
	OperationNotIn            string = "notin"
	OperationLike             string = "like"
	OperationILike            string = "ilike"
	OperationNotLike          string = "notlike"
	OperationBetween          string = "between"
	OperationLinkAnd          string = "and"
	OperationLinkOr           string = "or"
	OperationTypeFilter       string = "filter"
	OperationTypeOrderBy      string = "order"
	OperationOrderByAsc       string = "ASC"
	OperationOrderByDesc      string = "DESC"
)
