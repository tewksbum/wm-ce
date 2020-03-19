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
	DefaultSelectColumnList = []string{"record"}
	DefaultColumnList       = []string{"signatures", "eventIds", "passthrough", "attributes"}
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
	OperationEquals           string = "Equals"
	OperationNotEquals        string = "NotEquals"
	OperationLessThan         string = "LessThan"
	OperationLessThanEqual    string = "LessThanEqual"
	OperationGreaterThan      string = "GreaterThan"
	OperationGreaterThanEqual string = "GreaterThanEqual"
	OperationIs               string = "Is"
	OperationIsNull           string = "IsNull"
	OperationIsNotNull        string = "IsNotNull"
	OperationIn               string = "In"
	OperationLike             string = "Like"
	OperationILike            string = "ILike"
	OperationNotLike          string = "NotLike"
	OperationBetween          string = "Between"
	OperationMatch            string = "Match"
	OperationNotIn            string = "NotIn"
	OperationLinkAnd          string = "AND"
	OperationLinkOr           string = "OR"
	OperationTypeFilter       string = "filter"
	OperationTypeOrderBy      string = "order"
	OperationOrderByAsc       string = "ASC"
	OperationOrderByDesc      string = "DESC"
)
