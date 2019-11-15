package models

import (
	"segment/utils"
	"time"
)

// QueryFilter filter for queries
type QueryFilter struct {
	Field  string        `json:"field"`
	OpType string        `json:"opType"` // filter
	OpLink *string       `json:"opLink"`
	Op     string        `json:"op"`
	Value  *interface{}  `json:"value"`
	Values []interface{} `json:"values"`
}

// ParsedQueryFilter filter parsed to build queries
type ParsedQueryFilter struct {
	ParsedCondition string        `json:"parsedCondition"`
	ParamNames      []string      `json:"paramNames"`
	Values          []interface{} `json:"values"`
}

// Options db options for entities
type Options struct {
	Type               string // csql or bq
	IsPartitioned      bool
	PartitionField     string
	SchemaName         string
	Tablename          string
	TablenamePrefix    string
	TablenameSuffix    string
	HasTablenamePrefix bool
	HasTablenameSuffix bool
	Filters            []QueryFilter
}

//Record interface
type Record interface {
	GetTablename() string
	GetTablenamePrefix() string
	GetTablenameSuffix() string
	GetOwnerID() int64
	GetStrOwnerID() string
	GetEntityType() string
	GetSource() string
	GetOwner() string
	GetPassthrough() string
	GetAttributes() string
	GetDBType() string
	GetIDField() string
	GetTimestamp() time.Time
	GetDBOptions() Options
	GetSignatures() []string
	GetColumnList() []string
	GetColumnBlackList() []string
	GetMap() map[string]interface{}
	SetCSQLSchemaName(string)
	SetCSQLConnStr(string)
}

// DecodeRecord decode table record
type DecodeRecord struct {
	BaseRecord
	Signature string    `json:"signature" bigquery:"signature" sql:"signature"`
	PeopleID  string    `json:"peopleId" bigquery:"peopleid" sql:"people_id"`
	CreatedAt time.Time `json:"-"  bigquery:"created_at" sql:"created_at"`
	UpdatedAt time.Time `json:"-"  bigquery:"updated_at" sql:"updated_at"`
}

// GetMap gets the column list for DecodeRecord
func (r *DecodeRecord) GetMap() map[string]interface{} {
	return utils.StructToMap(r, r.ColumnBlackList)
}

// GetAttributes gets the entity type
func (r *DecodeRecord) GetAttributes() string {
	return r.Attributes
}

// BaseRecord input for the API
type BaseRecord struct {
	OwnerID         int64     `json:"ownerId" bigquery:"ownerid"`
	EntityType      string    `json:"entityType" bigquery:"entitytype"`
	Source          string    `json:"source" bigquery:"source"`
	Owner           string    `json:"owner" bigquery:"owner"`
	Passthrough     string    `json:"passthrough" bigquery:"passthrough"`
	Attributes      string    `json:"attributes" bigquery:"attributes"`
	Signatures      []string  `json:"signatures" bigquery:"signatures"`
	Timestamp       time.Time `json:"timestamp" bigquery:"timestamp"`
	DBopts          Options   `json:"-" bigquery:"-"`
	StorageType     string    `json:"-" sql:"-" bigquery:"-"` // csql or bq
	IDField         string    `json:"-" sql:"-" bigquery:"-"`
	ColumnList      []string  `json:"-" sql:"-" bigquery:"-"`
	ColumnBlackList []string  `json:"-" sql:"-" bigquery:"-"`
}

// GetOwnerID gets the Customer id
func (r *BaseRecord) GetOwnerID() int64 {
	return r.OwnerID
}

// GetStrOwnerID gets the Customer id
func (r *BaseRecord) GetStrOwnerID() string {
	return utils.I64toa(r.OwnerID)
}

// GetEntityType gets the entity type
func (r *BaseRecord) GetEntityType() string {
	return r.EntityType
}

// GetOwner gets the entity type
func (r *BaseRecord) GetOwner() string {
	return r.Owner
}

// GetSource gets the entity type
func (r *BaseRecord) GetSource() string {
	return r.Source
}

// GetPassthrough gets the entity type
func (r *BaseRecord) GetPassthrough() string {
	return r.Passthrough
}

// GetAttributes gets the entity type
func (r *BaseRecord) GetAttributes() string {
	return r.Attributes
}

// GetTimestamp gets the entity type
func (r *BaseRecord) GetTimestamp() time.Time {
	return r.Timestamp
}

// GetDBOptions gets the db options
func (r *BaseRecord) GetDBOptions() Options {
	return r.DBopts
}

// GetDBType gets the db Type
func (r *BaseRecord) GetDBType() string {
	return r.DBopts.Type
}

// GetTablename gets table name
func (r *BaseRecord) GetTablename() string {
	return r.DBopts.Tablename
}

// GetTablenamePrefix gets table name as a suffix
func (r *BaseRecord) GetTablenamePrefix() string {
	return r.DBopts.TablenamePrefix
}

// GetTablenameAsSuffix gets table name as a suffix
func (r *BaseRecord) GetTablenameAsSuffix() string {
	return "_" + r.DBopts.Tablename
}

// GetTablenameSuffix gets table name as a suffix
func (r *BaseRecord) GetTablenameSuffix() string {
	if r.DBopts.TablenameSuffix != "" {
		return "_" + r.DBopts.TablenameSuffix
	}
	return r.GetTablenameAsSuffix()
}

// GetMap gets the column list
func (r *BaseRecord) GetMap() map[string]interface{} {
	return utils.StructToMap(r, r.ColumnBlackList)
}

// GetIDField gets the id field of the table record
func (r *BaseRecord) GetIDField() string {
	return r.IDField
}

// GetSignatures gets the column list
func (r *BaseRecord) GetSignatures() []string {
	return r.Signatures
}

// GetColumnList gets the column list
func (r *BaseRecord) GetColumnList() []string {
	return r.ColumnList
}

// GetColumnBlackList gets the blacklisted column list
func (r *BaseRecord) GetColumnBlackList() []string {
	return r.ColumnBlackList
}

// SetCSQLSchemaName Sets the schema name for CSQL
func (r *BaseRecord) SetCSQLSchemaName(schemaname string) {
	r.DBopts.SchemaName = schemaname
}

// SetCSQLConnStr Sets the connection string for CSQL
func (r *BaseRecord) SetCSQLConnStr(cnnstr string) {
	r.DBopts.SchemaName = cnnstr
}

// Record Structs

// EventRecord a event type record
type EventRecord struct {
	BaseRecord
	Record Event `json:"record" bigquery:"record"`
}

// CampaignRecord a Campaign type record
type CampaignRecord struct {
	BaseRecord
	SurrogateID string   `json:"surrogateId" bigquery:"surrogateid"`
	Record      Campaign `json:"record" bigquery:"record"`
}

// ProductRecord a Product type record
type ProductRecord struct {
	BaseRecord
	SurrogateID string  `json:"surrogateId" bigquery:"surrogateid"`
	Record      Product `json:"record" bigquery:"record"`
}

// OrderHeaderRecord a event type record
type OrderHeaderRecord struct {
	BaseRecord
	SurrogateID string      `json:"surrogateId" bigquery:"surrogateid"`
	Record      OrderHeader `json:"record" bigquery:"record"`
}

// OrderConsignmentRecord a event type record
type OrderConsignmentRecord struct {
	BaseRecord
	SurrogateID string           `json:"surrogateId" bigquery:"surrogateid"`
	Record      OrderConsignment `json:"record" bigquery:"record"`
}

// OrderDetailRecord a event type record
type OrderDetailRecord struct {
	BaseRecord
	SurrogateID string      `json:"surrogateId" bigquery:"surrogateid"`
	Record      OrderDetail `json:"record" bigquery:"record"`
}

// PeopleRecord a event type record
type PeopleRecord struct {
	BaseRecord
	Record People `json:"record" bigquery:"record"`
}

// HouseholdRecord a Household type record
type HouseholdRecord struct {
	BaseRecord
	SurrogateID string    `json:"surrogateId" bigquery:"surrogateid"`
	Record      Household `json:"record" bigquery:"record"`
}

// FallbackRecord a fallback type record
type FallbackRecord struct {
	BaseRecord
	SurrogateID string       `json:"surrogateId" bigquery:"surrogateid"`
	Record      FallbackData `json:"record" bigquery:"record"`
}

// Data Structs

// Event data
type Event struct {
	EventID  string `json:"eventId" bigquery:"eventid"` // signature#
	Type     string `json:"type" bigquery:"type"`
	Browser  string `json:"browser" bigquery:"browser"`
	OS       string `json:"os" bigquery:"os"`
	Channel  string `json:"channel" bigquery:"channel"`
	Location string `json:"location" bigquery:"location"`
	Domain   string `json:"domain" bigquery:"domain"`
	URL      string `json:"url" bigquery:"url"`
	Referrer string `json:"referrer" bigquery:"referrer"`
}

// Campaign data
type Campaign struct {
	CampaignID string    `json:"campaignId" bigquery:"campaignid"`
	Name       string    `json:"name" bigquery:"name"`
	StartDate  time.Time `json:"startDate" bigquery:"startdate"`
}

// Product data
type Product struct {
	ProductID string `json:"productId" bigquery:"productid"`
	Category  string `json:"category" bigquery:"category"`
	SKU       string `json:"sku" bigquery:"sku"`
	Size      string `json:"size" bigquery:"size"`
	Color     string `json:"color" bigquery:"color"`
}

// OrderHeader data
type OrderHeader struct {
	OrderID   string    `json:"orderId" bigquery:"orderid"`
	OrderDate time.Time `json:"orderDate" bigquery:"orderdate"`
	SubTotal  string    `json:"subTotal" bigquery:"subtotal"`
	Total     string    `json:"total" bigquery:"total"`
	Discount  string    `json:"discount" bigquery:"discount"`
	Shipping  string    `json:"shipping" bigquery:"shipping"`
	Tax       string    `json:"tax" bigquery:"tax"`
}

// OrderConsignment data
type OrderConsignment struct {
	OrderID       string    `json:"orderId" bigquery:"orderid"`
	ConsignmentID string    `json:"consignmentId" bigquery:"consignmentid"`
	ShipDate      time.Time `json:"shipDate" bigquery:"shipdate"`
	SubTotal      string    `json:"subTotal" bigquery:"subtotal"`
}

// OrderDetail data
type OrderDetail struct {
	OrderID       string    `json:"orderId" bigquery:"orderid"`
	ConsignmentID string    `json:"consignmentId" bigquery:"consignmentid"`
	OrderDetailID string    `json:"orderDetailId" bigquery:"orderdetailid"`
	ProductID     string    `json:"productId" bigquery:"productid"`
	SKU           string    `json:"sku" bigquery:"sku"`
	Quantity      int       `json:"quantity" bigquery:"quantity"`
	ShipDate      time.Time `json:"shipDate" bigquery:"shipdate"`
	SubTotal      string    `json:"subTotal" bigquery:"subtotal"`
	UnitPrice     string    `json:"unitPrice" bigquery:"unitprice"`
}

// People data
type People struct {
	PeopleID     string `json:"peopleId" bigquery:"peopleid"`
	Salutation   string `json:"salutation" bigquery:"salutation"`
	FirstName    string `json:"firstName" bigquery:"firstname"`
	LastName     string `json:"lastName" bigquery:"lastname"`
	Gender       string `json:"gender" bigquery:"gender"`
	Age          string `json:"age" bigquery:"age"`
	Organization string `json:"organization" bigquery:"organization"`
	Title        string `json:"title" bigquery:"title"`
	Role         string `json:"role" bigquery:"role"`
}

// Household data
type Household struct {
	HouseholdID string `json:"householdId" bigquery:"householdid"`
	LastName    string `json:"lastName" bigquery:"lastname"`
	Address1    string `json:"address1" bigquery:"address1"`
	Address2    string `json:"address2" bigquery:"address2"`
	City        string `json:"city" bigquery:"city"`
	State       string `json:"state" bigquery:"state"`
	Zip         string `json:"zip" bigquery:"zip"`
	Country     string `json:"country" bigquery:"country"`
}

// FallbackData fallback data struct
type FallbackData struct {
	Data string `json:"data" bigquery:"data"`
}
