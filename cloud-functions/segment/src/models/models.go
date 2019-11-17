package models

import (
	"segment/utils"
	"time"

	"github.com/google/uuid"
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
	GetSurrogateID() string
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
	Signature  string    `json:"signature" bigquery:"signature" sql:"signature"`
	Signatures []string  `json:"signatures" bigquery:"signatures" sql:"signatures"`
	PeopleID   string    `json:"peopleId" bigquery:"peopleid" sql:"people_id"`
	CreatedAt  time.Time `json:"-"  bigquery:"created_at" sql:"created_at"`
	UpdatedAt  time.Time `json:"-"  bigquery:"updated_at" sql:"updated_at"`
}

// GetMap gets the column list for DecodeRecord
func (r *DecodeRecord) GetMap() map[string]interface{} {
	return utils.StructToMap(r, r.ColumnBlackList)
}

// GetSurrogateID gets an invalid surrogate ID (DecodeRecord)
func (r *DecodeRecord) GetSurrogateID() string {
	return uuid.Invalid.String()
}

// GetSignatures gets the decode signatures
func (r *DecodeRecord) GetSignatures() []string {
	sigs := r.Signatures
	if r.Signature != "" {
		sigs = append(sigs, r.Signature)
	}
	return sigs
}

// BaseRecord input for the API
type BaseRecord struct {
	OwnerID         int64     `json:"ownerId" bigquery:"-"`
	EntityType      string    `json:"entityType" bigquery:"-"`
	Source          string    `json:"source" bigquery:"-"`
	Owner           string    `json:"owner" bigquery:"-"`
	Passthrough     string    `json:"passthrough" bigquery:"passthrough"`
	Attributes      string    `json:"attributes" bigquery:"-"`
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

// GetSignatures gets the record signatures (always empty on base record)
func (r *BaseRecord) GetSignatures() []string {
	return []string{}
}

// GetSurrogateID gets an invalid surrogate ID (BaseRecord)
func (r *BaseRecord) GetSurrogateID() string {
	return uuid.Invalid.String()
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
	Signatures  []string    `json:"signatures" bigquery:"signatures"`
	SurrogateID string      `json:"surrogateId" bigquery:"surrogateid"`
	Record      OrderHeader `json:"record" bigquery:"record"`
}

// GetSignatures gets the order header signatures
func (r *OrderHeaderRecord) GetSignatures() []string {
	return r.Signatures
}

// GetSurrogateID gets the order header surrogate ID
func (r *OrderHeaderRecord) GetSurrogateID() string {
	return r.SurrogateID
}

// OrderConsignmentRecord a event type record
type OrderConsignmentRecord struct {
	BaseRecord
	Signatures  []string         `json:"signatures" bigquery:"signatures"`
	SurrogateID string           `json:"surrogateId" bigquery:"surrogateid"`
	Record      OrderConsignment `json:"record" bigquery:"record"`
}

// GetSignatures gets the order consignment signatures
func (r *OrderConsignmentRecord) GetSignatures() []string {
	return r.Signatures
}

// GetSurrogateID gets the order consignment surrogate ID
func (r *OrderConsignmentRecord) GetSurrogateID() string {
	return r.SurrogateID
}

// OrderDetailRecord a event type record
type OrderDetailRecord struct {
	BaseRecord
	Signatures  []string    `json:"signatures" bigquery:"signatures"`
	SurrogateID string      `json:"surrogateId" bigquery:"surrogateid"`
	Record      OrderDetail `json:"record" bigquery:"record"`
}

// GetSignatures gets the order detail signatures
func (r *OrderDetailRecord) GetSignatures() []string {
	return r.Signatures
}

// GetSurrogateID gets the order detail surrogate ID
func (r *OrderDetailRecord) GetSurrogateID() string {
	return r.SurrogateID
}

// PeopleRecord a event type record
type PeopleRecord struct {
	BaseRecord
	Signatures []string `json:"signatures" bigquery:"signatures"`
	// SurrogateID string   `json:"surrogateId" bigquery:"surrogateid"`
	Record People `json:"record" bigquery:"record"`
}

// GetSignatures gets the person signatures
func (r *PeopleRecord) GetSignatures() []string {
	return r.Signatures
}

// // GetSurrogateID gets the person surrogateID
// func (r *PeopleRecord) GetSurrogateID() string {
// 	return r.SurrogateID
// }

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
	PeopleID     string  `json:"peopleId" bigquery:"peopleid"`
	Salutation   string  `json:"salutation" bigquery:"salutation"`
	FirstName    string  `json:"firstName" bigquery:"firstname"`
	LastName     string  `json:"lastName" bigquery:"lastname"`
	Gender       string  `json:"gender" bigquery:"gender"`
	Age          string  `json:"age" bigquery:"age"`
	Emails       []Email `json:"emails" bigquery:"emails"`
	Phones       []Phone `json:"phones" bigquery:"phones"`
	Organization string  `json:"organization" bigquery:"organization"`
	Title        string  `json:"title" bigquery:"title"`
	Role         string  `json:"role" bigquery:"role"`
}

// Email email+type struct
type Email struct {
	Email string `json:"email" bigquery:"email"`
	Type  string `json:"type" bigquery:"type"`
}

// Phone phone+type struct
type Phone struct {
	Phone string `json:"phone" bigquery:"phone"`
	Type  string `json:"type" bigquery:"type"`
}

// Household data
type Household struct {
	HouseholdID string `json:"householdId" bigquery:"householdid"`
	LastName    string `json:"lastName" bigquery:"lastname"`
	Address1    string `json:"address1" bigquery:"address1"`
	Address2    string `json:"address2" bigquery:"address2"`
	Address3    string `json:"address3" bigquery:"address3"`
	City        string `json:"city" bigquery:"city"`
	State       string `json:"state" bigquery:"state"`
	Zip         string `json:"zip" bigquery:"zip"`
	Country     string `json:"country" bigquery:"country"`
}

// FallbackData fallback data struct
type FallbackData struct {
	Data string `json:"data" bigquery:"data"`
}
