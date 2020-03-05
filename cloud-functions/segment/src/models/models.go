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

// QueryJoin joins for queries
type QueryJoin struct {
	Type  string `json:"type"`
	Table string `json:"table"`
	On    string `json:"on"` // filter
	Alias string `json:"alias"`
}

// ParsedQueryFilter filter parsed to build queries
type ParsedQueryFilter struct {
	ParsedCondition string        `json:"parsedCondition"`
	Op              string        `json:"op"`
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
	IgnoreUniqueFields bool
	Filters            []QueryFilter
	Joins              []QueryJoin
}

//Record interface
type Record interface {
	GetTablename() string
	GetTablenamePrefix() string
	GetTablenameSuffix() string
	GetOwnerID() int64
	GetEventIDs() []string
	GetStrOwnerID() string
	GetEntityType() string
	GetSurrogateID() string
	GetSource() string
	GetOwner() string
	GetAccessKey() string
	GetWriteToOwner() bool
	GetPassthrough() []Passthrough360
	GetAttributes() string
	GetDBType() string
	GetIDField() string
	GetTimestamp() time.Time
	GetDBOptions() Options
	GetSignatures() []string
	GetExpiredSets() []string
	GetColumnList() []string
	GetColumnBlackList() []string
	GetSelectColumnList() []string
	GetMap() map[string]interface{}
	SetCSQLSchemaName(string)
	SetCSQLConnStr(string)
	SetSelectColumnList([]string)
	AddSelectColumnList(string)
	AddDBFilter(QueryFilter)
	AddDBJoin(QueryJoin)
}

// DecodeRecord decode table record
type DecodeRecord struct {
	BaseRecord
	Signature   string    `json:"signature" bigquery:"signature" sql:"signature"`
	Signatures  []string  `json:"signatures" bigquery:"signatures" sql:"signatures"`
	EventIDs    []string  `json:"eventIds" bigquery:"eventIds" sql:"eventIds"`
	ExpiredSets []string  `json:"expiredSets" bigquery:"expiredsets" sql:"expiredSets"`
	PeopleID    string    `json:"peopleId" bigquery:"peopleid" sql:"peopleId"`
	HouseholdID string    `json:"householdId" bigquery:"householdid" sql:"householdId"`
	CreatedAt   time.Time `json:"-" bigquery:"created_at" sql:"created_at"`
	UpdatedAt   time.Time `json:"-" bigquery:"updated_at" sql:"updated_at"`
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

// GetEventIDs gets the decode EventIDs
func (r *DecodeRecord) GetEventIDs() []string {
	return r.EventIDs
}

// GetExpiredSets gets the decode expired sets
func (r *DecodeRecord) GetExpiredSets() []string {
	return r.ExpiredSets
}

// Passthrough360 defines a passthrough object
type Passthrough360 struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// BaseRecord input for the API
type BaseRecord struct {
	OwnerID          int64            `json:"ownerId" bigquery:"-" sql:"-"`
	EntityType       string           `json:"entityType" bigquery:"-" sql:"-"`
	Source           string           `json:"source" bigquery:"-" sql:"-"`
	Owner            string           `json:"owner" bigquery:"-" sql:"-"`
	Passthrough      []Passthrough360 `json:"passthrough" bigquery:"passthrough" sql:"passthrough"`
	ExpiredSets      []string         `json:"expiredSets,omitifempty" bigquery:"expiredsets" sql:"expiredSets"`
	Attributes       string           `json:"attributes" bigquery:"-" sql:"-"`
	Timestamp        time.Time        `json:"timestamp" bigquery:"timestamp" sql:"timestamp"`
	DBopts           Options          `json:"-" bigquery:"-" sql:"-"`
	StorageType      string           `json:"-" sql:"-" bigquery:"-"` // csql or bq
	IDField          string           `json:"-" sql:"-" bigquery:"-"`
	ColumnList       []string         `json:"-" sql:"-" bigquery:"-"`
	ColumnBlackList  []string         `json:"-" sql:"-" bigquery:"-"`
	SelectColumnList []string         `json:"-" sql:"-" bigquery:"-"`
	AccessKey        string           `json:"-" sql:"-" bigquery:"-"`
	WriteToOwner     bool             `json:"-" sql:"-" bigquery:"-"`
}

// GetWriteToOwner get write to owner
func (r *BaseRecord) GetWriteToOwner() bool {
	return r.WriteToOwner
}

// GetAccessKey get the access key
func (r *BaseRecord) GetAccessKey() string {
	return r.AccessKey
}

// GetOwnerID gets the Customer id
func (r *BaseRecord) GetOwnerID() int64 {
	return r.OwnerID
}

// GetStrOwnerID gets the Customer id
func (r *BaseRecord) GetStrOwnerID() string {
	if r.OwnerID == 0 {
		return r.Owner
	}
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
func (r *BaseRecord) GetPassthrough() []Passthrough360 {
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

// GetEventIDs gets the record signatures (always empty on base record)
func (r *BaseRecord) GetEventIDs() []string {
	return []string{}
}

// GetExpiredSets gets the decode expired sets
func (r *BaseRecord) GetExpiredSets() []string {
	return r.ExpiredSets
}

// GetSurrogateID gets an invalid surrogate ID (BaseRecord)
func (r *BaseRecord) GetSurrogateID() string {
	return uuid.Invalid.String()
}

// GetColumnList gets the column list
func (r *BaseRecord) GetColumnList() []string {
	return r.ColumnList
}

// GetSelectColumnList gets the column list
func (r *BaseRecord) GetSelectColumnList() []string {
	return r.SelectColumnList
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

// SetSelectColumnList sets the column list
func (r *BaseRecord) SetSelectColumnList(list []string) {
	r.SelectColumnList = list
}

// AddDBFilter adds a QueryFilter to current options.QueryFilters
func (r *BaseRecord) AddDBFilter(q QueryFilter) {
	r.DBopts.Filters = append(r.DBopts.Filters, q)
}

// AddDBJoin adds a QueryJoin to current options.Join
func (r *BaseRecord) AddDBJoin(join QueryJoin) {
	r.DBopts.Joins = append(r.DBopts.Joins, join)
}

// AddSelectColumnList sets the column list
func (r *BaseRecord) AddSelectColumnList(col string) {
	r.SelectColumnList = append(r.SelectColumnList, col)
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
	EventIDs    []string    `json:"eventIds" bigquery:"eventIds"`
	SurrogateID string      `json:"surrogateId" bigquery:"surrogateid"`
	Record      OrderHeader `json:"record" bigquery:"record"`
}

// GetSignatures gets the order header signatures
func (r *OrderHeaderRecord) GetSignatures() []string {
	return r.Signatures
}

// GetEventIDs gets the order header EventIDs
func (r *OrderHeaderRecord) GetEventIDs() []string {
	return r.EventIDs
}

// GetSurrogateID gets the order header surrogate ID
func (r *OrderHeaderRecord) GetSurrogateID() string {
	return r.SurrogateID
}

// OrderConsignmentRecord a event type record
type OrderConsignmentRecord struct {
	BaseRecord
	Signatures  []string         `json:"signatures" bigquery:"signatures"`
	EventIDs    []string         `json:"eventIds" bigquery:"eventIds"`
	SurrogateID string           `json:"surrogateId" bigquery:"surrogateid"`
	Record      OrderConsignment `json:"record" bigquery:"record"`
}

// GetSignatures gets the order consignment signatures
func (r *OrderConsignmentRecord) GetSignatures() []string {
	return r.Signatures
}

// GetEventIDs gets the order consignment EventIDs
func (r *OrderConsignmentRecord) GetEventIDs() []string {
	return r.EventIDs
}

// GetSurrogateID gets the order consignment surrogate ID
func (r *OrderConsignmentRecord) GetSurrogateID() string {
	return r.SurrogateID
}

// OrderDetailRecord a event type record
type OrderDetailRecord struct {
	BaseRecord
	Signatures  []string    `json:"signatures" bigquery:"signatures"`
	EventIDs    []string    `json:"eventIds" bigquery:"eventIds"`
	SurrogateID string      `json:"surrogateId" bigquery:"surrogateid"`
	Record      OrderDetail `json:"record" bigquery:"record"`
}

// GetSignatures gets the order detail signatures
func (r *OrderDetailRecord) GetSignatures() []string {
	return r.Signatures
}

// GetEventIDs gets the order detail EventIDs
func (r *OrderDetailRecord) GetEventIDs() []string {
	return r.EventIDs
}

// GetSurrogateID gets the order detail surrogate ID
func (r *OrderDetailRecord) GetSurrogateID() string {
	return r.SurrogateID
}

// PeopleRecord a people type record
type PeopleRecord struct {
	BaseRecord
	// SurrogateID string   `json:"surrogateId" bigquery:"surrogateid"`
	Signatures  []string `json:"signatures" bigquery:"signatures" sql:"signatures"`
	EventIDs    []string `json:"eventIds" bigquery:"eventIds" sql:"eventIds"`
	ExpiredSets []string `json:"expiredSets" bigquery:"expiredsets" sql:"expiredSets"`
	Record      People   `json:"record" bigquery:"record" sql:"record"`
}

// GetMap gets the column list for DecodeRecord
func (r *PeopleRecord) GetMap() map[string]interface{} {
	return utils.StructToMap(r, r.ColumnBlackList)
}

// GetSignatures gets the person signatures
func (r *PeopleRecord) GetSignatures() []string {
	return r.Signatures
}

// GetEventIDs gets the person EventIDs
func (r *PeopleRecord) GetEventIDs() []string {
	return r.EventIDs
}

// GetExpiredSets gets the person ExpiredSets
func (r *PeopleRecord) GetExpiredSets() []string {
	return r.ExpiredSets
}

// // GetSurrogateID gets the person surrogateID
// func (r *PeopleRecord) GetSurrogateID() string {
// 	return r.SurrogateID
// }

// HouseholdRecord a Household type record
type HouseholdRecord struct {
	BaseRecord
	SurrogateID string    `json:"surrogateId" bigquery:"surrogateid"`
	Signatures  []string  `json:"signatures" bigquery:"signatures" sql:"signatures"`
	EventIDs    []string  `json:"eventIds" bigquery:"eventIds" sql:"eventIds"`
	ExpiredSets []string  `json:"expiredSets" bigquery:"expiredsets" sql:"expiredSets"`
	Record      Household `json:"record" bigquery:"record" sql:"record"`
}

// GetMap gets the column list for DecodeRecord
func (r *HouseholdRecord) GetMap() map[string]interface{} {
	return utils.StructToMap(r, r.ColumnBlackList)
}

// GetSignatures gets the person signatures
func (r *HouseholdRecord) GetSignatures() []string {
	return r.Signatures
}

// GetEventIDs gets the person EventIDs
func (r *HouseholdRecord) GetEventIDs() []string {
	return r.EventIDs
}

// GetExpiredSets gets the household ExpiredSets
func (r *HouseholdRecord) GetExpiredSets() []string {
	return r.ExpiredSets
}

// FallbackRecord a fallback type record
type FallbackRecord struct {
	BaseRecord
	SurrogateID string       `json:"surrogateId" bigquery:"surrogateid"`
	Record      FallbackData `json:"record" bigquery:"record"`
}

// ExpiredSetRecord a expired type record
type ExpiredSetRecord struct {
	ExpiredID string `json:"expiredId" bigquery:"expiredid" sql:"expiredId"`
	Entity    string `json:"entity" bigquery:"entity" sql:"entity"`
	BaseRecord
}

// GetMap gets the column list for DecodeRecord
func (r *ExpiredSetRecord) GetMap() map[string]interface{} {
	return utils.StructToMap(r, r.ColumnBlackList)
}

// Data Structs

// Event data
type Event struct {
	EventID  string `json:"eventId" bigquery:"eventid" sql:"event_id"` // signature#
	Type     string `json:"type" bigquery:"type" sql:"type"`
	Browser  string `json:"browser" bigquery:"browser" sql:"browser"`
	OS       string `json:"os" bigquery:"os" sql:"od"`
	Channel  string `json:"channel" bigquery:"channel" sql:"channel"`
	Location string `json:"location" bigquery:"location" sql:"location"`
	Domain   string `json:"domain" bigquery:"domain" sql:"domain"`
	URL      string `json:"url" bigquery:"url" sql:"url"`
	Referrer string `json:"referrer" bigquery:"referrer" sql:"referrer"`
}

// Campaign data
type Campaign struct {
	CampaignID string    `json:"campaignId" bigquery:"campaignid" sql:"campaign_id"`
	Name       string    `json:"name" bigquery:"name" sql:"name"`
	StartDate  time.Time `json:"startDate" bigquery:"startdate" sql:"startdate"`
}

// Product data
type Product struct {
	ProductID string `json:"productId" bigquery:"productid" sql:"product_id"`
	Category  string `json:"category" bigquery:"category" sql:"category"`
	SKU       string `json:"sku" bigquery:"sku" sql:"sku"`
	Size      string `json:"size" bigquery:"size" sql:"size"`
	Color     string `json:"color" bigquery:"color" sql:"color"`
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
	OrderID       string    `json:"orderId" bigquery:"orderid" sql:"order_id"`
	ConsignmentID string    `json:"consignmentId" bigquery:"consignmentid" sql:"consignment_id"`
	ShipDate      time.Time `json:"shipDate" bigquery:"shipdate" sql:"shipdate"`
	SubTotal      string    `json:"subTotal" bigquery:"subtotal" sql:"subtotal"`
}

// OrderDetail data
type OrderDetail struct {
	OrderID       string    `json:"orderId" bigquery:"orderid" sql:"order_id"`
	ConsignmentID string    `json:"consignmentId" bigquery:"consignmentid" sql:"consigment_id"`
	OrderDetailID string    `json:"orderDetailId" bigquery:"orderdetailid" sql:"order_detail_id"`
	ProductID     string    `json:"productId" bigquery:"productid" sql:"product_id"`
	SKU           string    `json:"sku" bigquery:"sku" sql:"sku"`
	Quantity      int       `json:"quantity" bigquery:"quantity" sql:"quantity"`
	ShipDate      time.Time `json:"shipDate" bigquery:"shipdate" sql:"shipdate"`
	SubTotal      string    `json:"subTotal" bigquery:"subtotal" sql:"subtotal"`
	UnitPrice     string    `json:"unitPrice" bigquery:"unitprice" sql:"unitprice"`
}

// People data
type People struct {
	PeopleID     string  `json:"peopleId" bigquery:"peopleid"`
	Salutation   string  `json:"salutation" bigquery:"salutation"`
	FirstName    string  `json:"firstName" bigquery:"firstname"`
	LastName     string  `json:"lastName" bigquery:"lastname"`
	Gender       string  `json:"gender" bigquery:"gender"`
	Age          string  `json:"age" bigquery:"age"`
	Organization string  `json:"organization" bigquery:"organization"`
	Title        string  `json:"title" bigquery:"title"`
	Role         string  `json:"role" bigquery:"role"`
	Address1     string  `json:"address1" bigquery:"address1"`
	Address2     string  `json:"address2" bigquery:"address2"`
	Address3     string  `json:"address3" bigquery:"address3"`
	City         string  `json:"city" bigquery:"city"`
	State        string  `json:"state" bigquery:"state"`
	Zip          string  `json:"zip" bigquery:"zip"`
	Country      string  `json:"country" bigquery:"country"`
	Perme        string  `json:"perme" bigquery:"perme"`
	Permm        string  `json:"permm" bigquery:"permm"`
	Perms        string  `json:"perms" bigquery:"perms"`
	Adcorrect    string  `json:"adcorrect" bigquery:"adcorrect"`
	Adtype       string  `json:"adtype" bigquery:"adtype"`
	Advalid      string  `json:"advalid" bigquery:"advalid"`
	Adbook       string  `json:"adbook" bigquery:"adbook"`
	Adparser     string  `json:"adparser" bigquery:"adparser"`
	Ziptype      string  `json:"ziptype" bigquery:"ziptype"`
	Recordtype   string  `json:"recordtype" bigquery:"recordtype"`
	Dob          string  `json:"dob" bigquery:"dob"`
	Status       string  `json:"status" bigquery:"status"`
	Emails       []Email `json:"emails" bigquery:"emails"`
	Phones       []Phone `json:"phones" bigquery:"phones"`
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
	HouseholdID string `json:"householdId" bigquery:"householdid" sql:"householdid"`
	LastName    string `json:"lastName" bigquery:"lastname" sql:"lastName"`
	Address1    string `json:"address1" bigquery:"address1" sql:"address1"`
	Address2    string `json:"address2" bigquery:"address2" sql:"address2"`
	Address3    string `json:"address3" bigquery:"address3" sql:"address3"`
	AdCorrect   string `json:"adcorrect" bigquery:"adcorrect" sql:"adcorrect"`
	City        string `json:"city" bigquery:"city" sql:"city"`
	State       string `json:"state" bigquery:"state" sql:"state"`
	Zip         string `json:"zip" bigquery:"zip" sql:"zip"`
	Country     string `json:"country" bigquery:"country" sql:"country"`
}

// FallbackData fallback data struct
type FallbackData struct {
	Data string `json:"data" bigquery:"data"`
}
