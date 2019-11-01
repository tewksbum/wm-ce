package wemade

import (
	"segment/utils"
	"time"

	"cloud.google.com/go/datastore"
)

// WMCustomer contains Customer fields
type WMCustomer struct {
	Name      string
	AccessKey string
	Enabled   bool
	Key       *datastore.Key `datastore:"__key__"`
}

// Event data
type Event struct {
	EventID    string `json:"eventId" bigquery:"eventid"` // signature#
	WMPeopleID string `json:"wmPeopleId" bigquery:"wmpeopleid"`
	Type       string `json:"type" bigquery:"type"`
	Browser    string `json:"browser" bigquery:"browser"`
	OS         string `json:"os" bigquery:"os"`
	Channel    string `json:"channel" bigquery:"channel"`
	Location   string `json:"location" bigquery:"location"`
	Domain     string `json:"domain" bigquery:"domain"`
	URL        string `json:"url" bigquery:"url"`
	Referrer   string `json:"referrer" bigquery:"referrer"`
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
	OrderID    string    `json:"orderId" bigquery:"orderid"`
	OrderDate  time.Time `json:"orderDate" bigquery:"orderdate"`
	SubTotal   string    `json:"subTotal" bigquery:"subtotal"`
	Total      string    `json:"total" bigquery:"total"`
	Discount   string    `json:"discount" bigquery:"discount"`
	Shipping   string    `json:"shipping" bigquery:"shipping"`
	Tax        string    `json:"tax" bigquery:"tax"`
	HashedSigs string    `json:"hashedSigs" bigquery:"hashedsigs"`
}

// OrderConsignment data
type OrderConsignment struct {
	OrderID       string    `json:"orderId" bigquery:"orderid"`
	ConsignmentID string    `json:"consignmentId" bigquery:"consignmentid"`
	ShipDate      time.Time `json:"shipDate" bigquery:"shipdate"`
	SubTotal      string    `json:"subTotal" bigquery:"subtotal"`
	HashedSigs    string    `json:"hashedSigs" bigquery:"hashedsigs"`
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
	HashedSigs    string    `json:"hashedSigs" bigquery:"hashedsigs"`
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
	HashedSigs   string `json:"hashedSigs" bigquery:"hashedsigs"`
}

// APIInput input for the API
type APIInput struct {
	RequestID    string            `json:"requestId"`
	AccessKey    string            `json:"accessKey"`
	EntityType   string            `json:"entityType"` //events, order, product
	Organization string            `json:"organization"`
	Source       string            `json:"source"`
	Owner        string            `json:"owner"`
	Passthrough  map[string]string `json:"passthrough"`
	Attributes   map[string]string `json:"attributes"`
	Data         interface{}       `json:"data"`
}

// APIOutput it's basic json that the API returns in response
type APIOutput struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

//Record interface
type Record interface {
	GetStrCustomerID() string
	GetEntityType() string
}

// BaseRecord input for the API
type BaseRecord struct {
	CustomerID   int64     `json:"customerId" bigquery:"customerid"`
	RequestID    string    `json:"requestId" bigquery:"requestid"`
	EntityType   string    `json:"entityType" bigquery:"entitytype"`
	Organization string    `json:"organization" bigquery:"organization"`
	Source       string    `json:"source" bigquery:"source"`
	Owner        string    `json:"owner" bigquery:"owner"`
	Passthrough  string    `json:"passthrough" bigquery:"passthrough"`
	Attributes   string    `json:"attributes" bigquery:"attributes"`
	Timestamp    time.Time `json:"timestamp" bigquery:"timestamp"`
}

// GetStrCustomerID gets the customer id
func (br *BaseRecord) GetStrCustomerID() string {
	return utils.I64toa(br.CustomerID)
}

// GetEntityType gets the customer id
func (br *BaseRecord) GetEntityType() string {
	return br.EntityType
}

// EventRecord a event type record
type EventRecord struct {
	BaseRecord
	Data Event `json:"record" bigquery:"record"`
}

// CampaignRecord a Campaign type record
type CampaignRecord struct {
	BaseRecord
	Data Campaign `json:"record" bigquery:"record"`
}

// ProductRecord a Product type record
type ProductRecord struct {
	BaseRecord
	Data Product `json:"record" bigquery:"record"`
}

// OrderHeaderRecord a event type record
type OrderHeaderRecord struct {
	BaseRecord
	Data OrderHeader `json:"record" bigquery:"record"`
}

// OrderConsignmentRecord a event type record
type OrderConsignmentRecord struct {
	BaseRecord
	Data OrderConsignment `json:"record" bigquery:"record"`
}

// OrderDetailRecord a event type record
type OrderDetailRecord struct {
	BaseRecord
	Data OrderDetail `json:"record" bigquery:"record"`
}

// PeopleRecord a event type record
type PeopleRecord struct {
	BaseRecord
	Data People `json:"record" bigquery:"record"`
}
