package orderpipeline

// OrderERR Entity Recognition
type OrderERR struct {
	//  Trusted ID
	ID     int `json:"ID"`
	Number int `json:"Number"`
	//  Header
	CustomerID int `json:"CustomerID"`
	OrderDate  int `json:"OrderDate"`
	Terms      int `json:"Terms"`
	Total      int `json:"Total"`
	BillTo     int `json:"BillTo"`
	//  Consignment
	Consignment OrderConsignmentERR `json:"Consignment"`
	//  Detail
	Detail OrderDetailERR `json:"Detail"`
}

// OrderConsignmentERR Entity Recognition
type OrderConsignmentERR struct {
	ID       int `json:"ID"`
	ShipDate int `json:"ShipDate"`
}

// OrderDetailERR Entity Recognition
type OrderDetailERR struct {
	ID           int    `json:"ID"`
	OrderID      int    `json:"OrderID"`
	ConsigmentID int    `json:"ConsigmentID"`
	CustomerID   string `json:"CustomerId"`
	ProductID    int    `json:"ProductID"`
	ProductSKU   int    `json:"ProductSKU"`
	ProductUPC   int    `json:"ProductUPC"`
}

// Consignment detail
type Consignment struct {
	OrderID      string `json:"OrderId" bigquery:"orderid"`
	ConsigmentID string `json:"ConsigmentId" bigquery:"consigmentid"`
	ShipDate     string `json:"ShipDate" bigquery:"shipdate"`
}

// Detail of the order
type Detail struct {
	DetailID     string `json:"DetailId" bigquery:"detailid"`
	OrderID      string `json:"OrderId" bigquery:"orderid"`
	ConsigmentID string `json:"ConsigmentId" bigquery:"consigmentid"`
	CustomerID   string `json:"CustomerId" bigquery:"customerid"`
	ProductID    string `json:"ProductId" bigquery:"productid"`
	SKU          string `json:"SKU" bigquery:"sku"`
	UPC          string `json:"UPC" bigquery:"upc"`
}

// InputERR struct defining the input columns
type InputERR struct {
	//  Trusted ID
	TrustedID int      `json:"TrustedID"`
	Order     OrderERR `json:"Order"`
}

// InputVER value regex
type InputVER struct {
	Hashcode int64
}

// InputColumn input column
type InputColumn struct {
	ERR   InputERR `json:"ERR"`
	VER   InputVER `json:"VER"`
	Name  string   `json:"Name"`
	Value string   `json:"Value"`
}

// InputRecord the input record
type InputRecord struct {
	Columns   []InputColumn `json:"Columns"`
	Owner     int64         `json:"Owner"`
	Request   string        `json:"Request"`
	Row       int           `json:"Row"`
	Source    string        `json:"Source"`
	TimeStamp string        `json:"TimeStamp"`
}

// OutputTrustedID Trusted ID
type OutputTrustedID struct {
	Source   string `json:"Source"`
	SourceID string `json:"SourceId"`
}

// PubSubMessage is the payload of a pubsub event
type PubSubMessage struct {
	Data []byte `json:"data"`
}

// IdentifiedRecord struct
type IdentifiedRecord struct {
	TrustedID   string      `json:"TrustedId" bigquery:"trustedid"`
	OrderID     string      `json:"OrderId" bigquery:"orderid"`
	OrderNumber string      `json:"OrderNumber" bigquery:"ordernumber"`
	CustomerID  string      `json:"CustomerId" bigquery:"customerid"`
	OrderDate   string      `json:"OrderDate" bigquery:"orderdate"`
	BillTo      string      `json:"BillTo" bigquery:"billto"`
	Terms       string      `json:"Terms" bigquery:"terms"`
	Total       string      `json:"Total" bigquery:"total"`
	Consignment Consignment `json:"Consigment" bigquery:"consigment"`
	Detail      Detail      `json:"Detail" bigquery:"detail"`
}

// OutputRecord the output result
type OutputRecord struct {
	TrustedID []OutputTrustedID `json:"TrustedId"`
	Owner     int64             `json:"Owner"`
	Source    string            `json:"Source"`
	Request   string            `json:"Request"`
	Row       int               `json:"Row"`
	TimeStamp string            `json:"TimeStamp"`
	Record    IdentifiedRecord  `json:"Record" bigquery:"OrderDetail"`
}
