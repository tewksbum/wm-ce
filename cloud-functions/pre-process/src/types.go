package preprocess

import "time"

type PubSubMessage struct {
	Data []byte `json:"data"`
}

type Signature struct {
	OwnerID   string `json:"ownerId"`
	Source    string `json:"source"`
	EventID   string `json:"eventId"`
	EventType string `json:"eventType"`
	RecordID  string `json:"recordId"`
	RowNumber int    `json:"rowNum"`
}

type Input struct {
	Signature   Signature         `json:"signature"`
	Passthrough map[string]string `json:"passthrough"`
	Fields      map[string]string `json:"fields"`
	Attributes  map[string]string `json:"attributes"`
}

type Prediction struct {
	Predictions []float64 `json:"predictions"`
}

type MLInput struct {
	Instances [][]float64 `json:"instances"`
}

type RecordDS struct {
	EventType     string    `datastore:"Type"`
	EventID       string    `datastore:"EventID"`
	RecordID      string    `datastore:"RecordID"`
	RowNumber     int       `datastore:"RowNo"`
	Fields        []KVP     `datastore:"Fields,noindex"`
	TimeStamp     time.Time `datastore:"Created"`
	IsPeople      bool      `datastore:"IsPeople"`
	IsProduct     bool      `datastore:"IsProduct"`
	IsCampaign    bool      `datastore:"IsCampaign"`
	IsOrder       bool      `datastore:"IsOrder"`
	IsConsignment bool      `datastore:"IsConsignment"`
	IsOrderDetail bool      `datastore:"IsOrderDetail"`
	IsEvent       bool      `datastore:"IsEvent"`
	MLError       bool      `datastore:"MLError"`
}

type KVP struct {
	Key   string `json:"k" datastore:"k"`
	Value string `json:"v" datastore:"v"`
}

type CampaignERR struct {
	TrustedID  int `json:"TrustedID"`
	CampaignID int `json:"CampaignId"`
	Name       int `json:"Name"`
	Type       int `json:"Type"`
	Channel    int `json:"Channel"`
	StartDate  int `json:"StartDate"`
	EndDate    int `json:"EndDate"`
	Budget     int `json:"Budget"`
}

type ConsignmentERR struct {
	ID       int `json:"ID"`
	ShipDate int `json:"ShipDate"`
}

type EventERR struct {
	ID         int `json:"ID"`
	Type       int `json:"Type"`
	CampaignID int `json:"CampaignId"`
	Browser    int `json:"Browser"`
	Channel    int `json:"Channel"`
	OS         int `json:"StartDate"`
	Domain     int `json:"Domain"`
	URL        int `json:"URL"`
	Location   int `json:"Location"`
	Referrer   int `json:"Referrer"`
	SearchTerm int `json:"SearchTerm"`
}

type OrderERR struct {
	ID         int `json:"ID"`
	Number     int `json:"Number"`
	Date       int `json:"Date"`
	CustomerID int `json:"CustomerID"`
	SubTotal   int `json:"SubTotal"`
	Shipping   int `json:"Shipping"`
	Discount   int `json:"Tax"`
	Tax        int `json:"Discount"`
	Total      int `json:"Total"`
	Channel    int `json:"Channel"`
}

type OrderDetailERR struct {
	ID              int `json:"ID"`
	OrderID         int `json:"OrderID"`
	OrderNumber     int `json:"OrderNumber"`
	ConsigmentID    int `json:"ConsigmentID"`
	ProductID       int `json:"ProductID"`
	ProductSKU      int `json:"ProductSKU"`
	ProductUPC      int `json:"ProductUPC"`
	ProductQuantity int `json:"ProductQuantity"`
	MasterCategory  int `json:"MasterCategory"`
}

type PeopleERR struct {
	Address              int `json:"Address"`
	Address1             int `json:"Address1"`
	Address2             int `json:"Address2"`
	Address3             int `json:"Address3"`
	Address4             int `json:"Address4"`
	FullAddress          int `json:"FullAddress"`
	Age                  int `json:"Age"`
	Birthday             int `json:"Birthday"`
	City                 int `json:"City"`
	Country              int `json:"Country"`
	County               int `json:"County"`
	Email                int `json:"Email"`
	FirstName            int `json:"FirstName"`
	FullName             int `json:"FullName"`
	Gender               int `json:"Gender"`
	LastName             int `json:"LastName"`
	MiddleName           int `json:"MiddleName"`
	ParentEmail          int `json:"ParentEmail"`
	ParentFirstName      int `json:"ParentFirstName"`
	ParentLastName       int `json:"ParentLastName"`
	ParentName           int `json:"ParentName"`
	Phone                int `json:"Phone"`
	State                int `json:"State"`
	Suffix               int `json:"Suffix"`
	ZipCode              int `json:"ZipCode"`
	TrustedID            int `json:"TrustedID"`
	Title                int `json:"Title"`
	SchoolYear           int `json:"SchoolYear"`
	Role                 int `json:"Role"`
	Dorm                 int `json:"Dorm"`
	Room                 int `json:"Room"`
	Organization         int `json:"Organization"`
	AddressTypeResidence int `json:"ATResidence"`
	AddressTypeCampus    int `json:"ATCampus"`
	AddressTypeBusiness  int `json:"ATBusiness"`
	AddressBookBill      int `json:"ABBill"`
	AddressBookShip      int `json:"ABShip"`
	ContainsFirstName    int `json:"ContainsFirstName"`
	ContainsLastName     int `json:"ContainsLastName"`
	ContainsName         int `json:"ContainsName"`
	ContainsCountry      int `json:"ContainsCountry"`
	ContainsEmail        int `json:"ContainsEmail"`
	ContainsAddress      int `json:"ContainsAddress"`
	ContainsCity         int `json:"ContainsCity"`
	ContainsState        int `json:"ContainsState"`
	ContainsZipCode      int `json:"ContainsZipCode"`
	ContainsPhone        int `json:"ContainsPhone"`
	ContainsTitle        int `json:"ContainsTitle"`
	ContainsRole         int `json:"ContainsRole"`
	ContainsStudentRole  int `json:"ContainsStudentRole"`
	Junk                 int `json:"Junk"`
	PermE                int `json:"PermE"`
	PermM                int `json:"PermM"`
	PermS                int `json:"PermS"`
}

type ProductERR struct {
	PID         int `json:"ID"`
	SKU         int `json:"SKU"`
	UPC         int `json:"UPC"`
	Name        int `json:"Name"`
	Description int `json:"Description"`
	Size        int `json:"Size"`
	Color       int `json:"Color"`
	UnitPrice   int `json:"UnitPrice"`
	Contains    int `json:"Contains"`
	Type        int `json:"Type"`
	VendorID    int `json:"VendorId"`
	Vendor      int `json:"Vendor"`
	Cost        int `json:"Cost"`
	Stars       int `json:"Stars"`
	Category    int `json:"Category"`
	Margin      int `json:"Margin"`
}

type ERRFlags struct {
	PeopleFirstName bool
	PeopleLastName  bool
	PeopleAddress   bool
	PeopleAddress1  bool
	PeopleCity      bool
	PeopleZip       bool
	PeoplePhone     bool
	PeopleEmail     bool
	PeopleClientID  bool
	ProductID       bool
	ProductSKU      bool
	ProductName     bool
	ProductVendor   bool
	CampaignID      bool
	EventID         bool
	OrderID         bool
	ConsignmentID   bool
	OrderDetailID   bool
	PeopleFullName  bool
}

type NER struct {
	FAC       float64 `json:"FAC"`
	GPE       float64 `json:"GPE"`
	LOC       float64 `json:"LOC"`
	NORP      float64 `json:"NORP"`
	ORG       float64 `json:"ORG"`
	PERSON    float64 `json:"PERSON"`
	PRODUCT   float64 `json:"PRODUCT"`
	EVENT     float64 `json:"EVENT"`
	WORKOFART float64 `json:"WORK_OF_ART"`
	LAW       float64 `json:"LAW"`
	LANGUAGE  float64 `json:"LANGUAGE"`
	DATE      float64 `json:"DATE"`
	TIME      float64 `json:"TIME"`
	PERCENT   float64 `json:"PERCENT"`
	MONEY     float64 `json:"MONEY"`
	QUANTITY  float64 `json:"QUANTITY"`
	ORDINAL   float64 `json:"ORDINAL"`
	CARDINAL  float64 `json:"CARDINAL"`
}

type PeopleVER struct {
	HASHCODE     int64 `json:"HASH"`
	IS_FIRSTNAME bool  `json:"isFIRSTNAME"`
	IS_LASTNAME  bool  `json:"isLASTNAME"`
	IS_STREET1   bool  `json:"isSTREET1"`
	IS_STREET2   bool  `json:"isSTREET2"`
	IS_STREET3   bool  `json:"isSTREET3"`
	IS_CITY      bool  `json:"isCITY"`
	IS_STATE     bool  `json:"isSTATE"`
	IS_ZIPCODE   bool  `json:"isZIPCODE"`
	IS_COUNTRY   bool  `json:"isCOUNTRY"`
	IS_EMAIL     bool  `json:"isEMAIL"`
	IS_PHONE     bool  `json:"isPHONE"`
}

type EventVER struct {
	IS_BROWSER bool `json:"isBROWSER"`
	IS_CHANNEL bool `json:"isCHANNEL"`
}

type InputColumn struct {
	NER            NER            `json:"NER"`
	PeopleERR      PeopleERR      `json:"PeopleERR"`
	ProductERR     ProductERR     `json:"ProductERR"`
	CampaignERR    CampaignERR    `json:"CampaignERR"`
	OrderERR       OrderERR       `json:"OrderERR"`
	ConsignmentERR ConsignmentERR `json:"ConsignmentERR"`
	OrderDetailERR OrderDetailERR `json:"OrderDetailERR"`
	PeopleVER      PeopleVER      `json:"VER"`
	EventERR       EventERR       `json:"EventERR"`
	EventVER       EventVER       `json:"EventVER"`
	Name           string         `json:"Name"`
	Value          string         `json:"Value"`
	MatchKey       string         `json:"MK"`
	IsAttribute    bool           `json:"IsAttr"`
}

type CityStateZip struct {
	Cities []string `json:"cities"`
	State  string   `json:"state"`
	Zip    string   `json:"zip"`
}

type NERCache struct {
	Columns      []NERcolumns `json:"columns"`
	TimeStamp    time.Time    `json:"time"`
	ApplyCounter int          `json:"counter"`
	Recompute    bool         `json:"dirty"`
	Source       string       `json:"source"`
}

type NERcolumns struct {
	ColumnName  string             `json:"ColumnName"`
	NEREntities map[string]float64 `json:"NEREntities"`
}

type Output struct {
	Signature   Signature         `json:"signature"`
	Passthrough map[string]string `json:"passthrough"`
	Prediction  Prediction        `json:"prediction"`
	Columns     []InputColumn     `json:"columns"`
}

type OutputFlag struct {
	People      bool
	Product     bool
	Campaign    bool
	Order       bool
	Consignment bool
	OrderDetail bool
	Event       bool
}
