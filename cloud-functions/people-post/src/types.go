package peoplepost

import (
	"time"

	"cloud.google.com/go/datastore"
)

// PubSubMessage is the payload of a pubsub event
type PubSubMessage struct {
	Data []byte `json:"data"`
}

type FileReport struct {
	ID               string          `json:"id,omitempty"`
	ProcessingBegin  time.Time       `json:"processingBegin,omitempty"`
	StatusLabel      string          `json:"statusLabel,omitempty"`
	StatusBy         string          `json:"statusBy,omitempty"`
	ColumnMaps       []NameValue     `json:"map,omitempty"` // this is for input
	Errors           []ReportError   `json:"errors"`
	Warnings         []ReportError   `json:"warnings"`
	Counters         []ReportCounter `json:"counters"`
	InputStatistics  []ReportStat    `json:"inputStats"`
	OutputStatistics []ReportStat    `json:"outputStats"`
}

// ReportError stores errors and warnings
type ReportError struct {
	FileLevel bool   `json:"file_level,omitempty"`
	Row       int    `json:"row,omitempty"`
	RecordID  string `json:"record_id,omitempty"`
	Field     string `json:"field,omitempty"`
	Value     string `json:"value,omitempty"`
	Message   string `json:"message,omitempty"`
}

// ReportCounter stores record, purge, murge
type ReportCounter struct {
	Type      string `json:"type,omitempty"`
	Name      string `json:"name,omitempty"`
	Count     int    `json:"count,omitempty"`
	Increment bool   `json:"inc,omitempty"`
}

// ReportStat stores metric such as sparsity
type ReportStat struct {
	Field  string `json:"field,omitempty"`
	Metric string `json:"metric,omitempty"`
	Value  int    `json:"value,omitempty"`
}

type Signature struct {
	OwnerID   string `json:"ownerId"`
	Source    string `json:"source"`
	EventID   string `json:"eventId"`
	EventType string `json:"eventType"`
	FiberType string `json:"fiberType"`
	RecordID  string `json:"recordId"`
}

type Prediction struct {
	Predictions []float64 `json:"predictions"`
}

type InputColumn struct {
	NER         NER       `json:"NER"`
	PeopleERR   PeopleERR `json:"PeopleERR"`
	PeopleVER   PeopleVER `json:"VER"`
	Name        string    `json:"Name"`
	Value       string    `json:"Value"`
	Type        string    `json:"Type"`
	MatchKey    string    `json:"MK"`  // model match key
	MatchKey1   string    `json:"MK1"` // assigned key 1
	MatchKey2   string    `json:"MK2"` // assigned key 2
	MatchKey3   string    `json:"MK3"` // assigned key 3
	IsAttribute bool      `json:"IsAttr"`
}

type Input struct {
	Signature   Signature         `json:"signature"`
	Passthrough map[string]string `json:"passthrough"`
	Prediction  Prediction        `json:"prediction"`
	Columns     []InputColumn     `json:"columns"`
}

type Output struct {
	Signature   Signature         `json:"signature"`
	Passthrough map[string]string `json:"passthrough"`
	MatchKeys   PeopleOutput      `json:"matchkeys"`
}

type MatchKeyField struct {
	Value  string `json:"value"`
	Source string `json:"source"`
	Type   string `json:"type"`
}

type PeopleOutput struct {
	SALUTATION   MatchKeyField `json:"salutation"`
	NICKNAME     MatchKeyField `json:"nickname"`
	FNAME        MatchKeyField `json:"fname"`
	FINITIAL     MatchKeyField `json:"finitial"`
	MNAME        MatchKeyField `json:"mname"`
	LNAME        MatchKeyField `json:"lname"`
	FULLNAME     MatchKeyField `json:"-"` // do not output in json or store in BQ
	AD1          MatchKeyField `json:"ad1"`
	AD1NO        MatchKeyField `json:"ad1no"`
	AD2          MatchKeyField `json:"ad2"`
	AD3          MatchKeyField `json:"ad3"`
	CITY         MatchKeyField `json:"city"`
	STATE        MatchKeyField `json:"state"`
	ZIP          MatchKeyField `json:"zip"`
	ZIP5         MatchKeyField `json:"zip5"`
	COUNTRY      MatchKeyField `json:"country"`
	MAILROUTE    MatchKeyField `json:"mailroute"`
	ADTYPE       MatchKeyField `json:"adtype"`
	ZIPTYPE      MatchKeyField `json:"ziptype"`
	RECORDTYPE   MatchKeyField `json:"recordtype"`
	ADBOOK       MatchKeyField `json:"adbook"`
	ADPARSER     MatchKeyField `json:"adparser"`
	ADCORRECT    MatchKeyField `json:"adcorrect"`
	ADVALID      MatchKeyField `json:"advalid"`
	DORM         MatchKeyField `json:"-"` // do not output in json or store in BQ
	ROOM         MatchKeyField `json:"-"` // do not output in json or store in BQ
	FULLADDRESS  MatchKeyField `json:"-"` // do not output in json or store in BQ
	CITYSTATEZIP MatchKeyField `json:"-"` // do not output in json or store in BQ
	EMAIL        MatchKeyField `json:"email"`
	PHONE        MatchKeyField `json:"phone"`
	TRUSTEDID    MatchKeyField `json:"trustedId"`
	CLIENTID     MatchKeyField `json:"clientId"`
	GENDER       MatchKeyField `json:"gender"`
	AGE          MatchKeyField `json:"age"`
	DOB          MatchKeyField `json:"dob"`
	ORGANIZATION MatchKeyField `json:"organization"`
	TITLE        MatchKeyField `json:"title"`
	ROLE         MatchKeyField `json:"role"`
	STATUS       MatchKeyField `json:"status"`
	PermE        MatchKeyField `json:"PermE"`
	PermM        MatchKeyField `json:"PermM"`
	PermS        MatchKeyField `json:"PermS"`
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
	ContainsName         int `json:"ContainsName"`
	ContainsLastName     int `json:"ContainsLastName"`
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

type SmartyStreetResponse []struct {
	InputIndex           int    `json:"input_index"`
	CandidateIndex       int    `json:"candidate_index"`
	DeliveryLine1        string `json:"delivery_line_1"`
	LastLine             string `json:"last_line"`
	DeliveryPointBarcode string `json:"delivery_point_barcode"`
	Components           struct {
		PrimaryNumber           string `json:"primary_number"`
		StreetPredirection      string `json:"street_predirection"`
		StreetName              string `json:"street_name"`
		StreetSuffix            string `json:"street_suffix"`
		SecondaryNumber         string `json:"secondary_number"`
		SecondaryDesignator     string `json:"secondary_designator"`
		CityName                string `json:"city_name"`
		DefaultCityName         string `json:"default_city_name"`
		StateAbbreviation       string `json:"state_abbreviation"`
		Zipcode                 string `json:"zipcode"`
		Plus4Code               string `json:"plus4_code"`
		DeliveryPoint           string `json:"delivery_point"`
		DeliveryPointCheckDigit string `json:"delivery_point_check_digit"`
	} `json:"components"`
	Metadata struct {
		RecordType            string  `json:"record_type"`
		ZipType               string  `json:"zip_type"`
		CountyFips            string  `json:"county_fips"`
		CountyName            string  `json:"county_name"`
		CarrierRoute          string  `json:"carrier_route"`
		CongressionalDistrict string  `json:"congressional_district"`
		Rdi                   string  `json:"rdi"`
		ElotSequence          string  `json:"elot_sequence"`
		ElotSort              string  `json:"elot_sort"`
		Latitude              float64 `json:"latitude"`
		Longitude             float64 `json:"longitude"`
		Precision             string  `json:"precision"`
		TimeZone              string  `json:"time_zone"`
		UtcOffset             int     `json:"utc_offset"`
		Dst                   bool    `json:"dst"`
	} `json:"metadata"`
	Analysis struct {
		DpvMatchCode string `json:"dpv_match_code"`
		DpvFootnotes string `json:"dpv_footnotes"`
		DpvCmra      string `json:"dpv_cmra"`
		DpvVacant    string `json:"dpv_vacant"`
		Active       string `json:"active"`
		Footnotes    string `json:"footnotes"`
	} `json:"analysis"`
}

type AddressParsed struct {
	Number      string `json:"number"`
	Street      string `json:"street"`
	Type        string `json:"type"`
	SecUnitType string `json:"sec_unit_type"`
	SecUnitNum  string `json:"sec_unit_num"`
	City        string `json:"city"`
	State       string `json:"state"`
	Zip         string `json:"zip"`
	Plus4       string `json:"plus4"`
}

type LibPostal struct {
	Label string `json:"label"`
	Value string `json:"value"`
}

type CityStateZip struct {
	Cities []string `json:"cities"`
	State  string   `json:"state"`
	Zip    string   `json:"zip"`
}

type CityState struct {
	City  string
	State string
}

type LibPostalParsed struct {
	HOUSE          string
	CATEGORY       string
	NEAR           string
	HOUSE_NUMBER   string
	ROAD           string
	UNIT           string
	LEVEL          string
	STAIRCASE      string
	ENTRANCE       string
	PO_BOX         string
	POSTCODE       string
	SUBURB         string
	CITY_DISTRICT  string
	CITY           string
	ISLAND         string
	STATE_DISTRICT string
	STATE          string
	COUNTRY_REGION string
	COUNTRY        string
	WORLD_REGION   string
}

type NameParsed struct {
	FNAME  string
	LNAME  string
	SUFFIX string
}

type PostRecord struct {
	Type     string
	Sequence int
	Output   PeopleOutput
}

type PubQueue struct {
	Output PeopleOutput
	Suffix string
	Type   string
}

type PeopleSetDS struct {
	ID                     *datastore.Key `datastore:"__key__"`
	OwnerID                []string       `datastore:"ownerid"`
	Source                 []string       `datastore:"source"`
	EventID                []string       `datastore:"eventid"`
	EventType              []string       `datastore:"eventtype"`
	FiberType              []string       `datastore:"fibertype"`
	RecordID               []string       `datastore:"recordid"`
	RecordIDNormalized     []string       `datastore:"recordidnormalized"`
	CreatedAt              time.Time      `datastore:"createdat"`
	Fibers                 []string       `datastore:"fibers"`
	Search                 []string       `datastore:"search"`
	SALUTATION             []string       `datastore:"salutation"`
	SALUTATIONNormalized   []string       `datastore:"salutationnormalized"`
	NICKNAME               []string       `datastore:"nickname"`
	NICKNAMENormalized     []string       `datastore:"nicknamenormalized"`
	FNAME                  []string       `datastore:"fname"`
	FNAMENormalized        []string       `datastore:"fnamenormalized"`
	FINITIAL               []string       `datastore:"finitial"`
	FINITIALNormalized     []string       `datastore:"finitialnormalized"`
	LNAME                  []string       `datastore:"lname"`
	LNAMENormalized        []string       `datastore:"lnamenormalized"`
	MNAME                  []string       `datastore:"mname"`
	MNAMENormalized        []string       `datastore:"mnamenormalized"`
	AD1                    []string       `datastore:"ad1"`
	AD1Normalized          []string       `datastore:"ad1normalized"`
	AD1NO                  []string       `datastore:"ad1no"`
	AD1NONormalized        []string       `datastore:"ad1nonormalized"`
	AD2                    []string       `datastore:"ad2"`
	AD2Normalized          []string       `datastore:"ad2normalized"`
	AD3                    []string       `datastore:"ad3"`
	AD3Normalized          []string       `datastore:"ad3normalized"`
	AD4                    []string       `datastore:"ad4"`
	AD4Normalized          []string       `datastore:"ad4normalized"`
	CITY                   []string       `datastore:"city"`
	CITYNormalized         []string       `datastore:"citynormalized"`
	STATE                  []string       `datastore:"state"`
	STATENormalized        []string       `datastore:"statenormalized"`
	ZIP                    []string       `datastore:"zip"`
	ZIPNormalized          []string       `datastore:"zipnormalized"`
	ZIP5                   []string       `datastore:"zip5"`
	ZIP5Normalized         []string       `datastore:"zip5normalized"`
	COUNTRY                []string       `datastore:"country"`
	COUNTRYNormalized      []string       `datastore:"countrynormalized"`
	MAILROUTE              []string       `datastore:"mailroute"`
	MAILROUTENormalized    []string       `datastore:"mailroutenormalized"`
	ADTYPE                 []string       `datastore:"adtype"`
	ADTYPENormalized       []string       `datastore:"adtypenormalized"`
	ZIPTYPE                []string       `datastore:"ziptype"`
	ZIPTYPENormalized      []string       `datastore:"ziptypenormalized"`
	RECORDTYPE             []string       `datastore:"recordtype"`
	RECORDTYPENormalized   []string       `datastore:"recordtypenormalized"`
	ADBOOK                 []string       `datastore:"adbook"`
	ADBOOKNormalized       []string       `datastore:"adbooknormalized"`
	ADPARSER               []string       `datastore:"adparser"`
	ADPARSERNormalized     []string       `datastore:"adparsernormalized"`
	ADCORRECT              []string       `datastore:"adcorrect"`
	ADCORRECTNormalized    []string       `datastore:"adcorrectnormalized"`
	ADVALID                []string       `datastore:"advalid"`
	ADVALIDNormalized      []string       `datastore:"advalidnormalized"`
	EMAIL                  []string       `datastore:"email"`
	EMAILNormalized        []string       `datastore:"emailnormalized"`
	PHONE                  []string       `datastore:"phone"`
	PHONENormalized        []string       `datastore:"phonenormalized"`
	TRUSTEDID              []string       `datastore:"trustedid"`
	TRUSTEDIDNormalized    []string       `datastore:"trustedidnormalized"`
	CLIENTID               []string       `datastore:"clientid"`
	CLIENTIDNormalized     []string       `datastore:"clientidnormalized"`
	GENDER                 []string       `datastore:"gender"`
	GENDERNormalized       []string       `datastore:"gendernormalized"`
	AGE                    []string       `datastore:"age"`
	AGENormalized          []string       `datastore:"agenormalized"`
	DOB                    []string       `datastore:"dob"`
	DOBNormalized          []string       `datastore:"dobnormalized"`
	ORGANIZATION           []string       `datastore:"organization"`
	ORGANIZATIONNormalized []string       `datastore:"organizationnormalized"`
	TITLE                  []string       `datastore:"title"`
	TITLENormalized        []string       `datastore:"titlenormalized"`
	ROLE                   []string       `datastore:"role"`
	ROLENormalized         []string       `datastore:"rolenormalized"`
	STATUS                 []string       `datastore:"status"`
	STATUSNormalized       []string       `datastore:"statusnormalized"`
	PermE                  []string       `datastore:"perme"`
	PermENormalized        []string       `datastore:"permenormalized"`
	PermM                  []string       `datastore:"permm"`
	PermMNormalized        []string       `datastore:"permmnormalized"`
	PermS                  []string       `datastore:"perms"`
	PermSNormalized        []string       `datastore:"permsnormalized"`
}

type PeopleFiberDS struct {
	ID           *datastore.Key `datastore:"__key__"`
	CreatedAt    time.Time      `datastore:"createdat"`
	OwnerID      string         `datastore:"ownerid"`
	Source       string         `datastore:"source"`
	EventID      string         `datastore:"eventid"`
	EventType    string         `datastore:"eventtype"`
	RecordID     string         `datastore:"recordid"`
	FiberType    string         `datastore:"fibertype"`
	Disposition  string         `datastore:"disposition"`
	Search       []string       `datastore:"search"`
	Passthrough  []Passthrough  `datastore:"passthrough"`
	SALUTATION   MatchKeyField  `datastore:"salutation"`
	NICKNAME     MatchKeyField  `datastore:"nickname"`
	FNAME        MatchKeyField  `datastore:"fname"`
	FINITIAL     MatchKeyField  `datastore:"finitial"`
	LNAME        MatchKeyField  `datastore:"lname"`
	MNAME        MatchKeyField  `datastore:"mname"`
	AD1          MatchKeyField  `datastore:"ad1"`
	AD1NO        MatchKeyField  `datastore:"ad1no"`
	AD2          MatchKeyField  `datastore:"ad2"`
	AD3          MatchKeyField  `datastore:"ad3"`
	CITY         MatchKeyField  `datastore:"city"`
	STATE        MatchKeyField  `datastore:"state"`
	ZIP          MatchKeyField  `datastore:"zip"`
	ZIP5         MatchKeyField  `datastore:"zip5"`
	COUNTRY      MatchKeyField  `datastore:"country"`
	MAILROUTE    MatchKeyField  `datastore:"mailroute"`
	ADTYPE       MatchKeyField  `datastore:"adtype"`
	ZIPTYPE      MatchKeyField  `datastore:"ziptype"`
	RECORDTYPE   MatchKeyField  `datastore:"recordtype"`
	ADBOOK       MatchKeyField  `datastore:"adbook"`
	ADPARSER     MatchKeyField  `datastore:"adparser"`
	ADCORRECT    MatchKeyField  `datastore:"adcorrect"`
	ADVALID      MatchKeyField  `datastore:"advalid"`
	EMAIL        MatchKeyField  `datastore:"email"`
	PHONE        MatchKeyField  `datastore:"phone"`
	TRUSTEDID    MatchKeyField  `datastore:"trustedid"`
	CLIENTID     MatchKeyField  `datastore:"clientid"`
	GENDER       MatchKeyField  `datastore:"gender"`
	AGE          MatchKeyField  `datastore:"age"`
	DOB          MatchKeyField  `datastore:"dob"`
	ORGANIZATION MatchKeyField  `datastore:"organization"`
	TITLE        MatchKeyField  `datastore:"title"`
	ROLE         MatchKeyField  `datastore:"role"`
	STATUS       MatchKeyField  `datastore:"status"`
	PermE        MatchKeyField  `datastore:"perme"`
	PermM        MatchKeyField  `datastore:"permm"`
	PermS        MatchKeyField  `datastore:"perms"`
}

type Passthrough struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type PeopleDelete struct {
	OwnerID string   `json:"ownerId"`
	Expired []string `json:"expired"`
}

// NameValue stores name value pair
type NameValue struct {
	Name  string `json:"k,omitempty"`
	Value string `json:"v,omitempty"`
}
