package house720

import (
	"time"

	"cloud.google.com/go/datastore"
)

type PubSubMessage struct {
	Data []byte `json:"data"`
}

type FileReport struct {
	ID              string          `json:"id,omitempty"`
	ProcessingBegin time.Time       `json:"processingBegin,omitempty"`
	StatusLabel     string          `json:"statusLabel,omitempty"`
	StatusBy        string          `json:"statusBy,omitempty"`
	StatusTime      time.Time       `json:"statusTime,omitempty"`
	Errors          []ReportError   `json:"errors"`
	Warnings        []ReportError   `json:"warnings"`
	Counters        []ReportCounter `json:"counters"`
	SetList         []SetDetail     `json:"setList,omitempty"`
}

// SetDetail stores detail about a record
type SetDetail struct {
	ID         string    `json:"id,omitempty"`
	FiberCount int       `json:"fiberCount,omitempty"`
	CreatedOn  time.Time `json:"createdOn,omitempty"`
	DeletedOn  time.Time `json:"deletedOn,omitempty"`
	IsDeleted  bool      `json:"isDeleted,omitempty"`
	ReplacedBy string    `json:"replacedBy,omitempty"`
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

type FileComplete struct {
	EventID string `json:"eventId"`
	OwnerID string `json:"ownerId"`
}

type Signature struct {
	OwnerID   string `json:"ownerId"`
	Source    string `json:"source"`
	EventID   string `json:"eventId"`
	EventType string `json:"eventType"`
	FiberType string `json:"fiberType"`
	RecordID  string `json:"recordId"`
	FiberID   string `json:"id"`
}

type EventData struct {
	Signature   Signature              `json:"signature"`
	Passthrough map[string]string      `json:"passthrough"`
	Attributes  map[string]string      `json:"attributes"`
	EventData   map[string]interface{} `json:"eventData"`
}

type HouseInput struct {
	Signature   Signature         `json:"signature"`
	Passthrough map[string]string `json:"passthrough"`
	MatchKeys   HouseOutput       `json:"matchkeys"`
}

type HouseFiber struct {
	Signature Signature `json:"signature"`
	// Passthrough map[string]string `json:"passthrough"`
	Passthrough []Passthrough360 `json:"passthrough"`
	MatchKeys   HouseOutput      `json:"matchkeys"`
	ID          string           `json:"fiberId"`
	CreatedAt   time.Time        `json:"createdAt"`
}

type House360Input struct {
	Signature   Signature         `json:"signature"`
	Passthrough map[string]string `json:"passthrough"`
	MatchKeys   HouseOutput       `json:"matchkeys"`
}

type HouseFiberDSProjected struct {
	ID          *datastore.Key `datastore:"__key__"`
	Search      []string       `datastore:"search"`
	Disposition string         `datastore:"disposition"`
	FiberType   string         `datastore:"fibertype"`
}
type HouseFiberDS struct {
	ID           *datastore.Key   `datastore:"__key__"`
	CreatedAt    time.Time        `datastore:"createdat"`
	OwnerID      string           `datastore:"ownerid"`
	Source       string           `datastore:"source"`
	EventID      string           `datastore:"eventid"`
	EventType    string           `datastore:"eventtype"`
	RecordID     string           `datastore:"recordid"`
	FiberType    string           `datastore:"fibertype"`
	Disposition  string           `datastore:"disposition"`
	Search       []string         `datastore:"search"`
	Passthrough  []Passthrough360 `datastore:"passthrough"`
	AD1          MatchKeyField    `datastore:"ad1"`
	AD1NO        MatchKeyField    `datastore:"ad1no"`
	AD2          MatchKeyField    `datastore:"ad2"`
	AD3          MatchKeyField    `datastore:"ad3"`
	CITY         MatchKeyField    `datastore:"city"`
	STATE        MatchKeyField    `datastore:"state"`
	ZIP          MatchKeyField    `datastore:"zip"`
	ZIP5         MatchKeyField    `datastore:"zip5"`
	COUNTRY      MatchKeyField    `datastore:"country"`
	MAILROUTE    MatchKeyField    `datastore:"mailroute"`
	ADTYPE       MatchKeyField    `datastore:"adtype"`
	ZIPTYPE      MatchKeyField    `datastore:"ziptype"`
	RECORDTYPE   MatchKeyField    `datastore:"recordtype"`
	ADBOOK       MatchKeyField    `datastore:"adbook"`
	ADPARSER     MatchKeyField    `datastore:"adparser"`
	ADCORRECT    MatchKeyField    `datastore:"adcorrect"`
	ADVALID      MatchKeyField    `datastore:"advalid"`
	EMAIL        MatchKeyField    `datastore:"email"`
	TRUSTEDID    MatchKeyField    `datastore:"trustedid"`
	CLIENTID     MatchKeyField    `datastore:"clientid"`
	ORGANIZATION MatchKeyField    `datastore:"organization"`
	TITLE        MatchKeyField    `datastore:"title"`
	ROLE         MatchKeyField    `datastore:"role"`
	STATUS       MatchKeyField    `datastore:"status"`
	PermE        MatchKeyField    `datastore:"perme"`
	PermM        MatchKeyField    `datastore:"permm"`
	PermS        MatchKeyField    `datastore:"perms"`
}

type MatchKeyField struct {
	Value  string `json:"value"`
	Source string `json:"source"`
	Type   string `json:"type"`
}

type HouseOutput struct {
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
	ADBOOK       MatchKeyField `json:"adbook"`
	ADPARSER     MatchKeyField `json:"adparser"`
	ADCORRECT    MatchKeyField `json:"adcorrect"`
	ADVALID      MatchKeyField `json:"advalid"`
	ZIPTYPE      MatchKeyField `json:"ziptype"`
	RECORDTYPE   MatchKeyField `json:"recordtype"`
	EMAIL        MatchKeyField `json:"email"`
	TRUSTEDID    MatchKeyField `json:"trustedId"`
	CLIENTID     MatchKeyField `json:"clientId"`
	ORGANIZATION MatchKeyField `json:"organization"`
	TITLE        MatchKeyField `json:"title"`
	ROLE         MatchKeyField `json:"role"`
	STATUS       MatchKeyField `json:"status"`
	PermE        MatchKeyField `json:"perme"`
	PermM        MatchKeyField `json:"permm"`
	PermS        MatchKeyField `json:"perms"`
}

type Signature360 struct {
	OwnerID   string `json:"ownerId"`
	Source    string `json:"source"`
	EventID   string `json:"eventId"`
	EventType string `json:"eventType"`
}

type MatchKey360 struct {
	Key    string   `json:"key"`
	Type   string   `json:"type"`
	Value  string   `json:"value"`
	Values []string `json:"values"`
}

type Passthrough360 struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type House360Output struct {
	ID           string           `json:"id"`
	Signature    Signature360     `json:"signature"`
	Signatures   []Signature      `json:"signatures"`
	CreatedAt    time.Time        `json:"createdAt"`
	Fibers       []string         `json:"fibers"`
	ExpiredSets  []string         `json:"expiredSets"`
	Passthroughs []Passthrough360 `json:"passthroughs"`
	MatchKeys    []MatchKey360    `json:"matchKeys"`
}

type HouseSetDSProjected struct {
	ID     *datastore.Key `datastore:"__key__"`
	Search []string       `datastore:"search"`
}

type HouseSetDS struct {
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
	TRUSTEDID              []string       `datastore:"trustedid"`
	TRUSTEDIDNormalized    []string       `datastore:"trustedidnormalized"`
	CLIENTID               []string       `datastore:"clientid"`
	CLIENTIDNormalized     []string       `datastore:"clientidnormalized"`
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

type HouseGoldenDS struct {
	ID           *datastore.Key `datastore:"__key__"`
	CreatedAt    time.Time      `datastore:"createdat"`
	Search       []string       `datastore:"search"`
	AD1          string         `datastore:"ad1"`
	AD1NO        string         `datastore:"ad1no"`
	AD2          string         `datastore:"ad2"`
	AD3          string         `datastore:"ad3"`
	CITY         string         `datastore:"city"`
	STATE        string         `datastore:"state"`
	ZIP          string         `datastore:"zip"`
	ZIP5         string         `datastore:"zip5"`
	COUNTRY      string         `datastore:"country"`
	MAILROUTE    string         `datastore:"mailroute"`
	ADTYPE       string         `datastore:"adtype"`
	ZIPTYPE      string         `datastore:"ziptype"`
	RECORDTYPE   string         `datastore:"recordtype"`
	ADBOOK       string         `datastore:"adbook"`
	ADPARSER     string         `datastore:"adparser"`
	ADCORRECT    string         `datastore:"adcorrect"`
	ADVALID      string         `datastore:"advalid"`
	EMAIL        string         `datastore:"email"`
	TRUSTEDID    string         `datastore:"trustedid"`
	CLIENTID     string         `datastore:"clientid"`
	ORGANIZATION string         `datastore:"organization"`
	TITLE        string         `datastore:"title"`
	ROLE         string         `datastore:"role"`
	STATUS       string         `datastore:"status"`
	PermE        string         `datastore:"perme"`
	PermM        string         `datastore:"permm"`
	PermS        string         `datastore:"perms"`
}
