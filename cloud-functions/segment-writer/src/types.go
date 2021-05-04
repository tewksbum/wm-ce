package segmentwriter

import (
	"time"

	"cloud.google.com/go/datastore"
)


type dbSecrets struct {
	MySQLDSN string `json:"mysql"`
}

type PubSubMessage struct {
	Data       []byte            `json:"data"`
	Attributes map[string]string `json:"attributes"`
}

type PeopleGoldenDS struct {
	ID           *datastore.Key `datastore:"__key__"`
	CreatedAt    time.Time      `datastore:"createdat"`
	Search       []string       `datastore:"search"`
	SALUTATION   string         `datastore:"salutation"`
	NICKNAME     string         `datastore:"nickname"`
	FNAME        string         `datastore:"fname"`
	FINITIAL     string         `datastore:"finitial"`
	LNAME        string         `datastore:"lname"`
	MNAME        string         `datastore:"mname"`
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
	PHONE        string         `datastore:"phone"`
	TRUSTEDID    string         `datastore:"trustedid"`
	CLIENTID     string         `datastore:"clientid"`
	GENDER       string         `datastore:"gender"`
	AGE          string         `datastore:"age"`
	DOB          string         `datastore:"dob"`
	ORGANIZATION string         `datastore:"organization"`
	TITLE        string         `datastore:"title"`
	ROLE         string         `datastore:"role"`
	STATUS       string         `datastore:"status"`
	PermE        string         `datastore:"perme"`
	PermM        string         `datastore:"permm"`
	PermS        string         `datastore:"perms"`
}

type GoldenOutput struct {
	ID string `json:"id"`
	Sponsor string `json:"sponsor"`
	Signatures   []Signature      `json:"signatures"`
	People PeopleGoldenDS `json:"people"`
	ExternalIDs map[string][]string `json:"externalIds"` // for this map, key is the external source such as netsuite, value is a list of IDs
	ExpiredIDs  []string         `json:"expiredIds"`
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
