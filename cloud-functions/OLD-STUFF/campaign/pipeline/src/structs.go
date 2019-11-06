package pipeline

// PubSubMessage is the payload of a pubsub event
type PubSubMessage struct {
	Data []byte `json:"data"`
}

// IdentifiedRecord struct
type IdentifiedRecord struct {
	CampaignID string `json:"CampaignId" bigquery:"campaignId"`
	Name       string `json:"Name" bigquery:"name"`
	Type       string `json:"Type" bigquery:"type"`
	Channel    string `json:"Channel" bigquery:"channel"`
	StartDate  string `json:"StartDate" bigquery:"startDate"`
	EndDate    string `json:"EndDate" bigquery:"endDate"`
	Budget     string `json:"Budget" bigquery:"budget"`
}

// InputERR struct defining the input columns
type InputERR struct {
	TrustedID  int `json:"TrustedID"`
	CampaignID int `json:"CampaignId"`
	Name       int `json:"Name"`
	Type       int `json:"Type"`
	Channel    int `json:"Channel"`
	StartDate  int `json:"StartDate"`
	EndDate    int `json:"EndDate"`
	Budget     int `json:"Budget"`
}

// InputColumn input column
type InputColumn struct {
	ERR   InputERR `json:"ERR"`
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

// OutputRecord the output result
type OutputRecord struct {
	TrustedID []OutputTrustedID `json:"TrustedId"`
	Owner     int64             `json:"Owner"`
	Source    string            `json:"Source"`
	Request   string            `json:"Request"`
	Row       int               `json:"Row"`
	TimeStamp string            `json:"TimeStamp"`
	Record    IdentifiedRecord  `json:"Record" bigquery:"campaign"`
}

// InputVER value regex
type InputVER struct {
	Hashcode int64
}
