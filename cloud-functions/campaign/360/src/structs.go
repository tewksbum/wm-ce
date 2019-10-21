package threesixty

// OutputTrustedID trusted id
type OutputTrustedID struct {
	Source   string `json:"Source"`
	SourceID string `json:"SourceId"`
}

// Record order record
type Record struct {
	TrustedID  string `json:"TrustedId" bigquery:"trustedid"`
	CampaignID string `json:"CampaignId" bigquery:"campaignId"`
	Name       string `json:"Name" bigquery:"name"`
	Type       string `json:"Type" bigquery:"type"`
	Channel    string `json:"Channel" bigquery:"channel"`
	StartDate  string `json:"StartDate" bigquery:"startDate"`
	EndDate    string `json:"EndDate" bigquery:"endDate"`
	Budget     string `json:"Budget" bigquery:"budget"`
}

// OutputRecord pipeline record struct
type OutputRecord struct {
	TrustedID []OutputTrustedID `json:"TrustedId"`
	Owner     int64             `json:"Owner"`
	Source    string            `json:"Source"`
	Request   string            `json:"Request"`
	Row       int               `json:"Row"`
	TimeStamp string            `json:"TimeStamp"`
	Campaign  Record            `json:"Record" bigquery:"campaign"`
}

// O3OutputRecord output record to bq
type O3OutputRecord struct {
	Campaign  Record `json:"Campaign" bigquery:"campaign"`
	Owner     int64  `json:"owner" bigquery:"owner"`
	Source    string `json:"source" bigquery:"source"`
	Request   string `json:"request" bigquery:"request"`
	Row       int    `json:"row" bigquery:"row"`
	TimeStamp string `json:"timestamp" bigquery:"timestamp"`
	SetID     string `json:"set" bigquery:"set"`
}

// PubSubMessage pubsub
type PubSubMessage struct {
	Data []byte `json:"data"`
}
