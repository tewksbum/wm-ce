package filereport

import (
	"time"

	"cloud.google.com/go/datastore"
)

type reportRequest struct {
	EventID    string `json:"eventId"`
	Owner      string `json:"owner"`
	AccessKey  string `json:"accessKey"`
	CustomerID string `json:"customerId"`
	Bypass     string `json:"bypass"`
}

type elasticSecret struct {
	URL      string `json:"url"`
	User     string `json:"user"`
	Password string `json:"password"`
}

type customer struct {
	Name        string
	AccessKey   string
	Enabled     bool
	Owner       string
	Key         *datastore.Key `datastore:"__key__"`
	CreatedBy   *datastore.Key
	Permissions []string
}

type event struct {
	CustomerID  string
	Owner       string
	EventID     string
	EventType   string
	Source      string
	Status      string
	Message     string
	Created     time.Time
	Endpoint    string
	Passthrough string
	Attributes  string
	Detail      string
	RowLimit    int
}

type psMessage struct {
	Data       []byte            `json:"data"`
	Attributes map[string]string `json:"attributes"`
}
