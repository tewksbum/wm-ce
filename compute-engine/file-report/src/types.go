package main

import (
	"sync"
	"time"

	"cloud.google.com/go/datastore"
	"github.com/olivere/elastic/v7"
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

// Bulker is an example for a process that needs to push data into
// Elasticsearch via BulkProcessor.
type Bulker struct {
	c       *elastic.Client
	p       *elastic.BulkProcessor
	workers int
	index   string

	beforeCalls  int64         // # of calls into before callback
	afterCalls   int64         // # of calls into after callback
	failureCalls int64         // # of successful calls into after callback
	successCalls int64         // # of successful calls into after callback
	seq          int64         // sequential id
	stopC        chan struct{} // stop channel for the indexer

	throttleMu sync.Mutex // guards the following block
	throttle   bool       // throttle (or stop) sending data into bulk processor?
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
