package wemade

import (
	"cloud.google.com/go/datastore"
)

// APIInput input for the API
type APIInput struct {
	AccessKey    string            `json:"accessKey"`
	EntityType   string            `json:"entityType"` //events, order, product
	Organization string            `json:"organization"`
	Source       string            `json:"source"`
	Owner        string            `json:"owner"`
	Passthrough  map[string]string `json:"passthrough"`
	Attributes   map[string]string `json:"attributes"`
	Data         interface{}       `json:"data"`
}

// DSCustomer contains wemade Customer fields
type DSCustomer struct {
	Name      string
	AccessKey string
	Enabled   bool
	Key       *datastore.Key `datastore:"__key__"`
}

// APIOutput it's basic json that the API returns in response
type APIOutput struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}
