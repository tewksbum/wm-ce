package wemade

import (
	"cloud.google.com/go/datastore"
)

// APIInput input for the Segment API
type APIInput struct {
	AccessKey string             `json:"accessKey"`
	Customer  *DatastoreCustomer `json:"customer,omitifempty"`
}

// APIOutput basic json to return API responses
type APIOutput struct {
	Success bool        `json:"success"`
	Message string      `json:"message"`
	Result  interface{} `json:"result,omitempty"`
}

// DatastoreCustomer contains wemade Customer fields
type DatastoreCustomer struct {
	Name        string         `json:"name"`
	ID          *int64         `json:"id" datastore:"-"`
	ExternalID  *string        `json:"externalId" datastore:"-"`
	Owner       string         `json:"owner"`
	AccessKey   string         `json:"accessKey"`
	Enabled     bool           `json:"enabled"`
	Permissions []string       `json:"permissions"`
	CreatedBy   *datastore.Key `json:"createdBy"`
	Key         *datastore.Key `json:"key" datastore:"__key__"`
	Updated     bool           `json:"-" datastore:"-"`
}
