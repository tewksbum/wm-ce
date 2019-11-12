package wemade

import (
	"segment/models"

	"cloud.google.com/go/datastore"
)

// Error messages
var (
	ErrDecodingRequest      = "Error decoding request %#v"
	ErrInternalErrorOcurred = "Internal error occurred %#v"
	ErrInvalidAccessKey     = "Invalid access key"
	ErrAccountNotEnabled    = "Account not enabled"
	ErrStatusNoContent      = "Method [%s] is not allowed"
)

// APIInput input for the Segment API
type APIInput struct {
	AccessKey    string               `json:"accessKey"`
	EntityType   string               `json:"entityType"`
	Organization string               `json:"organization"`
	Source       string               `json:"source"`
	OwnerID      int64                `json:"ownerId"`
	Owner        string               `json:"owner"`
	Passthrough  map[string]string    `json:"passthrough"`
	Attributes   map[string]string    `json:"attributes"`
	Filters      []models.QueryFilter `json:"filters"`
	// InputData         interface{}          `json:"inputData"`
}

// APIOutput basic json to return API responses
type APIOutput struct {
	Success bool          `json:"success"`
	Message string        `json:"message"`
	Records OutputRecords `json:"records"`
}

// OutputRecords the struct that will hold the records
type OutputRecords struct {
	List  []interface{} `json:"list"`
	Count int           `json:"count"`
}

// DatastoreCustomer contains wemade Customer fields
type DatastoreCustomer struct {
	Name        string
	Owner       string
	AccessKey   string
	Permissions string
	CreatedBy   *datastore.Key
	Enabled     bool
	Key         *datastore.Key `datastore:"__key__"`
}
