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

// PubSubMessage is the payload of a Pub/Sub event.
type PubSubMessage struct {
	Data       []byte            `json:"data"`
	Attributes map[string]string `json:"attributes"`
}

// SweeperInput input for the Sweeper Segment API
type SweeperInput struct {
	AccessKey       string   `json:"accessKey"`
	EntityType      string   `json:"entityType"`
	EntityBlacklist []string `json:"entityBlacklist"`
}

// APIInput input for the Segment API
type APIInput struct {
	OwnerID      int64                   `json:"ownerId"`
	AccessKey    string                  `json:"accessKey"`
	EntityType   string                  `json:"entityType"`
	Source       string                  `json:"source"`
	Owner        string                  `json:"owner"`
	Organization string                  `json:"organization,omitifempty"`
	Signatures   []string                `json:"signatures,omitifempty"`
	EventIDs     []string                `json:"eventIds,omitifempty"`
	ExpiredSets  []string                `json:"expiredSets,omitifempty"`
	Passthrough  []models.Passthrough360 `json:"passthrough"`
	Attributes   map[string]string       `json:"attributes"`
	Filters      []models.QueryFilter    `json:"filters"`
	Columns      []string                `json:"columns"`
	WriteToOwner bool                    `json:"writeToOwner"`
	// Passthrough  map[string]string    `json:"passthrough"`
	// InputData    interface{}          `json:"inputData"`
}

// APIOutput basic json to return API responses
type APIOutput struct {
	Success bool          `json:"success"`
	Message string        `json:"message"`
	Records *OutputRecord `json:"records,omitempty"`
}

// OutputRecord the struct that will hold the records
type OutputRecord struct {
	List  []interface{} `json:"list"`
	Count int           `json:"count"`
}

// DatastoreCustomer contains wemade Customer fields
type DatastoreCustomer struct {
	Name        string
	Owner       string
	AccessKey   string
	Permissions []string
	CreatedBy   *datastore.Key
	Enabled     bool
	Key         *datastore.Key `datastore:"__key__"`
}
