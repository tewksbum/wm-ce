// Package streamer contains a series of cloud functions for streamer
package streamer

import "cloud.google.com/go/datastore"

// Customer contains Customer fields
type Customer struct {
	Name      string
	AccessKey string
	Enabled   bool
	Key       *datastore.Key `datastore:"__key__"`
}
