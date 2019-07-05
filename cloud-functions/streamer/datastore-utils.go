// Package streamer contains a series of cloud functions for streamer
package streamer

import "cloud.google.com/go/datastore"

// Profile is a dynamic map of the profile results
type Profile map[string]interface {
}

// Load a datastore field
func (d *Profile) Load(props []datastore.Property) error {
	// Note: you might want to clear current values from the map or create a new map
	for _, p := range props {
		(*d)[p.Name] = p.Value
	}
	return nil
}

// Save a datastore field
func (d *Profile) Save() (props []datastore.Property, err error) {
	for k, v := range *d {
		props = append(props, datastore.Property{Name: k, Value: v})
	}
	return
}
