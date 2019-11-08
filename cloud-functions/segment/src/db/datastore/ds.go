package datastore

import (
	"context"

	ds "cloud.google.com/go/datastore"
)

// GetClient checks if the user has permissions
func GetClient(ctx *context.Context, projectID string) (*ds.Client, error) {
	dsc, err := ds.NewClient(*ctx, projectID)
	if err != nil {
		return nil, err
	}
	return dsc, nil
}

// QueryTableNamespace builds a query for a table in a aspecific namespace
func QueryTableNamespace(table string, namespace string) *ds.Query {
	return ds.NewQuery(table).Namespace(namespace)
}
