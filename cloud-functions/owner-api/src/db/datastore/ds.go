package ds

import (
	"context"

	"cloud.google.com/go/datastore"
)

// GetClient checks if the user has permissions
func GetClient(ctx *context.Context, projectID string) (*datastore.Client, error) {
	dsc, err := datastore.NewClient(*ctx, projectID)
	if err != nil {
		return nil, err
	}
	return dsc, nil
}

// QueryTablenamespace builds a query for a table in a aspecific namespace
func QueryTablenamespace(kind string, namespace string) *datastore.Query {
	return datastore.NewQuery(kind).Namespace(namespace)
}

// UpsertMutation builds an upsert mutation record
func UpsertMutation(k *datastore.Key, src interface{}) *datastore.Mutation {
	return datastore.NewUpsert(k, src)
}

// InsertMutation insert object in ds kind
func InsertMutation(k *datastore.Key, src interface{}) *datastore.Mutation {
	return datastore.NewInsert(k, src)
}

// BuildKey builds a ds key
func BuildKey(kind string, namespace string, id *int64, name *string) *datastore.Key {
	key := &datastore.Key{
		Kind:      kind,
		Namespace: namespace,
	}
	if id != nil {
		key.ID = *id
	}
	if name != nil {
		key.Name = *name
		key.ID = 0
	}
	return key
}
