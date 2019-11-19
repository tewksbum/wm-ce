package wemade

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"

	"cloud.google.com/go/datastore"
	"github.com/google/uuid"

	ds "segment/db/datastore"
	"segment/utils/logger"
)

// UpsertCustomer serialize a json into a Request struct, checks the API key and
func UpsertCustomer(projID string, namespace string, data []byte) (customer *DatastoreCustomer, err error) {
	var input APIInput
	ctx := context.Background()

	logger.InfoFmt("RawInputJSON: %s", string(data))

	err = json.Unmarshal(data, &input)
	if err != nil {
		return customer, logger.ErrFmt(ErrDecodingRequest, err)
	}

	accessKey := input.AccessKey

	owner, err := getCustomer(ctx, projID, namespace, FilterCustomersAccessKey, accessKey, true)
	if err != nil || !owner.Enabled {
		return customer, err
	}

	if err := checkPermission(owner.Permissions, EntityCustomer, PermissionCreate); err != nil {
		return customer, err
	}

	if input.Customer == nil {
		return nil, logger.ErrFmtStr(ErrInternalErrorOcurred, "Customer is empty")
	}
	// if input.Customer.ExternalID == nil {
	// 	return nil, logger.ErrFmtStr(ErrInternalErrorOcurred, "ExternalID is not set")
	// }

	key := ds.BuildKey(EntityCustomer, namespace, input.Customer.ID, input.Customer.ExternalID)
	c, err := getCustomer(ctx, projID, namespace, FilterCustomersByKey, key, false)
	if err != nil && err == logger.ErrFmtStr(ErrRecordNotFound, EntityCustomer) {
		input.Customer.AccessKey = generateAccessKey(owner.AccessKey)
	}
	if c.AccessKey == "" {
		input.Customer.AccessKey = generateAccessKey(owner.AccessKey)
	} else {
		input.Customer.Updated = true
		input.Customer.AccessKey = c.AccessKey
		input.Customer.CreatedBy = c.CreatedBy
		if input.Customer.Name == "" {
			input.Customer.Name = c.Name
		}
		if input.Customer.Owner == "" {
			input.Customer.Owner = c.Owner
		}
	}
	if c.CreatedBy == nil {
		input.Customer.CreatedBy = owner.Key
	}
	err = upsertKind(ctx, projID, key, input.Customer)
	return input.Customer, err
}

func generateAccessKey(salty string) string {
	h := sha1.New()
	h.Write([]byte(uuid.New().String() + ":" + salty))
	return fmt.Sprintf("%x", h.Sum(nil))
}
func checkPermission(permissions []string, entity string, permission string) error {
	pc := buildPermission(entity, permission)
	for _, p := range permissions {
		if p == pc {
			return nil
		}
	}
	return logger.ErrFmtStr(ErrNoPermission, pc)
}

func buildPermission(entity string, permission string) string {
	return entity + ":" + permission
}

func getCustomer(ctx context.Context, projID string, ns string, filter string, filterKey interface{}, checkEnabled bool) (DatastoreCustomer, error) {
	var c DatastoreCustomer
	dsClient, err := ds.GetClient(&ctx, projID)
	if err != nil {
		return c, logger.ErrFmt(ErrInternalErrorOcurred, err)
	}
	query := ds.QueryTablenamespace(EntityCustomer, ns).
		Filter(filter, filterKey).Limit(1)

	var entities []DatastoreCustomer

	if _, err := dsClient.GetAll(ctx, query, &entities); err != nil {
		return c, logger.ErrFmt(ErrInternalErrorOcurred, err)
	}

	if len(entities) == 0 {
		return c, logger.ErrFmtStr(ErrRecordNotFound, EntityCustomer)
	}

	c = entities[0]
	if checkEnabled && c.Enabled == false {
		return c, logger.ErrStr(ErrAccountNotEnabled)
	}
	return c, nil
}

// UpsertKind upserts the kind data
func upsertKind(ctx context.Context, projID string, key *datastore.Key, src interface{}) error {
	dsClient, err := ds.GetClient(&ctx, projID)
	if err != nil {
		return logger.ErrFmt(ErrInternalErrorOcurred, err)
	}
	m := ds.UpsertMutation(key, src)
	keys, err := dsClient.Mutate(ctx, m)
	if err != nil {
		return logger.ErrFmt(ErrInternalErrorOcurred, err)
	}
	logger.InfoFmt("[upsertKind] Stored key(s): %q", keys)
	return nil
}
