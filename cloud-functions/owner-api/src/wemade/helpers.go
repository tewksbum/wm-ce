package wemade

import (
	"context"
	"crypto/sha1"
	"encoding/json"
	"fmt"
	"segment/utils"
	"strings"

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

	filters := []DSFilter{
		DSFilter{
			Filter: FilterCustomersAccessKey,
			Value:  accessKey,
		},
	}
	owner, err := getCustomer(ctx, projID, namespace, filters, true)
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
	input.Customer.AccessKey = generateAccessKey(owner.AccessKey)
	if input.Customer.ID != nil || input.Customer.ExternalID != nil {
		filters = []DSFilter{
			DSFilter{
				Filter: FilterCustomersByKey,
				Value:  key,
			},
			// DSFilter{
			// 	Filter: FilterCustomersCreatedBy,
			// 	Value:  owner.Key,
			// },
		}
		c, err := getCustomer(ctx, projID, namespace, filters, true)
		if err != nil {
			if strings.Contains(err.Error(), fmt.Sprintf(ErrRecordNotFound, EntityCustomer)) {
				input.Customer.AccessKey = accessKey
				// logger.InfoFmt("input.Customer: %+v", input.Customer)
				if input.Customer.Name == "" {
					return nil, logger.ErrFmtStr(ErrInternalErrorOcurred, "Customer NAME is empty")
				}
			} else {
				return nil, logger.Err(err)
			}
		} else {
			if c.CreatedBy != owner.Key {
				return nil, logger.ErrFmtStr(ErrRecordNotCreatedBy, EntityCustomer)
			}
		}
		if c.AccessKey != "" {
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
		// Permissions storage
		if len(c.Permissions) > 0 {
			input.Customer.Permissions = append(input.Customer.Permissions, c.Permissions...)
		}
		// if c.CreatedBy == nil {
		// 	input.Customer.CreatedBy = owner.Key
		// }
	}
	input.Customer.CreatedBy = owner.Key
	input.Customer.Permissions = utils.SliceUniqMapStr(input.Customer.Permissions)
	var ps []string
	for _, p := range input.Customer.Permissions {
		for _, op := range owner.Permissions {
			if p == op {
				ps = append(ps, p)
			}
		}
	}
	// logger.InfoFmt("ps: %q", ps)
	input.Customer.Permissions = ps
	customer = input.Customer
	// logger.InfoFmt("key: %#v\ncustomer: %#v\nsrc: %#v", key, customer, customer)
	err = UpsertKind(ctx, projID, key, customer)
	return customer, err
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

func getCustomer(ctx context.Context, projID string, ns string, filters []DSFilter,
	checkEnabled bool) (c DatastoreCustomer, err error) {
	// Instance a datastore client
	dsClient, err := ds.GetClient(&ctx, projID)
	if err != nil {
		return c, logger.ErrFmt(ErrInternalErrorOcurred, err)
	}
	query := ds.QueryTablenamespace(EntityCustomer, ns)
	for _, f := range filters {
		query = query.Filter(f.Filter, f.Value)
	}

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
func UpsertKind(ctx context.Context, projID string, key *datastore.Key, src interface{}) error {
	dsClient, err := ds.GetClient(&ctx, projID)
	if err != nil {
		return logger.ErrFmt(ErrInternalErrorOcurred, err)
	}
	m := ds.UpsertMutation(key, src)
	keys, err := dsClient.Mutate(ctx, m)
	if err != nil {
		if err.Error() == "datastore: invalid entity type" {
			m = ds.InsertMutation(key, src)
			keys, err = dsClient.Mutate(ctx, m)
			if err != nil {
				return logger.ErrFmt("[UpsertKind.Insert] - "+ErrInternalErrorOcurred, err)
			}
		} else {
			return logger.ErrFmt("[UpsertKind.Upsert] - "+ErrInternalErrorOcurred, err)
		}
	}
	logger.InfoFmt("[UpsertKind] Stored key(s): %q", keys)
	return nil
}
