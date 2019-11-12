package wemade

import (
	"context"
	"encoding/json"
	"io"
	"io/ioutil"
	"strings"
	"time"

	"segment/db/datastore"
	"segment/models"
	"segment/utils"
	"segment/utils/logger"

	"github.com/google/uuid"
)

// BuildRecordFromInput serialize a json into a Request struct, checks the API key and
func BuildRecordFromInput(projectID string, namespace string, body io.ReadCloser) (models.Record, error) {
	var input APIInput
	ctx := context.Background()
	surrogateID := uuid.New().String()
	data, err := ioutil.ReadAll(body)
	if err != nil {
		return nil, logger.ErrFmt(ErrDecodingRequest, err)
	}
	json.Unmarshal(data, &input)
	if err != nil {
		return nil, logger.ErrFmt(ErrDecodingRequest, err)
	}

	cust, err := validateCustomer(ctx, projectID, namespace, input.AccessKey)
	if err != nil {
		return nil, err
	}

	owner := cust.Owner
	if owner == "" {
		owner = input.Owner
		if owner == "" {
			owner = cust.Name
		}
	}
	br := models.BaseRecord{
		EntityType:  input.EntityType,
		OwnerID:     cust.Key.ID,
		Owner:       owner,
		Source:      input.Source,
		Passthrough: utils.FlattenMap(input.Passthrough),
		Attributes:  utils.FlattenMap(input.Attributes),
		Timestamp:   time.Now(),
	}

	entityType := strings.ToLower(input.EntityType)

	switch entityType {
	case models.TypeHousehold:
		br.DBopts = models.Options{
			Type:          models.BQ,
			TableName:     models.TblHousehold,
			Filters:       input.Filters,
			IsPartitioned: true, PartitionField: models.DefPartitionField,
		}
		record := models.Household{}
		json.Unmarshal(data, &record)
		return &models.HouseholdRecord{
			SurrogateID: surrogateID,
			BaseRecord:  br,
			Record:      record,
		}, nil
	case models.TypeEvent:
		br.DBopts = models.Options{
			Type:          models.BQ,
			TableName:     models.TblEvent,
			Filters:       input.Filters,
			IsPartitioned: true, PartitionField: models.DefPartitionField,
		}
		record := models.Event{}
		json.Unmarshal(data, &record)
		return &models.EventRecord{
			BaseRecord: br,
			Record:     record,
		}, nil
	case models.TypeProduct:
		br.DBopts = models.Options{
			Type:          models.BQ,
			TableName:     models.TblProduct,
			Filters:       input.Filters,
			IsPartitioned: true, PartitionField: models.DefPartitionField,
		}
		record := models.Product{}
		json.Unmarshal(data, &record)
		return &models.ProductRecord{
			SurrogateID: surrogateID,
			BaseRecord:  br,
			Record:      record,
		}, nil
	case models.TypePeople:
		br.DBopts = models.Options{
			Type:          models.BQ,
			TableName:     models.TblPeople,
			Filters:       input.Filters,
			IsPartitioned: true, PartitionField: models.DefPartitionField,
		}
		record := models.People{}
		json.Unmarshal(data, &record)
		return &models.PeopleRecord{
			BaseRecord: br,
			Record:     record,
		}, nil
	case models.TypeOrderHeader:
		br.DBopts = models.Options{
			Type:          models.BQ,
			TableName:     models.TblOrderHeader,
			Filters:       input.Filters,
			IsPartitioned: true, PartitionField: models.DefPartitionField,
		}
		record := models.OrderHeader{}
		json.Unmarshal(data, &record)
		return &models.OrderHeaderRecord{
			SurrogateID: surrogateID,
			BaseRecord:  br,
			Record:      record,
		}, nil
	case models.TypeOrderConsignment:
		br.DBopts = models.Options{
			Type:          models.BQ,
			TableName:     models.TblOrderConsignment,
			Filters:       input.Filters,
			IsPartitioned: true, PartitionField: models.DefPartitionField,
		}
		record := models.OrderConsignment{}
		json.Unmarshal(data, &record)
		return &models.OrderConsignmentRecord{
			SurrogateID: surrogateID,
			BaseRecord:  br,
			Record:      record,
		}, nil
	case models.TypeOrderDetail:
		br.DBopts = models.Options{
			Type:          models.BQ,
			TableName:     models.TblOrderDetail,
			Filters:       input.Filters,
			IsPartitioned: true, PartitionField: models.DefPartitionField,
		}
		record := models.OrderDetail{}
		json.Unmarshal(data, &record)
		return &models.OrderDetailRecord{
			SurrogateID: surrogateID,
			BaseRecord:  br,
			Record:      record,
		}, nil
	case models.TypeCampaign:
		br.DBopts = models.Options{
			Type:          models.BQ,
			TableName:     models.TblCampaign,
			Filters:       input.Filters,
			IsPartitioned: true, PartitionField: models.DefPartitionField,
		}
		record := models.Campaign{}
		json.Unmarshal(data, &record)
		return &models.CampaignRecord{
			SurrogateID: surrogateID,
			BaseRecord:  br,
			Record:      record,
		}, nil
	case models.TypeDecode: // FCD table
		record := &models.DecodeRecord{}
		json.Unmarshal(data, record)
		record.BaseRecord = br
		record.IDField = models.DecodeIDField
		record.ColumnList = models.DecodeColumnList
		record.ColumnBlackList = models.DecodeBlackList
		record.DBopts = models.Options{
			Type:              models.CSQL,
			TableName:         models.TblDecode,
			Filters:           input.Filters,
			IsPartitioned:     false,
			IsTableNameSuffix: true,
		}
		return record, nil
	default:
		br.DBopts = models.Options{
			Type:          models.BQ,
			TableName:     models.TblShed,
			Filters:       input.Filters,
			IsPartitioned: true, PartitionField: models.DefPartitionField,
		}
		// the Shed - shabby werehouse where any dummy requests die in.
		br.EntityType = input.EntityType
		return &models.FallbackRecord{
			SurrogateID: surrogateID,
			BaseRecord:  br,
			Record:      models.FallbackData{Data: string(data)},
		}, nil
	}
}

func validateCustomer(ctx context.Context, pID string, ns string, aKey string) (DatastoreCustomer, error) {
	var c DatastoreCustomer
	dsClient, err := datastore.GetClient(&ctx, pID)
	if err != nil {
		return c, logger.ErrFmt(ErrInternalErrorOcurred, err)
	}
	query := datastore.QueryTableNamespace(models.DsTblCustomers, ns).
		Filter(models.DsFilterCustomers, aKey).Limit(1)

	var entities []DatastoreCustomer

	if _, err := dsClient.GetAll(ctx, query, &entities); err != nil {
		return c, logger.ErrFmt(ErrInternalErrorOcurred, err)
	}

	if len(entities) == 0 {
		return c, logger.ErrStr(ErrInvalidAccessKey)
	}

	c = entities[0]
	if c.Enabled == false {
		return c, logger.ErrStr(ErrAccountNotEnabled)
	}
	return c, nil
}
