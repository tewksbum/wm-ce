package wemade

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"time"

	"segment/db/datastore"
	"segment/models"
	"segment/utils"
	"segment/utils/logger"

	"github.com/google/uuid"
)

// Error messages
var (
	ErrDecodingRequest      string = "Error decoding request %#v"
	ErrInternalErrorOcurred string = "Internal error occurred %#v"
	ErrInvalidAccessKey     string = "Invalid access key"
	ErrAccountNotEnabled    string = "Account not enabled"
	ErrStatusNoContent      string = "Method Options: No content"
)

// Table names
var (
	tblDecode           string = "decode"
	tblEvent            string = "events"
	tblOrderHeader      string = "orderheaders"
	tblOrderDetail      string = "orderdetails"
	tblOrderConsignment string = "orderconsignments"
	tblHousehold        string = "households"
	tblProduct          string = "products"
	tblPeople           string = "people"
	tblShed             string = "shed"
	tblCampaign         string = "campaign"
	defPartitionField   string = "timestamp"
	dstblCustomers      string = "Customer"
	dsfilterCustomers   string = "AccessKey = "
)

// DecodeAPIInput serialize a json into a wemade.Request struct, checks the API key and
func DecodeAPIInput(projectID string, namespace string, body io.ReadCloser) (models.Record, error) {
	var input APIInput
	ctx := context.Background()
	surrogateID := uuid.New().String()
	if err := json.NewDecoder(body).Decode(&input); err != nil {
		return nil, logger.ErrFmt(ErrDecodingRequest, err)
	}

	dsClient, err := datastore.GetClient(&ctx, projectID)
	if err != nil {
		return nil, logger.ErrFmt(ErrInternalErrorOcurred, err)
	}
	query := datastore.QueryTableNamespace(dstblCustomers, namespace)
	query.Filter(dsfilterCustomers, input.AccessKey).Limit(1)

	var entities []DSCustomer

	if _, err := dsClient.GetAll(ctx, query, &entities); err != nil {
		return nil, logger.ErrFmt(ErrInternalErrorOcurred, err)
	}

	if len(entities) == 0 {
		return nil, logger.ErrStr(ErrInvalidAccessKey)
	}

	customer := entities[0]
	if customer.Enabled == false {
		return nil, logger.ErrStr(ErrAccountNotEnabled)
	}

	output := models.BaseRecord{
		EntityType:  input.EntityType,
		OwnerID:     customer.Key.ID,
		Owner:       input.Owner,
		Source:      input.Source,
		Passthrough: utils.FlattenMap(input.Passthrough),
		Attributes:  utils.FlattenMap(input.Attributes),
		Timestamp:   time.Now(),
	}

	entityType := strings.ToLower(input.EntityType)
	b, _ := json.Marshal(input.Data)

	switch entityType {
	case tblHousehold:
		output.DBopts = models.Options{
			Type:          models.BQ,
			IsPartitioned: true, PartitionField: defPartitionField,
		}
		record := models.Household{}
		json.Unmarshal(b, &record)
		return &models.HouseholdRecord{
			SurrogateID: surrogateID,
			BaseRecord:  output,
			Record:      record,
		}, nil
	case tblEvent:
		output.DBopts = models.Options{
			Type:          models.BQ,
			IsPartitioned: true, PartitionField: defPartitionField,
		}
		record := models.Event{}
		json.Unmarshal(b, &record)
		return &models.EventRecord{
			BaseRecord: output,
			Record:     record,
		}, nil
	case tblProduct:
		output.DBopts = models.Options{
			Type:          models.BQ,
			IsPartitioned: true, PartitionField: defPartitionField,
		}
		record := models.Product{}
		json.Unmarshal(b, &record)
		return &models.ProductRecord{
			SurrogateID: surrogateID,
			BaseRecord:  output,
			Record:      record,
		}, nil
	case tblPeople:
		output.DBopts = models.Options{
			Type:          models.BQ,
			IsPartitioned: true, PartitionField: defPartitionField,
		}
		record := models.People{}
		json.Unmarshal(b, &record)
		return &models.PeopleRecord{
			BaseRecord: output,
			Record:     record,
		}, nil
	case tblOrderHeader:
		output.DBopts = models.Options{
			Type:          models.BQ,
			IsPartitioned: true, PartitionField: defPartitionField,
		}
		record := models.OrderHeader{}
		json.Unmarshal(b, &record)
		return &models.OrderHeaderRecord{
			SurrogateID: surrogateID,
			BaseRecord:  output,
			Record:      record,
		}, nil
	case tblOrderConsignment:
		output.DBopts = models.Options{
			Type:          models.BQ,
			IsPartitioned: true, PartitionField: defPartitionField,
		}
		record := models.OrderConsignment{}
		json.Unmarshal(b, &record)
		return &models.OrderConsignmentRecord{
			SurrogateID: surrogateID,
			BaseRecord:  output,
			Record:      record,
		}, nil
	case tblOrderDetail:
		output.DBopts = models.Options{
			Type:          models.BQ,
			IsPartitioned: true, PartitionField: defPartitionField,
		}
		record := models.OrderDetail{}
		json.Unmarshal(b, &record)
		return &models.OrderDetailRecord{
			SurrogateID: surrogateID,
			BaseRecord:  output,
			Record:      record,
		}, nil
	case tblCampaign:
		output.DBopts = models.Options{
			Type:          models.BQ,
			IsPartitioned: true, PartitionField: defPartitionField,
		}
		record := models.Campaign{}
		json.Unmarshal(b, &record)
		return &models.CampaignRecord{
			SurrogateID: surrogateID,
			BaseRecord:  output,
			Record:      record,
		}, nil
	case tblDecode: // FCD table
		record := &models.DecodeRecord{}
		record.IDField = "signature"
		record.ColumnList = []string{"signature", "people_id"}
		record.ColumnBlackList = []string{"passthrough", "attributes", "source",
			"owner_id", "owner", "entity_type", "timestamp"}
		record.DBopts = models.Options{
			Type:              models.CSQL,
			IsPartitioned:     false,
			TableName:         tblDecode,
			IsTableNameSuffix: true,
		}
		json.Unmarshal(b, record)
		return record, nil
	default:
		dbyte, _ := json.Marshal(input.Data)
		output.DBopts = models.Options{
			Type:          models.BQ,
			IsPartitioned: true, PartitionField: defPartitionField,
		}
		// the Shed - shabby werehouse where any dummy requests die in.
		output.EntityType = tblShed
		return &models.FallbackRecord{
			SurrogateID: surrogateID,
			BaseRecord:  output,
			Record:      models.FallbackData{Data: string(dbyte)},
		}, nil
	}
}
