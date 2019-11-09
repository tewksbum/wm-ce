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

// Table and type names
var (
	typeDecode           = "decode"
	typeEvent            = "event"
	typeOrderHeader      = "orderheader"
	typeOrderDetail      = "orderdetail"
	typeOrderConsignment = "orderconsignment"
	typeHousehold        = "household"
	typeProduct          = "product"
	typePeople           = "people"
	typeCampaign         = "campaigns"
	tblDecode            = "decode"
	tblEvent             = "events"
	tblOrderHeader       = "orderheaders"
	tblOrderDetail       = "orderdetails"
	tblOrderConsignment  = "orderconsignments"
	tblHousehold         = "households"
	tblProduct           = "products"
	tblPeople            = "people"
	tblShed              = "shed"
	tblCampaign          = "campaigns"
	defPartitionField    = "timestamp"
	dstblCustomers       = "Customer"
	dsfilterCustomers    = "AccessKey = "
)

// "Decode" specific variables
var (
	decodeIDField    = "signature"
	decodeColumnList = []string{"signature", "people_id"}
	decodeBlackList  = []string{"passthrough", "attributes", "source",
		"owner_id", "owner", "entity_type", "timestamp"}
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

	br := models.BaseRecord{
		EntityType:  input.EntityType,
		OwnerID:     cust.Key.ID,
		Owner:       input.Owner,
		Source:      input.Source,
		Passthrough: utils.FlattenMap(input.Passthrough),
		Attributes:  utils.FlattenMap(input.Attributes),
		Timestamp:   time.Now(),
	}

	entityType := strings.ToLower(input.EntityType)

	switch entityType {
	case typeHousehold:
		br.DBopts = models.Options{
			Type:          models.BQ,
			TableName:     tblHousehold,
			Filters:       input.Filters,
			IsPartitioned: true, PartitionField: defPartitionField,
		}
		record := models.Household{}
		json.Unmarshal(data, &record)
		return &models.HouseholdRecord{
			SurrogateID: surrogateID,
			BaseRecord:  br,
			Record:      record,
		}, nil
	case typeEvent:
		br.DBopts = models.Options{
			Type:          models.BQ,
			TableName:     tblEvent,
			Filters:       input.Filters,
			IsPartitioned: true, PartitionField: defPartitionField,
		}
		record := models.Event{}
		json.Unmarshal(data, &record)
		return &models.EventRecord{
			BaseRecord: br,
			Record:     record,
		}, nil
	case typeProduct:
		br.DBopts = models.Options{
			Type:          models.BQ,
			TableName:     tblProduct,
			Filters:       input.Filters,
			IsPartitioned: true, PartitionField: defPartitionField,
		}
		record := models.Product{}
		json.Unmarshal(data, &record)
		return &models.ProductRecord{
			SurrogateID: surrogateID,
			BaseRecord:  br,
			Record:      record,
		}, nil
	case typePeople:
		br.DBopts = models.Options{
			Type:          models.BQ,
			TableName:     tblPeople,
			Filters:       input.Filters,
			IsPartitioned: true, PartitionField: defPartitionField,
		}
		record := models.People{}
		json.Unmarshal(data, &record)
		return &models.PeopleRecord{
			BaseRecord: br,
			Record:     record,
		}, nil
	case typeOrderHeader:
		br.DBopts = models.Options{
			Type:          models.BQ,
			TableName:     tblOrderHeader,
			Filters:       input.Filters,
			IsPartitioned: true, PartitionField: defPartitionField,
		}
		record := models.OrderHeader{}
		json.Unmarshal(data, &record)
		return &models.OrderHeaderRecord{
			SurrogateID: surrogateID,
			BaseRecord:  br,
			Record:      record,
		}, nil
	case typeOrderConsignment:
		br.DBopts = models.Options{
			Type:          models.BQ,
			TableName:     tblOrderConsignment,
			Filters:       input.Filters,
			IsPartitioned: true, PartitionField: defPartitionField,
		}
		record := models.OrderConsignment{}
		json.Unmarshal(data, &record)
		return &models.OrderConsignmentRecord{
			SurrogateID: surrogateID,
			BaseRecord:  br,
			Record:      record,
		}, nil
	case typeOrderDetail:
		br.DBopts = models.Options{
			Type:          models.BQ,
			TableName:     tblOrderDetail,
			Filters:       input.Filters,
			IsPartitioned: true, PartitionField: defPartitionField,
		}
		record := models.OrderDetail{}
		json.Unmarshal(data, &record)
		return &models.OrderDetailRecord{
			SurrogateID: surrogateID,
			BaseRecord:  br,
			Record:      record,
		}, nil
	case typeCampaign:
		br.DBopts = models.Options{
			Type:          models.BQ,
			TableName:     tblCampaign,
			Filters:       input.Filters,
			IsPartitioned: true, PartitionField: defPartitionField,
		}
		record := models.Campaign{}
		json.Unmarshal(data, &record)
		return &models.CampaignRecord{
			SurrogateID: surrogateID,
			BaseRecord:  br,
			Record:      record,
		}, nil
	case typeDecode: // FCD table
		record := &models.DecodeRecord{}
		json.Unmarshal(data, record)
		record.BaseRecord = br
		record.IDField = decodeIDField
		record.ColumnList = decodeColumnList
		record.ColumnBlackList = decodeBlackList
		record.DBopts = models.Options{
			Type:              models.CSQL,
			TableName:         tblDecode,
			Filters:           input.Filters,
			IsPartitioned:     false,
			IsTableNameSuffix: true,
		}
		return record, nil
	default:
		br.DBopts = models.Options{
			Type:          models.BQ,
			TableName:     tblShed,
			Filters:       input.Filters,
			IsPartitioned: true, PartitionField: defPartitionField,
		}
		// the Shed - shabby werehouse where any dummy requests die in.
		br.EntityType = tblShed
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
	query := datastore.QueryTableNamespace(dstblCustomers, ns).
		Filter(dsfilterCustomers, aKey).Limit(1)

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
