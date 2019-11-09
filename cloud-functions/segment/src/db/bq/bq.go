package bq

import (
	"context"
	"segment/models"
	"segment/utils/logger"
	"strings"

	"cloud.google.com/go/bigquery"
)

// Write writes the interface into BQ
func Write(projectID string, datasetID string, tableID string,
	isPartitioned bool, partitionField string, obj interface{}) (updated bool, err error) {
	ctx := context.Background()
	bqClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return updated, logger.Err(err)
	}
	recordSchema, err := bigquery.InferSchema(obj)
	if err != nil {
		return updated, logger.Err(err)
	}
	dsMetadata := &bigquery.DatasetMetadata{
		Name: datasetID,
	}
	recordMetadata := &bigquery.TableMetadata{
		Schema: recordSchema,
	}
	if isPartitioned {
		recordMetadata.TimePartitioning = &bigquery.TimePartitioning{
			Field: partitionField,
		}
	}
	dset := bqClient.Dataset(datasetID)
	if err := dset.Create(ctx, dsMetadata); err != nil {
		if !strings.Contains(err.Error(), "duplicate") &&
			!strings.Contains(err.Error(), "409") {
			return updated, logger.Fatal(err)
		}
	}
	recordTableRef := dset.Table(tableID)
	if err := recordTableRef.Create(ctx, recordMetadata); err != nil {
		if !strings.Contains(err.Error(), "409") {
			return updated, logger.Fatal(err)
		}
	}
	recordInserter := recordTableRef.Inserter()
	if err := recordInserter.Put(ctx, obj); err != nil {
		return updated, logger.ErrFmt("[BQ.Write.recordInserter] %#v", err)
	}
	return updated, nil
}

// Read the interfae from BQ
func Read(projectID string, datasetID string, tableID string, q []models.QueryFilter) (err error) {
	for _, filter := range q {
		logger.InfoFmt("filter: %#v", filter)
	}
	return err
}

// Delete the interfae from BQ
func Delete(projectID string, datasetID string, tableID string, q []models.QueryFilter) (err error) {
	for _, filter := range q {
		logger.InfoFmt("filter: %#v", filter)
	}
	return err
}
