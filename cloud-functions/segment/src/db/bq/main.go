package bq

import (
	"context"
	"segment/utils/logger"
	"strings"

	"cloud.google.com/go/bigquery"
)

// Write writes the interface into BQ
func Write(projectID string, datasetID string, tableID string, isPartitioned bool, partitionField string, obj interface{}) error {
	ctx := context.Background()
	bqClient, err := bigquery.NewClient(ctx, projectID)
	if err != nil {
		return logger.Err(err)
	}
	recordSchema, err := bigquery.InferSchema(obj)
	if err != nil {
		return logger.Err(err)
	}

	recordMetaData := &bigquery.TableMetadata{
		Schema: recordSchema,
	}
	if isPartitioned {
		recordMetaData.TimePartitioning = &bigquery.TimePartitioning{
			Field: partitionField,
		}
	}
	recordTableRef := bqClient.Dataset(datasetID).Table(tableID)
	if err := recordTableRef.Create(ctx, recordMetaData); err != nil {
		//Add update if error is 409
		if !strings.ContainsAny(err.Error(), "409") {
			return logger.Fatal(err)
		}
	}
	recordInserter := recordTableRef.Inserter()
	if err := recordInserter.Put(ctx, obj); err != nil {
		return logger.ErrFmt("[BQ.Write.recordInserter] %#v", err)
	}
	return nil
}
