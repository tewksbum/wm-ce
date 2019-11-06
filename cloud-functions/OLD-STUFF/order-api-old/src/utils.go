package orderapi

import (
	"bytes"
	"context"
	"io/ioutil"
	"log"

	"cloud.google.com/go/pubsub"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/google/uuid"
)

var (
	// These should be ENV vars
	esAddress   = "http://104.198.136.122:9200"
	esUser      = "elastic"
	esPassword  = "TsLv8BtM"
	pubsubTopic = "streamer-output-dev" //"streamer-output-prod"
	outputTopic = "order-output-dev"    // "order-output-prod"
	projectID   = "wemade-core"
)

func esLog(ctx *context.Context, body *bytes.Reader, index string, doctype string, refresh string) {
	cfg := elasticsearch.Config{
		Addresses: []string{
			esAddress,
		},
		Username: esUser,
		Password: esPassword,
	}
	es, err := elasticsearch.NewClient(cfg)
	docID := uuid.New().String()
	req := esapi.IndexRequest{
		Index:        index,
		DocumentType: doctype,
		DocumentID:   docID,
		Body:         body,
		Refresh:      refresh,
	}

	res, err := req.Do(*ctx, es)
	if err != nil {
		log.Printf("Error getting response: %s", err)
	}
	defer res.Body.Close()

	if res.IsError() {
		resB, _ := ioutil.ReadAll(res.Body)
		log.Printf("[%s] Error indexing document ID=%v, Message=%v", res.Status(), docID, string(resB))
	} else {
		resB, _ := ioutil.ReadAll(res.Body)
		log.Printf("[%s] document ID=%v, Message=%v", res.Status(), docID, string(resB))
	}
}

func pubMessage(msg []byte) error {
	ctx := context.Background()
	psclient, err := pubsub.NewClient(ctx, projectID)
	if err != nil {
		log.Fatalf("Could not create pubsub Client: %v", err)
		return err
	}
	pstopic := psclient.Topic(outputTopic)
	psresult := pstopic.Publish(ctx, &pubsub.Message{
		Data: msg,
	})
	psid, err := psresult.Get(ctx)
	if err != nil {
		log.Printf("Could not pub to pubsub: %v", err)
		return err
	}
	log.Printf("Published record message id %v", psid)
	return nil
}
