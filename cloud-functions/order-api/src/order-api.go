package orderapi

import (
	"context"
	"io/ioutil"
	"log"
	"net/http"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/google/uuid"
)

func SaveOrder(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	cfg := elasticsearch.Config{
		Addresses: []string{
			"http://104.198.136.122:9200",
		},
		Username: "elastic",
		Password: "TsLv8BtM",
	}
	es, err := elasticsearch.NewClient(cfg)
	docID := uuid.New().String()
	req := esapi.IndexRequest{
		Index:        "order",
		DocumentType: "record",
		DocumentID:   docID,
		Body:         r.Body,
		Refresh:      "true",
	}

	res, err := req.Do(ctx, es)
	if err != nil {
		log.Fatalf("Error getting response: %s", err)
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
