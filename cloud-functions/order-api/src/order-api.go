package orderapi

import (
	"bytes"
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
)

// SaveOrder Saves the order
func SaveOrder(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	b, err := ioutil.ReadAll(r.Body)
	if err != nil {
		log.Fatalf("[SaveOrder] err: %+v", err)
	}
	var input map[string]interface{}
	if err := json.Unmarshal(b, &input); err != nil {
		log.Fatalf("[SaveOrder]: Not a valid JSON. err: %s", err)
	}
	esLog(&ctx, bytes.NewReader(b), "order", "record", "true")
	parseOrderJSON(string(b))
}
