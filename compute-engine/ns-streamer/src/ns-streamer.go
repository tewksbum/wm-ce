package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"

	"cloud.google.com/go/pubsub"

	secretmanager "cloud.google.com/go/secretmanager/apiv1beta1"
	secretmanagerpb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1beta1"
)

var smClient *secretmanager.Client
var nsSecret secretsNS
var nsAuth string
var ps *pubsub.Client
var topic *pubsub.Topic
var ctx context.Context

type secretsNS struct {
	NSAuth string `json:"nsauth"`
}

type result struct {
	Records []record `json:"result"`
}

type record struct {
	ID string `json:"id"`
}

func init() {
	ctx = context.Background()
	smClient, err := secretmanager.NewClient(ctx)
	if err != nil {
		log.Fatalf("failed to setup client: %v", err)
	}

	secretReq := &secretmanagerpb.AccessSecretVersionRequest{
		Name: "projects/180297787522/secrets/netsuite/versions/1",
	}
	secretresult, err := smClient.AccessSecretVersion(ctx, secretReq)
	if err != nil {
		log.Fatalf("failed to get secret: %v", err)
	}
	secretsData1 := secretresult.Payload.Data
	if err := json.Unmarshal(secretsData1, &nsSecret); err != nil {
		log.Fatalf("error decoding secrets %v", err)
		return
	}

	nsAuth = nsSecret.NSAuth

	ps, _ = pubsub.NewClient(ctx, "wemade-core")
	topic = ps.Topic("wm-order-intake")
}

func main() {
	log.Printf("starting")
	listURL := "https://3312248.restlets.api.netsuite.com/app/site/hosting/restlet.nl?script=819&deploy=1&searchId=customsearch_wm_sales_orders_streaming"
	req, _ := http.NewRequest("GET", listURL, nil)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", nsAuth)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Fatalf("FATAL ERROR Unable to send request to netsuite: error %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)

	var input result
	err = json.Unmarshal(body, &input)

	// err = json.NewDecoder(resp.Body).Decode(input)
	if err != nil {
		log.Printf("FATAL ERROR Unable to decode netsuite response: error %v", err)
	}

	for i, record := range input.Records {
		if i < 10 {
			orderURL := fmt.Sprintf("https://3312248.restlets.api.netsuite.com/app/site/hosting/restlet.nl?script=821&deploy=1&id=%v", record.ID)
			req, _ := http.NewRequest("GET", orderURL, nil)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Accept", "application/json")
			req.Header.Set("Authorization", nsAuth)

			client := &http.Client{}
			resp, err := client.Do(req)
			if err != nil {
				log.Fatalf("FATAL ERROR Unable to send request to netsuite: error %v", err)
			}
			defer resp.Body.Close()

			body, err := ioutil.ReadAll(resp.Body)
			// drop this to pubsub
			log.Printf("%v", record.ID)
			psresult := topic.Publish(ctx, &pubsub.Message{
				Data: body,
			})
			_, err = psresult.Get(ctx)
			if err != nil {
				log.Printf("Error could not pub order exceptions to pubsub: %v", err)
			}

		}
	}

}
