package dsrorderlist

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"

	"cloud.google.com/go/pubsub"
	"github.com/ybbus/httpretry"

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
		Name: os.Getenv("NETSUITE_SECRET"),
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

	ps, _ = pubsub.NewClient(ctx, os.Getenv("GCP_PROJECT"))
	topic = ps.Topic(os.Getenv("ORDER_FETCH_PUBSUB"))
}

func Run(ctx context.Context, m *pubsub.Message) error {
	log.Printf("getting list")
	listURL := os.Getenv("ORDERS_LIST_URL")
	req, _ := http.NewRequest("GET", listURL, nil)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", nsAuth)

	client := httpretry.NewDefaultClient()
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

	log.Printf("distributing %v orders for fetching", len(input.Records))
	for i := 0; i < len(input.Records); i += 30 {
		ids := []string{}
		e := i + 30
		if e > len(input.Records) {
			e = len(input.Records)
		}
		for _, r := range input.Records[i:e] {
			if len(r.ID) > 0 {
				ids = append(ids, r.ID)
			}
		}
		log.Printf("pubbing %v orders", len(ids))
		data, _ := json.Marshal(ids)
		psresult := topic.Publish(ctx, &pubsub.Message{
			Data: data,
		})
		_, err = psresult.Get(ctx)
		if err != nil {
			log.Printf("Error could not pub order exceptions to pubsub: %v", err)
		}	
	}

	return nil
}
