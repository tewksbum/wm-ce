package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync"

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
	log.Printf("getting list")
	listURL := "https://3312248.restlets.api.netsuite.com/app/site/hosting/restlet.nl?script=819&deploy=1&searchId=customsearch_wm_sales_orders_streaming"
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

	N := 9
	wg := new(sync.WaitGroup)
	sem := make(chan struct{}, N)
	for i := 0; i < len(input.Records); i += 10 {
		ids := []string{}
		for _, r := range input.Records[i:i+10] {
			if len(r.ID) > 0 {
				ids = append(ids, r.ID)
			}
		}
		wg.Add(1)
		go func(ids []string) {
			defer wg.Done()
			sem <- struct{}{}
			defer func() {
				// Reading from the channel decrements the semaphore
				// (frees up buffer slot).
				<-sem
			}()
			log.Printf("pulling orders %v", ids)
			orderURL := fmt.Sprintf("https://3312248.restlets.api.netsuite.com/app/site/hosting/restlet.nl?script=825&deploy=1&orders=%v", strings.Join(ids, ","))
			req, _ := http.NewRequest("GET", orderURL, nil)
			req.Header.Set("Content-Type", "application/json")
			req.Header.Set("Accept", "application/json")
			req.Header.Set("Authorization", nsAuth)
	
			json := ""
			for {
				resp, err := client.Do(req)
				if err != nil {
					log.Printf("FATAL ERROR Unable to send request to netsuite: error %v", err)
				}
				defer resp.Body.Close()
		
				body, err := ioutil.ReadAll(resp.Body)
				json = string(body)
				if !strings.Contains(json, "SSS_REQUEST_LIMIT_EXCEEDED") && len(json) > 0 {
					break
				}
			}
			// drop this to pubsub
			psresult := topic.Publish(ctx, &pubsub.Message{
				Data: []byte(json),
			})
			_, err = psresult.Get(ctx)
			if err != nil {
				log.Printf("Error could not pub order exceptions to pubsub: %v", err)
			}
		}(ids)		
	}
	wg.Wait()
	return
}
