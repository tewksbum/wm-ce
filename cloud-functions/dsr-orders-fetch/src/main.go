package dsrorderfetch

import (
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/dghubble/oauth1"

	"cloud.google.com/go/pubsub"
	"github.com/ybbus/httpretry"

	secretmanager "cloud.google.com/go/secretmanager/apiv1beta1"
	secretmanagerpb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1beta1"
)

var smClient *secretmanager.Client
var nsSecret secretsNS
var consumerKey string
var consumerSecret string
var tokenSecret string
var tokenKey string
var realm string
var ps *pubsub.Client
var topic *pubsub.Topic
var ctx context.Context

type secretsNS struct {
	ConsumerKey    string `json:"consumerKey"`
	ConsumerSecret string `json:"consumerSecret"`
	TokenKey       string `json:"tokenKey"`
	TokenSecret    string `json:"tokenSecret"`
	Realm          string `json:"realm"`
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

	consumerKey = nsSecret.ConsumerKey
	consumerSecret = nsSecret.ConsumerSecret
	tokenSecret = nsSecret.TokenSecret
	tokenKey = nsSecret.TokenKey
	realm = nsSecret.Realm

	ps, _ = pubsub.NewClient(ctx, os.Getenv("GCP_PROJECT"))
	topic = ps.Topic(os.Getenv("ORDER_PUBSUB"))
}

func Run(ctx context.Context, m *pubsub.Message) error {
	var ids []string
	if err := json.Unmarshal(m.Data, &ids); err != nil {
		log.Fatalf("Unable to unmarshal message %v with error %v", string(m.Data), err)
	}
	log.Printf("pulling orders %v", ids)
	config := oauth1.Config{
		ConsumerKey:    consumerKey,
		ConsumerSecret: consumerSecret,
		Realm:          realm,
		Signer:         &oauth1.HMAC256Signer{ConsumerSecret: consumerSecret},
	}
	token := oauth1.NewToken(tokenKey, tokenSecret)
	httpClient := config.Client(oauth1.NoContext, token)
	orderURL := fmt.Sprintf(os.Getenv("ORDERS_FETCH_URL"), strings.Join(ids, ","))
	req, _ := http.NewRequest("GET", orderURL, nil)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	client := httpretry.NewCustomClient(httpClient)
	jsonString := ""
	retryCount := 0
	for {
		resp, err := client.Do(req)
		if err != nil {
			log.Printf("FATAL ERROR Unable to send request to netsuite: error %v", err)
		}
		defer resp.Body.Close()

		body, err := ioutil.ReadAll(resp.Body)
		jsonString = string(body)
		if !strings.Contains(jsonString, "SSS_REQUEST_LIMIT_EXCEEDED") && !strings.Contains(jsonString, "INVALID_LOGIN_CREDENTIALS") && !strings.Contains(jsonString, "possible service interruptions") && len(jsonString) > 0 {
			break
		}
		retryCount++
		if retryCount > 5 {
			log.Fatalf("Unable to get message after 5 reties")
		}
	}
	// drop this to pubsub
	psresult := topic.Publish(ctx, &pubsub.Message{
		Data: []byte(jsonString),
	})
	_, err := psresult.Get(ctx)
	if err != nil {
		log.Printf("Error could not pub order exceptions to pubsub: %v", err)
	}

	return nil
}
