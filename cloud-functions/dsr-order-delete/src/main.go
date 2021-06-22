package dsrschool

import (
	"context"
	"database/sql"
	"encoding/json"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/dghubble/oauth1"
	// cloud sql
	_ "github.com/go-sql-driver/mysql"

	"cloud.google.com/go/pubsub"

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
var ctx context.Context
var db *sql.DB

type secretsNS struct {
	ConsumerKey    string `json:"consumerKey"`
	ConsumerSecret string `json:"consumerSecret"`
	TokenKey       string `json:"tokenKey"`
	TokenSecret    string `json:"tokenSecret"`
	Realm          string `json:"realm"`
}

type record struct {
	RecordType string `json:"recordType"`
	Values     struct {
		Name        string `json:"name"`
		DeletedDate string `json:"deleteddate"`
		DeletedAt   time.Time
	}
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

	secretReq = &secretmanagerpb.AccessSecretVersionRequest{
		Name: os.Getenv("DW_SECRET"),
	}
	secretresult, err = smClient.AccessSecretVersion(ctx, secretReq)
	if err != nil {
		log.Fatalf("failed to get secret: %v", err)
	}

	secretsData2 := secretresult.Payload.Data
	dsn := string(secretsData2)

	db, err = sql.Open("mysql", dsn)
	if err != nil {
		log.Printf("error opening db %v", err)
	}
}

func Run(ctx context.Context, m *pubsub.Message) error {
	NSDateTimeFormat := "1/2/2006 3:04 pm"

	listURL := os.Getenv("DELETEDORDER_LIST_URL")
	config := oauth1.Config{
		ConsumerKey:    consumerKey,
		ConsumerSecret: consumerSecret,
		Realm:          realm,
		Signer:         &oauth1.HMAC256Signer{ConsumerSecret: consumerSecret},
	}
	token := oauth1.NewToken(tokenKey, tokenSecret)

	httpClient := config.Client(oauth1.NoContext, token)
	req, _ := http.NewRequest("GET", listURL, nil)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	resp, err := httpClient.Do(req)
	if err != nil {
		log.Fatalf("FATAL ERROR Unable to send request to netsuite: error %v", err)
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	log.Printf("Received body %v", string(body))
	var input []record
	err = json.Unmarshal(body, &input)

	// err = json.NewDecoder(resp.Body).Decode(input)
	if err != nil {
		log.Printf("FATAL ERROR Unable to decode netsuite response: error %v", err)
	}
	if len(string(body)) > 10 {
		N := 20
		wg := new(sync.WaitGroup)
		sem := make(chan struct{}, N)
		log.Printf("Running delete for %v orders", len(input))
		for _, rec := range input {
			wg.Add(1)
			go func(rec record) {
				defer wg.Done()
				sem <- struct{}{}
				defer func() {
					// Reading from the channel decrements the semaphore
					// (frees up buffer slot).
					<-sem
				}()
				if strings.HasPrefix(rec.Values.Name, "Sales Order #") {
					rec.Values.DeletedAt, _ = time.Parse(NSDateTimeFormat, rec.Values.DeletedDate)
					log.Printf("Marking order %v as deleted", rec.Values.Name[13:])
					_, err := db.Exec("CALL sp_delete_order (?, ?)", rec.Values.Name[13:], rec.Values.DeletedAt)
					if err != nil {
						log.Printf("Error %v", err)
					}
				}
			}(rec)
		}
		wg.Wait()
	} else {
		log.Printf("Nothing to update")
	}
	return nil
}
