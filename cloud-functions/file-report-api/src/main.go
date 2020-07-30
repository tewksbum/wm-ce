package filereport

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"

	"cloud.google.com/go/datastore"

	"github.com/olivere/elastic/v7"

	secretmanager "cloud.google.com/go/secretmanager/apiv1beta1"
	secretmanagerpb "google.golang.org/genproto/googleapis/cloud/secretmanager/v1beta1"
)

var (
	test FileReport

	ctx context.Context

	esClient *elastic.Client

	dsClient *datastore.Client
	fsClient *datastore.Client

	smClient *secretmanager.Client

	esSecret elasticSecret
)

var projectID = os.Getenv("GCP_PROJECT")

func init() {
	var err error
	ctx = context.Background()

	smClient, err := secretmanager.NewClient(ctx)
	if err != nil {
		log.Fatalf("failed to setup secrets manager client: %v", err)
	}

	secretReq := &secretmanagerpb.AccessSecretVersionRequest{
		Name: os.Getenv("ELASTIC_SECRET"),
	}
	secretresult, err := smClient.AccessSecretVersion(ctx, secretReq)
	if err != nil {
		log.Fatalf("failed to get secret: %v", err)
	}
	secretsData1 := secretresult.Payload.Data
	if err := json.Unmarshal(secretsData1, &esSecret); err != nil {
		log.Fatalf("error decoding secrets %v", err)
		return
	}
	log.Printf("secret: %v", string(secretsData1))

	esClient, err = elastic.NewClient(
		elastic.SetURL(esSecret.URL),
		elastic.SetBasicAuth(esSecret.User, esSecret.Password),
		elastic.SetSniff(false),
	)
	if err != nil {
		log.Panicf("Error creating elastic client %v", err)
	}
}

// GetReport returns all information about an event stored in elastic
func GetReport(w http.ResponseWriter, r *http.Request) {
	var input reportRequest
	if r.Method == http.MethodOptions {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		w.Header().Set("Access-Control-Max-Age", "3600")
		w.WriteHeader(http.StatusNoContent)
		return
	}
	// Set CORS headers for the main request.
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Content-Type", "application/json")

	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{success: false, message: \"Error decoding request\"}")
		log.Fatalf("error decoding request %v", err)
		return
	}

	// TODO: remove this
	if input.Bypass == "@U1Q6TAy^QH,98y" { // secret to bypass check for testing
	} else {

		// first verify customer has valid credentials
		var customerLookup []customer
		customerQuery := datastore.NewQuery("Customer").Namespace(os.Getenv("DATASTORENS")).Filter("AccessKey =", input.AccessKey).Limit(1)
		if _, err := dsClient.GetAll(ctx, customerQuery, &customerLookup); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.Fatalf("Error querying customer: %v", err)
			fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -2\"}")
			return
		}
		if len(customerLookup) == 0 {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, "{success: false, message: \"Invalid access key, -10\"}")
			return
		}
		log.Printf("found %v customer matches: %v", len(customerLookup), customerLookup)
		customer := customerLookup[0]
		if customer.Enabled == false {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, "{success: false, message: \"Account is not enabled, -11\"}")
			return
		}

		if !strings.EqualFold(customer.Owner, input.Owner) {
			w.WriteHeader(http.StatusUnauthorized)
			fmt.Fprint(w, "{success: false, message: \"Invalid credentials, -9\"}")
			return
		}

		// next validate the request id belongs to the customer
		if input.EventID != "skip" {
			var eventLookup []event
			eventQuery := datastore.NewQuery("Event").Namespace(os.Getenv("DATASTORENS")).Filter("EventID =", input.EventID).Limit(1)
			if _, err := fsClient.GetAll(ctx, eventQuery, &eventLookup); err != nil {
				w.WriteHeader(http.StatusInternalServerError)
				log.Fatalf("Error querying customer: %v", err)
				fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -2\"}")
				return
			}
			if len(eventLookup) == 0 {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, "{success: false, message: \"Event not found, -20\"}")
				return
			}
			log.Printf("found %v event matches: %v", len(eventLookup), eventLookup)
			event := eventLookup[0]
			if !strings.EqualFold(event.CustomerID, input.CustomerID) {
				w.WriteHeader(http.StatusUnauthorized)
				fmt.Fprint(w, "{success: false, message: \"Event does not match to the customer id, -25\"}")
				return
			}
		}
	}

	// fetch the doc from elastic
	detail := strings.ToLower(r.URL.Query().Get("detail"))
	if detail == "1" || detail == "true" { // return complete doc
		getRequest := esClient.Get().
			Index(os.Getenv("REPORT_ESINDEX")).
			Id(input.EventID).
			Pretty(false)
		getResult, err := getRequest.Do(ctx)
		if err != nil {
			if err.Error() == "elastic: Error 404 (Not Found)" {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, "{success: false, message: \"Not found\"}")
				log.Printf("Not found: %v", input.EventID)
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -121\"}")
			log.Fatalf("Error fetching from elastic: %v", err)
		}
		if !getResult.Found {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, "{success: false, message: \"Not found\"}")
			log.Printf("Not found: %v", input.EventID)
		} else {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, string(getResult.Source))
		}
	} else if detail == "2" || detail == "true" { // return complete doc
		getRequest := esClient.Get().
			Index(os.Getenv("REPORT_ESINDEX")).
			Id(input.CustomerID).
			Pretty(false)
		getResult, err := getRequest.Do(ctx)
		if err != nil {
			if err.Error() == "elastic: Error 404 (Not Found)" {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, "{success: false, message: \"Not found\"}")
				log.Printf("Not found: %v", input.CustomerID)
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -121\"}")
			log.Fatalf("Error fetching from elastic: %v", err)
		}
		if !getResult.Found {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, "{success: false, message: \"Not found\"}")
			log.Printf("Not found: %v", input.CustomerID)
		} else {
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, string(getResult.Source))
		}
	} else {
		sourceFilter := elastic.NewFetchSourceContext(true).Include("counts", "id", "requestedAt", "processingBegin", "processingEnd", "customerId", "inputFileName", "statusLabel")
		getRequest := esClient.Get().
			Index(os.Getenv("REPORT_ESINDEX")).
			Id(input.EventID).
			FetchSourceContext(sourceFilter).
			Pretty(false)

		getResult, err := getRequest.Do(ctx)
		if err != nil {
			if err.Error() == "elastic: Error 404 (Not Found)" {
				w.WriteHeader(http.StatusNotFound)
				fmt.Fprint(w, "{success: false, message: \"Not found\"}")
				log.Printf("Not found: %v", input.EventID)
				return
			}
			w.WriteHeader(http.StatusInternalServerError)
			fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -121\"}")
			log.Fatalf("Error fetching from elastic: %v", err)
		}
		if !getResult.Found {
			w.WriteHeader(http.StatusNotFound)
			fmt.Fprint(w, "{success: false, message: \"Result not found\"}")
			log.Printf("Not found: %v", input.EventID)
		} else {
			var report FileReport
			err = json.Unmarshal([]byte(getResult.Source), &report)
			if err != nil {
				log.Printf("Error unmarshaling %v", err)
			}
			var cleaned []CounterGroup
			for _, counter := range report.Counts {
				newCounter := CounterGroup{
					Group: counter.Group,
					Items: []KeyCounter{},
				}
				for _, item := range counter.Items {
					if !strings.Contains(item.Key, ":") {
						newCounter.Items = append(newCounter.Items, item)
					}
				}
				cleaned = append(cleaned, newCounter)
			}
			report.Counts = cleaned
			js, _ := json.Marshal(report)
			w.WriteHeader(http.StatusOK)
			fmt.Fprint(w, string(js))
		}
	}
}
