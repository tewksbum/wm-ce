// Package streamer contains a series of cloud functions for streamer
package streamer

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"time"

	"cloud.google.com/go/datastore"
	"cloud.google.com/go/storage"
	"github.com/google/uuid"
)

// Request contains a record for the request
type Request struct {
	CustomerID   int64
	RequestID    string
	Organization string
	Source       string
	FetchURL     string
	FilePath     string
	Status       string
	SubmittedAt  time.Time
	ProcessedAt  time.Time
}

// NameSpace of the entity types used by streamer api
const NameSpace = "wemade.streamer"

// BucketName the GS storage bucket name
const BucketName = "streamer-upload"

// UploadFile is the API body
func UploadFile(w http.ResponseWriter, r *http.Request) {
	var d struct {
		AccessKey    string `json:"accessKey"`
		FileURL      string `json:"fileUrl"`
		Organization string `json:"organization"`
		Source       string `json:"source"`
	}

	ctx := context.Background()

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

	if err := json.NewDecoder(r.Body).Decode(&d); err != nil {
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprint(w, "{success: false, message: \"Error decoding request\"}")
		return
	}
	requestID := uuid.New()
	log.Printf("request assigned with id %v", requestID)

	// validate key
	dsClient, err := datastore.NewClient(ctx, ProjectID)
	if err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatalf("Error accessing datastore: %v", err)
		fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -1\"}")
		return
	}
	var entities []Customer
	query := datastore.NewQuery("Customer").Namespace(NameSpace)
	query.Filter("AccessKey =", d.AccessKey).Limit(1)

	if _, err := dsClient.GetAll(ctx, query, &entities); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatalf("Error querying customer: %v", err)
		fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -2\"}")
		return
	}
	if len(entities) == 0 {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, "{success: false, message: \"Invalid access key, -10\"}")
		return
	}

	customer := entities[0]
	if customer.Enabled == false {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, "{success: false, message: \"Account is not enabled, -11\"}")
		return
	}

	// log the request
	request := &Request{
		CustomerID:   customer.Key.ID,
		Status:       "Submitted",
		SubmittedAt:  time.Now(),
		FetchURL:     d.FileURL,
		RequestID:    requestID.String(),
		Source:       d.Source,
		Organization: d.Organization,
	}

	requestKey := datastore.IncompleteKey("Request", nil)
	requestKey.Namespace = NameSpace
	if _, err := dsClient.Put(ctx, requestKey, request); err != nil {
		w.WriteHeader(http.StatusInternalServerError)
		log.Fatalf("Error logging request: %v", err)
		fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -3\"}")
		return
	}

	// let's fetch the file
	if len(d.FileURL) > 0 {
		resp, err := http.Get(d.FileURL)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprint(w, "{success: false, message: \"File cannot be downloaded\"}")
			return
		}
		sbclient, err := storage.NewClient(ctx)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.Fatalf("failed to create storage client: %v", err)
			fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -4\"}")
			return
		}
		fileName := strconv.FormatInt(customer.Key.ID, 10) + "/" + requestID.String() + ".txt"
		bucket := sbclient.Bucket(BucketName)
		file := bucket.Object(fileName).NewWriter(ctx)

		// Warning: storage.AllUsers gives public read access to anyone.
		// file.ACL = []storage.ACLRule{{Entity: storage.AllUsers, Role: storage.RoleReader}}
		// file.ContentType = fh.Header.Get("Content-Type")

		// Entries are immutable, be aggressive about caching (1 day).
		// file.CacheControl = "public, max-age=86400"

		if _, err := io.Copy(file, resp.Body); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.Fatalf("failed to store uploaded file: %v", err)
			fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -5\"}")
			return
		}
		if err := file.Close(); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			log.Fatalf("failed to close upload stream: %v", err)
			fmt.Fprint(w, "{success: false, message: \"Internal error occurred, -6\"}")
			return
		}

		defer resp.Body.Close()

		w.WriteHeader(http.StatusOK)
		fmt.Fprintf(w, "{success: true, message: \"Request queued\", id: \"%v\"}", requestID.String())
		return
	}
}