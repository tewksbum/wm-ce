package abm

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"net/http"
	"os"
	"strings"
	"time"

	"cloud.google.com/go/datastore"
)

// ProjectID is the GCP Project ID
var ProjectID = os.Getenv("PROJECTID")

// SORKindTemplate is the kind template to retrieve sor setup
var SORKindTemplate = os.Getenv("SORKINDTEMPLATTE")

// SORNamespace is the namespace where the sor setup is
var SORNamespace = os.Getenv("SORNAMESPACE")

// MatchKindTemplate is the kind template to retrieve sor setup
var MatchKindTemplate = os.Getenv("MATCHKEYKINDTEMPLATTE")

// MatchNamespace is the namespace where the sor setup is
var MatchNamespace = os.Getenv("MATCHKEYNAMESPACE")

// CustomerKind is the kind template to retrieve sor setup
var CustomerKind = os.Getenv("CUSTOMERKIND")

// CustomerNamespace is the namespace where the sor setup is
var CustomerNamespace = os.Getenv("CUSTOMERNAMESPACE")

// DefaultEndpoint self explanatory, default output endpoint
var DefaultEndpoint = os.Getenv("DEFAULTENDPOINT")

type Household struct {
	HouseholdID string   `json:"householdId,omitempty"`
	SurrogateID string   `json:"surrogateId,omitempty"`
	Address1    string   `json:"address1,omitempty"`
	Address2    string   `json:"address2,omitempty"`
	City        string   `json:"city,omitempty"`
	State       string   `json:"state,omitempty"`
	Zip         string   `json:"zip,omitempty"`
	Country     string   `json:"country,omitempty"`
	HashedSigs  []string `json:"hashedSigs,omitempty"`
}

// Event data
type Event struct {
	EventID  string `json:"eventId,omitempty"`
	Type     string `json:"type,omitempty"`
	Browser  string `json:"browser,omitempty"`
	OS       string `json:"os,omitempty"`
	Channel  string `json:"channel,omitempty"`
	Location string `json:"location,omitempty"`
	Domain   string `json:"domain,omitempty"`
	URL      string `json:"url,omitempty"`
	Referrer string `json:"referrer,omitempty"`
}

// Campaign data
type Campaign struct {
	CampaignID  string     `json:"campaignId,omitempty"`
	SurrogateID string     `json:"surrogateId,omitempty"`
	Name        string     `json:"name,omitempty"`
	StartDate   *time.Time `json:"startDate,omitempty"`
}

// Product data
type Product struct {
	ProductID   string `json:"productId,omitempty"`
	SurrogateID string `json:"surrogateId,omitempty"`
	Category    string `json:"category,omitempty"`
	SKU         string `json:"sku,omitempty"`
	Size        string `json:"size,omitempty"`
	Color       string `json:"color,omitempty"`
}

// OrderHeader data
type OrderHeader struct {
	OrderID     string     `json:"orderId,omitempty"`
	SurrogateID string     `json:"surrogateId,omitempty"`
	OrderDate   *time.Time `json:"orderDate,omitempty"`
	SubTotal    string     `json:"subTotal,omitempty"`
	Total       string     `json:"total,omitempty"`
	Discount    string     `json:"discount,omitempty"`
	Shipping    string     `json:"shipping,omitempty"`
	Tax         string     `json:"tax,omitempty"`
	HashedSigs  []string   `json:"hashedSigs,omitempty"`
}

// OrderConsignment data
type OrderConsignment struct {
	OrderID       string     `json:"orderId,omitempty"`
	SurrogateID   string     `json:"surrogateId,omitempty"`
	ConsignmentID string     `json:"consignmentId,omitempty"`
	ShipDate      *time.Time `json:"shipDate,omitempty"`
	SubTotal      string     `json:"subTotal,omitempty"`
	HashedSigs    []string   `json:"hashedSigs,omitempty"`
}

// OrderDetail data
type OrderDetail struct {
	OrderID       string     `json:"orderId,omitempty"`
	SurrogateID   string     `json:"surrogateId,omitempty"`
	ConsignmentID string     `json:"consignmentId,omitempty"`
	OrderDetailID string     `json:"orderDetailId,omitempty"`
	ProductID     string     `json:"productId,omitempty"`
	SKU           string     `json:"sku,omitempty"`
	Quantity      int        `json:"quantity,omitempty"`
	ShipDate      *time.Time `json:"shipDate,omitempty"`
	SubTotal      string     `json:"subTotal,omitempty"`
	UnitPrice     string     `json:"unitPrice,omitempty"`
	HashedSigs    []string   `json:"hashedSigs,omitempty"`
}

// People data
type People struct {
	PeopleID     string   `json:"peopleId,omitempty"`
	Salutation   string   `json:"salutation,omitempty"`
	FirstName    string   `json:"firstName,omitempty"`
	LastName     string   `json:"lastName,omitempty"`
	Gender       string   `json:"gender,omitempty"`
	Age          string   `json:"age,omitempty"`
	Organization string   `json:"organization,omitempty"`
	Title        string   `json:"title,omitempty"`
	Role         string   `json:"role,omitempty"`
	HashedSigs   []string `json:"hashedSigs,omitempty"`
}

// SegmentInput input for the API
//Entity type events, order, product  old//event, product, campaign, orderHeader, orderConsignment, orderDetail, people
type OutputHeader struct {
	AccessKey  string `json:"accessKey"` //Access key presumes owner info on segment side
	EntityType string `json:"entityType"`
	OwnerID    int64  `json:"ownerId"`
}
type Output struct {
	OutputHeader
	*People
	*OrderDetail
	*OrderConsignment
	*OrderHeader
	*Product
	*Campaign
	*Event
	*Household
}

type SegmentResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// PubSubMessage is the payload of a Pub/Sub event.
type PubSubMessage struct {
	Data       []byte            `json:"data"`
	Attributes map[string]string `json:"attributes"`
}

// ABM mapper structure
type MatchKeyMap struct {
	MatchKey string
	Source   string
	Type     string
	Key      *datastore.Key `datastore:"__key__"`
}

type CustomerInfo struct {
	AccessKey   string         `datastore:"AccessKey"`
	Enabled     bool           `datastore:"Enabled"`
	Name        string         `datastore:"Name"`
	Owner       string         `datastore:"Owner"`
	Permissions []string       `datastore:"Permissions"`
	CreatedBy   *datastore.Key `datastore:"CreatedBy"`
	Key         *datastore.Key `datastore:"__key__"`
}

type SORSETUP struct {
	Hook      string         `json:"Hook"`
	Type      string         `json:"Type"`
	Endpoint  string         `json:"Endpoint"`
	AccessKey string         `json:"AccessKey"`
	Entity    string         `json:"Entity"`
	MatchKeys []string       `json:"MatchKeys"`
	Key       *datastore.Key `datastore:"__key__"`
}
type Signature struct {
	OwnerID   int64  `json:"ownerId"`
	Source    string `json:"source"`
	EventType string `json:"eventType"`
	EventId   string `json:"eventId"`
	RecordId  string `json:"recordId"`
}

type MatchKey360 struct {
	Key    string   `json:"key" bigquery:"key"`
	Type   string   `json:"type" bigquery:"type"`
	Value  string   `json:"value" bigquery:"value"`
	Values []string `json:"values" bigquery:"values"`
}

type Passthrough360 struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

type Request360 struct {
	ID          string           `json:"id"`
	Signature   Signature        `json:"signature"`  // event
	Signatures  []Signature      `json:"signatures"` // people,order check how to get owner and source
	Fibers      []string         `json:"fibers"`
	Passthrough []Passthrough360 `json:"passthrough"`
	MatchKeys   []MatchKey360    `json:"matchKeys"`
	CreatedAt   time.Time        `json:"createdAt"`
	TimeStamp   time.Time        `json:"timestamp"`
}

func Main(ctx context.Context, m PubSubMessage) error {
	dsClient, err := datastore.NewClient(ctx, ProjectID)

	var request360 Request360
	if err := json.NewDecoder(bytes.NewBuffer(m.Data)).Decode(&request360); err != nil {
		log.Panicf("There was an issue decoding the message %v", string(m.Data))
		return err
	}
	inputType := m.Attributes["type"]
	inputSource := m.Attributes["source"]

	log.Printf("Decoded %v from %v pubsub message %v", string(inputType), string(inputSource), string(m.Data))
	var rSignature = request360.Signature
	var rSignatures = request360.Signatures
	//Get SOR setup
	var sskind bytes.Buffer
	dsKindtemplate, err := template.New("abmOwnerSourcess").Parse(SORKindTemplate)
	if err != nil {
		log.Printf("<%v>-<%v> Unable to parse text template: %v", rSignature.OwnerID, rSignature.Source, err)
		return err
	}
	if err := dsKindtemplate.Execute(&sskind, rSignature); err != nil {
		log.Printf("<%v>-<%v> Unable execute text template: %v", rSignature.OwnerID, rSignature.Source, err)
		return err
	}

	if err != nil {
		log.Printf("Error accessing datastore: %v", err)
		return err
	}
	var sorSetups []SORSETUP
	var sorSetup SORSETUP
	log.Printf("Getting source setup %v %v", SORNamespace, sskind.String())
	sorSetupQuery := datastore.NewQuery(sskind.String()).Namespace(SORNamespace)
	if _, err := dsClient.GetAll(ctx, sorSetupQuery, &sorSetups); err != nil {
		log.Printf("<%v>-<%v> Error querying SORSETUP: %v", rSignature.OwnerID, rSignature.Source, err)
		return err
	}
	if len(sorSetups) == 0 {
		log.Printf("<%v>-<%v> No SORSETUP, sending all fields to default endpoint <%v>", rSignature.OwnerID, rSignature.Source, inputType)
		sorSetup.Endpoint = DefaultEndpoint
	}
	for i, ss := range sorSetups {
		if strings.ToLower(ss.Type) == strings.ToLower(inputType) {
			sorSetup = sorSetups[i]
			break
		} else if strings.ToLower(ss.Type) == "default" {
			// If we don't have an event type in the sorsetup we log it and default it
			sorSetup = sorSetups[i]
		}
	}

	//Get the customer info
	log.Printf("Getting the customer info %v %v", CustomerKind, rSignature.OwnerID)
	var centities []CustomerInfo
	k := datastore.Key{
		Kind:      "Customer",
		ID:        5648073946562560,
		Namespace: "wemade-dev",
	}
	cquery := datastore.NewQuery("Customer").Namespace(CustomerNamespace)

	cquery.Filter("__key__ =", k).Limit(1)

	if _, err := dsClient.GetAll(ctx, cquery, &centities); err != nil {
		log.Printf("<%v>-<%v> Error querying CUSTOMER data: %v", rSignature.OwnerID, rSignature.Source, err)
		return err
	}
	if len(centities) == 0 {
		return fmt.Errorf("<%v>-<%v> No Customer info kind: %v namespace: %v", rSignature.OwnerID, rSignature.Source, CustomerKind, CustomerNamespace)
	}
	customerInfo := centities[0]

	// Get the matchkeys from mapper's datastore table
	var mkkind bytes.Buffer
	dsmkKindtemplate, err := template.New("abmOwnerSourcemk").Parse(MatchKindTemplate)
	if err != nil {
		log.Printf("<%v>-<%v> Unable to parse sor text template: %v", rSignature.OwnerID, rSignature.Source, err)
		return err
	}
	if err := dsmkKindtemplate.Execute(&mkkind, rSignature); err != nil {
		log.Printf("<%v>-<%v> Unable execute sor text template: %v", rSignature.OwnerID, rSignature.Source, err)
		return err
	}
	var matchKeysMap []MatchKeyMap
	// This may improve by just getting the required matchkeys instead of all of them with a .filter on
	// the query
	log.Printf("Getting source setup %v %v", MatchNamespace, mkkind.String())
	mkSetupQuery := datastore.NewQuery(mkkind.String()).Namespace(MatchNamespace)
	mkSetupQuery.Filter("EntityType =", inputType)
	if _, err := dsClient.GetAll(ctx, mkSetupQuery, &matchKeysMap); err != nil {
		log.Printf("<%v>-<%v> Error querying matchkeys: %v", rSignature.OwnerID, rSignature.Source, err)
		return err
	}
	log.Printf("Source %v %v size %v", MatchNamespace, mkkind.String(), len(matchKeysMap))

	log.Printf("<%v>-<%v> ABM processing started", rSignature.OwnerID, rSignature.Source)

	//Here process the input based on sorSetup
	//If we don't have a list of matchkeys on the sorsetup we don't filter it
	var r360filteredmk []MatchKey360
	if len(sorSetup.MatchKeys) > 0 {
		for _, mk := range request360.MatchKeys {
			if inSlice(mk.Key, sorSetup.MatchKeys) {
				r360filteredmk = append(r360filteredmk, mk)
			}
		}
	} else {
		r360filteredmk = request360.MatchKeys
	}
	var outputHeader OutputHeader
	outputHeader.AccessKey = customerInfo.AccessKey
	outputHeader.EntityType = inputType
	outputHeader.OwnerID = rSignature.OwnerID
	output := Output{
		outputHeader,
		&People{},
		&OrderDetail{},
		&OrderConsignment{},
		&OrderHeader{},
		&Product{},
		&Campaign{},
		&Event{},
		&Household{},
	}
	var dynamicMap map[string]interface{}
	//Prepare the ABM output and segment input
	//	Based on the signature event type we create and populate a struct
	switch inputType {
	case "event":
		event := Event{
			EventID: getSignatureHash(rSignature),
			Type:    inputType,
		}
		output.Event = &event

	case "orderHeader":
		var oid string
		if in360Slice("TrustedId", r360filteredmk) {
			oid = getFrom360Slice("TrustedId", r360filteredmk).Value
		} else if in360Slice("OrderId", r360filteredmk) {
			oid = getFrom360Slice("OrderId", r360filteredmk).Value
		} else if in360Slice("OrderNumber", r360filteredmk) {
			oid = getFrom360Slice("OrderNumber", r360filteredmk).Value
		}
		orderHeader := OrderHeader{
			HashedSigs:  getSignaturesHash(rSignatures),
			SurrogateID: request360.ID,
			OrderID:     oid,
		}
		output.OrderHeader = &orderHeader

	case "orderConsignment":
		var oid string
		if in360Slice("TrustedId", r360filteredmk) {
			oid = getFrom360Slice("TrustedId", r360filteredmk).Value
		} else if in360Slice("OrderId", r360filteredmk) {
			oid = getFrom360Slice("OrderId", r360filteredmk).Value
		} else if in360Slice("OrderNumber", r360filteredmk) {
			oid = getFrom360Slice("OrderNumber", r360filteredmk).Value
		}
		orderConsignment := OrderConsignment{
			HashedSigs:  getSignaturesHash(rSignatures),
			SurrogateID: request360.ID,
			OrderID:     oid,
		}
		output.OrderConsignment = &orderConsignment

	case "orderDetail":
		var oid string
		if in360Slice("TrustedId", r360filteredmk) {
			oid = getFrom360Slice("TrustedId", r360filteredmk).Value
		} else if in360Slice("OrderId", r360filteredmk) {
			oid = getFrom360Slice("OrderId", r360filteredmk).Value
		} else if in360Slice("OrderNumber", r360filteredmk) {
			oid = getFrom360Slice("OrderNumber", r360filteredmk).Value
		}
		orderDetail := OrderDetail{
			HashedSigs:  getSignaturesHash(rSignatures),
			SurrogateID: request360.ID,
			OrderID:     oid,
		}
		output.OrderDetail = &orderDetail

	case "household":
		var oid string
		if in360Slice("TrustedId", r360filteredmk) {
			oid = getFrom360Slice("TrustedId", r360filteredmk).Value
		} else if in360Slice("OrderId", r360filteredmk) {
			oid = getFrom360Slice("OrderId", r360filteredmk).Value
		} else if in360Slice("OrderNumber", r360filteredmk) {
			oid = getFrom360Slice("OrderNumber", r360filteredmk).Value
		}
		household := Household{
			HashedSigs:  getSignaturesHash(rSignatures),
			SurrogateID: request360.ID,
			HouseholdID: oid,
		}
		output.Household = &household

	case "product":
		var oid string
		if in360Slice("TrustedId", r360filteredmk) {
			oid = getFrom360Slice("TrustedId", r360filteredmk).Value
		} else if in360Slice("ProductId", r360filteredmk) {
			oid = getFrom360Slice("ProductId", r360filteredmk).Value
		} else if in360Slice("SKU", r360filteredmk) {
			oid = getFrom360Slice("SKU", r360filteredmk).Value
		}
		product := Product{
			ProductID:   oid,
			SurrogateID: request360.ID,
		}
		output.Product = &product

	case "campaign":
		var oid string
		if in360Slice("TrustedId", r360filteredmk) {
			oid = getFrom360Slice("TrustedId", r360filteredmk).Value
		} else if in360Slice("ProductId", r360filteredmk) {
			oid = getFrom360Slice("ProductId", r360filteredmk).Value
		} else if in360Slice("SKU", r360filteredmk) {
			oid = getFrom360Slice("SKU", r360filteredmk).Value
		}
		campaign := Campaign{
			CampaignID:  oid,
			SurrogateID: request360.ID,
		}

		output.Campaign = &campaign

	case "people":
		people := People{
			PeopleID:   request360.ID,
			HashedSigs: getSignaturesHash(rSignatures),
		}
		output.People = &people
	}
	dynamicMap = fillMap(r360filteredmk, matchKeysMap)

	var completeOutput map[string]interface{}
	jsonStrOutput, err := json.Marshal(output)
	if err != nil {
		log.Printf("<%v>-<%v> There was a problem preparing the output object %v ", rSignature.OwnerID, rSignature.Source, err)
		return err
	}
	json.Unmarshal(jsonStrOutput, &completeOutput)
	jsonStrDynamicMap, err := json.Marshal(dynamicMap)
	if err != nil {
		log.Printf("<%v>-<%v> There was a problem preparing the dynamic map %v ", rSignature.OwnerID, rSignature.Source, err)
		return err
	}
	json.Unmarshal(jsonStrDynamicMap, &completeOutput)
	jsonStr, err := json.Marshal(completeOutput)
	if err != nil {
		log.Printf("<%v>-<%v> There was a problem preparing the complete output %v ", rSignature.OwnerID, rSignature.Source, err)
		return err
	}

	log.Printf("<%v>-<%v> ABM pushing to sorSetup Endpoint: %v", rSignature.OwnerID, rSignature.Source, sorSetup.Endpoint)
	log.Printf("<%v>-<%v> Message: %v", rSignature.OwnerID, rSignature.Source, string(jsonStr))

	req, err := http.NewRequest("POST", sorSetup.Endpoint,
		bytes.NewBuffer(jsonStr))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("<%v>-<%v> [ABM OUTPUT]: %v ", rSignature.OwnerID, rSignature.Source, err)
		return err
	}
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	var sr SegmentResponse
	err = decoder.Decode(&sr)
	if err != nil {
		log.Printf("<%v>-<%v> There was a problem decoding the response %v", rSignature.OwnerID, rSignature.Source, sr.Message)
		return err
	}
	if sr.Success != true {
		return fmt.Errorf("<%v>-<%v> response not succesfull %v", rSignature.OwnerID, rSignature.Source, sr.Message)
	}
	log.Printf("<%v>-<%v> Succesfull response: Status <%v> Message <%v>", rSignature.OwnerID, rSignature.Source, sr.Success, sr.Message)

	return nil
}

func getSignaturesHash(ss []Signature) []string {
	var signatures []string
	for _, s := range ss {
		signatures = append(signatures, getSignatureHash(s))
	}
	return signatures
}

func getSignatureHash(s Signature) string {
	var text string
	text = string(s.OwnerID) + s.Source + s.EventType + s.EventId + s.RecordId
	hasher := md5.New()
	hasher.Write([]byte(text))
	return hex.EncodeToString(hasher.Sum(nil))
}

func inSlice(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return true
		}
	}
	return false
}
func in360Slice(a string, list []MatchKey360) bool {
	for _, b := range list {
		if b.Key == a {
			return true
		}
	}
	return false
}
func getFrom360Slice(a string, list []MatchKey360) MatchKey360 {
	for _, b := range list {
		if b.Key == a {
			return b
		}
	}
	return MatchKey360{}
}
func getMKSource(a string, mkm []MatchKeyMap) string {
	for _, mk := range mkm {
		if mk.MatchKey == a {
			return mk.Source
		}
	}
	return ""
}

func fillMap(rmks []MatchKey360, mkms []MatchKeyMap) map[string]interface{} {
	r := make(map[string]interface{})
	for _, rmk := range rmks {
		source := getMKSource(rmk.Key, mkms)
		if source != "" {
			r[source] = rmk.Value
		} else {
			//maybe we need to log the keys that we didn't found in the mapper
			continue
		}
	}
	return r
}
