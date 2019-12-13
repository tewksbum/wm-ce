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

// Debug if set will log extra inputs
var Debug = os.Getenv("DEBUG")

// Household data
type Household struct {
	HouseholdID string `json:"householdId,omitempty"`
	Address1    string `json:"address1,omitempty"`
	Address2    string `json:"address2,omitempty"`
	Address3    string `json:"address3,omitempty"`
	City        string `json:"city,omitempty"`
	State       string `json:"state,omitempty"`
	Zip         string `json:"zip,omitempty"`
	Country     string `json:"country,omitempty"`
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
	CampaignID string `json:"campaignId,omitempty"`
	Name       string `json:"name,omitempty"`
	StartDate  string `json:"startDate,omitempty"`
}

// Product data
type Product struct {
	Category string `json:"category,omitempty"`
	SKU      string `json:"sku,omitempty"`
	Size     string `json:"size,omitempty"`
	Color    string `json:"color,omitempty"`
}

// OrderHeader data
type OrderHeader struct {
	OrderDate string `json:"orderDate,omitempty"`
	SubTotal  string `json:"subTotal,omitempty"`
	Total     string `json:"total,omitempty"`
	Discount  string `json:"discount,omitempty"`
	Shipping  string `json:"shipping,omitempty"`
	Tax       string `json:"tax,omitempty"`
}

// OrderConsignment data
type OrderConsignment struct {
	ShipDate string `json:"shipDate,omitempty"`
	SubTotal string `json:"subTotal,omitempty"`
}

// OrderDetail data
type OrderDetail struct {
	OrderDetailID string `json:"orderDetailId,omitempty"`
	SKU           string `json:"sku,omitempty"`
	Quantity      string `json:"quantity,omitempty"`
	ShipDate      string `json:"shipDate,omitempty"`
	SubTotal      string `json:"subTotal,omitempty"`
	UnitPrice     string `json:"unitPrice,omitempty"`
}

// EmailSt people's email structure
type EmailSt struct {
	Email string `json:"email,omitempty"`
	Type  string `json:"type,omitempty"`
}

// PhoneSt people's email structure
type PhoneSt struct {
	Phone string `json:"phone,omitempty"`
	Type  string `json:"type,omitempty"`
}

// People data
type People struct {
	PeopleID     string    `json:"peopleId,omitempty"`
	Salutation   string    `json:"salutation,omitempty"`
	FirstName    string    `json:"firstName,omitempty"`
	Gender       string    `json:"gender,omitempty"`
	Age          string    `json:"age,omitempty"`
	Organization string    `json:"organization,omitempty"`
	Title        string    `json:"title,omitempty"`
	Role         string    `json:"role,omitempty"`
	Phones       []PhoneSt `json:"phones,omitempty"`
	Emails       []EmailSt `json:"emails,omitempty"`
}

// OutputHeader input for the API
type OutputHeader struct {
	AccessKey  string `json:"accessKey"`
	EntityType string `json:"entityType"`
	OwnerID    int64  `json:"ownerId"`
}

// Common allows us to merge structs with similar fields, these fields
// relate to other structs which will get merge before sending
type Common struct {
	OrderID       string   `json:"orderId,omitempty"`
	SurrogateID   string   `json:"surrogateId,omitempty"`
	ConsignmentID string   `json:"consignmentId,omitempty"`
	Signatures    []string `json:"signatures,omitempty"`
	LastName      string   `json:"lastName,omitempty"`
	ProductID     string   `json:"productId,omitempty"`
}

// Output merge struct
type Output struct {
	OutputHeader
	Common
	*People
	*OrderDetail
	*OrderConsignment
	*OrderHeader
	*Product
	*Campaign
	*Event
	*Household
}

// APIResponse struct to parse the output message response
type APIResponse struct {
	Success bool   `json:"success"`
	Message string `json:"message"`
}

// PubSubMessage is the payload of a Pub/Sub event.
type PubSubMessage struct {
	Data       []byte            `json:"data"`
	Attributes map[string]string `json:"attributes"`
}

// MatchKeyMap ABM mapper structure
type MatchKeyMap struct {
	MatchKey   string
	Source     string
	Type       string
	EntityType string
	Key        *datastore.Key `datastore:"__key__"`
}

// CustomerInfo is the struct for the customer database that we get
// with the Owner from the signature
type CustomerInfo struct {
	AccessKey   string         `datastore:"AccessKey"`
	Enabled     bool           `datastore:"Enabled"`
	Name        string         `datastore:"Name"`
	Owner       string         `datastore:"Owner"`
	Permissions []string       `datastore:"Permissions"`
	CreatedBy   *datastore.Key `datastore:"CreatedBy"`
	Key         *datastore.Key `datastore:"__key__"`
}

// SORSETUP is the entry of the source of record datastore table
// which defines what gets sent or not to the output
type SORSETUP struct {
	Hook      string         `json:"Hook"`
	Type      string         `json:"Type"`
	Endpoint  string         `json:"Endpoint"`
	AccessKey string         `json:"AccessKey"`
	Entity    string         `json:"Entity"`
	MatchKeys []string       `json:"MatchKeys"`
	Key       *datastore.Key `datastore:"__key__"`
}

// Signature the identifyier for every request
type Signature struct {
	OwnerID   string `json:"ownerId"`
	Source    string `json:"source"`
	EventType string `json:"eventType"`
	EventID   string `json:"eventId"`
	RecordID  string `json:"recordId"`
}

// MatchKey360 360 and source field map struct
type MatchKey360 struct {
	Key    string   `json:"key" bigquery:"key"`
	Type   string   `json:"type" bigquery:"type"`
	Value  string   `json:"value" bigquery:"value"`
	Values []string `json:"values" bigquery:"values"`
}

// Passthrough360 is the data that comes from the source and should be passed to the sor
type Passthrough360 struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// Request360 trigger message from every 360 cloud-function
type Request360 struct {
	ID          string           `json:"id"`
	Signature   Signature        `json:"signature"`
	Signatures  []Signature      `json:"signatures"`
	Fibers      []string         `json:"fibers"`
	Passthrough []Passthrough360 `json:"passthrough"`
	MatchKeys   []MatchKey360    `json:"matchKeys"`
	CreatedAt   time.Time        `json:"createdAt"`
	TimeStamp   time.Time        `json:"timestamp"`
}

func logDebug(message string) {
	if Debug == "true" {
		log.Print(message)
	}
}

// Main ABM processor, takes the 360 outputs and sends them to the stored source url
func Main(ctx context.Context, m PubSubMessage) error {
	dsClient, err := datastore.NewClient(ctx, ProjectID)

	var request360 Request360
	if err := json.NewDecoder(bytes.NewBuffer(m.Data)).Decode(&request360); err != nil {
		log.Panicf("There was an issue decoding the message %v", string(m.Data))
		return err
	}
	inputType := m.Attributes["type"]
	inputSource := m.Attributes["source"]
	logDebug(fmt.Sprintf("Input message decoded %v from %v pubsub message %v", string(inputType), string(inputSource), string(m.Data)))
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
	logDebug(fmt.Sprintf("Getting source setup %v %v", SORNamespace, sskind.String()))
	sorSetupQuery := datastore.NewQuery(sskind.String()).Namespace(SORNamespace)
	if _, err := dsClient.GetAll(ctx, sorSetupQuery, &sorSetups); err != nil {
		log.Printf("<%v>-<%v> Error querying SORSETUP: %v", rSignature.OwnerID, rSignature.Source, err)
		return err
	}
	if len(sorSetups) == 0 {
		logDebug(fmt.Sprintf("<%v>-<%v> No SORSETUP, sending all fields to default endpoint <%v>", rSignature.OwnerID, rSignature.Source, inputType))
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
	var centities []CustomerInfo
	k := datastore.Key{
		Kind:      "Customer",
		Name:      rSignature.OwnerID,
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
	mkSetupQuery := datastore.NewQuery(mkkind.String()).Namespace(MatchNamespace)
	if _, err := dsClient.GetAll(ctx, mkSetupQuery, &matchKeysMap); err != nil {
		log.Printf("<%v>-<%v> Error querying matchkeys: %v", rSignature.OwnerID, rSignature.Source, err)
		return err
	}

	//Here process the input based on sorSetup
	//If we don't have a list of matchkeys on the sorsetup we don't filter it
	var r360filteredmk []MatchKey360
	r360filteredmk = request360.MatchKeys

	// if len(sorSetup.MatchKeys) > 0 {
	// 	log.Printf("<%v>-<%v>  Only allowing matchkey from the sorSetup %v", rSignature.OwnerID, rSignature.Source, sorSetup.MatchKeys)
	// 	for _, mk := range request360.MatchKeys {
	// 		if inSlice(mk.Key, sorSetup.MatchKeys) {
	// 			r360filteredmk = append(r360filteredmk, mk)
	// 		}
	// 	}
	// } else {
	// 	log.Printf("<%v>-<%v> Passing every matchkey", rSignature.OwnerID, rSignature.Source)

	// 	r360filteredmk = request360.MatchKeys
	// }

	var outputHeader OutputHeader
	outputHeader.AccessKey = customerInfo.AccessKey // don't Change to ocm then?
	outputHeader.EntityType = inputType
	var common Common
	output := Output{
		outputHeader,
		common,
		&People{},
		&OrderDetail{},
		&OrderConsignment{},
		&OrderHeader{},
		&Product{},
		&Campaign{},
		&Event{},
		&Household{},
	}
	// var dynamicMap map[string]interface{}
	//Prepare the ABM output and segment input
	//	Based on the signature event type we create and populate a struct
	switch inputType {
	case "event":
		event := Event{
			EventID:  getSignatureHash(rSignature),
			Type:     rSignature.EventType,
			Browser:  getFrom360Slice("Browser", r360filteredmk).Value,
			OS:       getFrom360Slice("OS", r360filteredmk).Value,
			Channel:  getFrom360Slice("Channel", r360filteredmk).Value,
			Location: getFrom360Slice("Location", r360filteredmk).Value,
			Domain:   getFrom360Slice("Domain", r360filteredmk).Value,
			URL:      getFrom360Slice("URL", r360filteredmk).Value,
			Referrer: getFrom360Slice("Referrer", r360filteredmk).Value,
		}
		output.Event = &event
	case "order":
		orderHeader := OrderHeader{
			OrderDate: getFrom360Slice("DATE", r360filteredmk).Value,
			SubTotal:  getFrom360Slice("SUBTOTAL", r360filteredmk).Value,
			Total:     getFrom360Slice("TOTAL", r360filteredmk).Value,
			Discount:  getFrom360Slice("DISCOUNT", r360filteredmk).Value,
			Shipping:  getFrom360Slice("SHIPPING", r360filteredmk).Value,
			Tax:       getFrom360Slice("TAX", r360filteredmk).Value,
		}
		output.Common.Signatures = getSignaturesHash(rSignatures)
		output.Common.OrderID = getFrom360Slice("ID", r360filteredmk).Value
		output.Common.SurrogateID = request360.ID

		output.OrderHeader = &orderHeader

	case "consignment":
		orderConsignment := OrderConsignment{
			ShipDate: getFrom360Slice("ShipDate", r360filteredmk).Value,
			SubTotal: getFrom360Slice("SubTotal", r360filteredmk).Value,
		}
		output.Common.ConsignmentID = getFrom360Slice("ConsignmentID", r360filteredmk).Value //Check if valid
		output.Common.SurrogateID = request360.ID
		output.OrderConsignment = &orderConsignment

	case "orderdetail":
		orderDetail := OrderDetail{
			OrderDetailID: getFrom360Slice("OrderDetailID", r360filteredmk).Value,
			SKU:           getFrom360Slice("SKU", r360filteredmk).Value,
			Quantity:      getFrom360Slice("Quantity", r360filteredmk).Value,
			ShipDate:      getFrom360Slice("ShipDate", r360filteredmk).Value,
			SubTotal:      getFrom360Slice("SubTotal", r360filteredmk).Value,
			UnitPrice:     getFrom360Slice("UnitPrice", r360filteredmk).Value,
		}
		output.OrderDetail = &orderDetail
		output.Common.OrderID = getFrom360Slice("ID", r360filteredmk).Value
		output.Common.ConsignmentID = getFrom360Slice("ConsignmentID", r360filteredmk).Value
		output.Common.ProductID = getFrom360Slice("ProductID", r360filteredmk).Value
		output.Common.SurrogateID = request360.ID
		output.Common.Signatures = getSignaturesHash(rSignatures)

	case "household":
		household := Household{
			HouseholdID: getFrom360Slice("TrustedId", r360filteredmk).Value,
			Address1:    getFrom360Slice("AD1", r360filteredmk).Value,
			Address2:    getFrom360Slice("AD2", r360filteredmk).Value,
			Address3:    getFrom360Slice("AD3", r360filteredmk).Value,
			City:        getFrom360Slice("CITY", r360filteredmk).Value,
			State:       getFrom360Slice("STATE", r360filteredmk).Value,
			Zip:         getFrom360Slice("ZIP", r360filteredmk).Value,
			Country:     getFrom360Slice("COUNTRY", r360filteredmk).Value,
		}
		output.Household = &household
		output.Common.Signatures = getSignaturesHash(rSignatures)
		output.Common.LastName = getFrom360Slice("LNAME", r360filteredmk).Value
		output.Common.SurrogateID = request360.ID

	case "product":
		product := Product{
			Category: getFrom360Slice("CATEGORY", r360filteredmk).Value,
			SKU:      getFrom360Slice("SKU", r360filteredmk).Value,
			Size:     getFrom360Slice("SIZE", r360filteredmk).Value,
			Color:    getFrom360Slice("COLOR", r360filteredmk).Value,
		}
		output.Product = &product
		output.Common.SurrogateID = request360.ID
		output.Common.ProductID = getFrom360Slice("ProductID", r360filteredmk).Value

	case "campaign":
		campaign := Campaign{
			CampaignID: getFrom360Slice("CAMPAIGNID", r360filteredmk).Value,
			Name:       getFrom360Slice("NAME", r360filteredmk).Value,
			StartDate:  getFrom360Slice("STARTDATE", r360filteredmk).Value,
		}

		output.Campaign = &campaign
		output.Common.SurrogateID = request360.ID

	case "people":
		people := People{
			PeopleID:     request360.ID,
			Salutation:   getFrom360Slice("SALUTATION", r360filteredmk).Value,
			FirstName:    getFrom360Slice("FNAME", r360filteredmk).Value,
			Gender:       getFrom360Slice("GENDER", r360filteredmk).Value,
			Age:          getFrom360Slice("AGE", r360filteredmk).Value,
			Organization: getFrom360Slice("ORGANIZATION", r360filteredmk).Value,
			Title:        getFrom360Slice("TITLE", r360filteredmk).Value,
			Role:         getFrom360Slice("ROLE", r360filteredmk).Value,
		}
		output.People = &people
		output.People.Phones = []PhoneSt{
			PhoneSt{
				Phone: getFrom360Slice("PHONE", r360filteredmk).Value,
				Type:  getFrom360Slice("PHONE", r360filteredmk).Type,
			},
		}

		output.People.Emails = []EmailSt{
			EmailSt{
				Email: getFrom360Slice("EMAIL", r360filteredmk).Value,
				Type:  getFrom360Slice("EMAIL", r360filteredmk).Type,
			},
		}
		output.Common.Signatures = getSignaturesHash(rSignatures)
		output.Common.LastName = getFrom360Slice("LNAME", r360filteredmk).Value

	}
	// If/when we go back to mapper we can re-enable this
	// dynamicMap = fillMap(r360filteredmk, matchKeysMap)

	// var completeOutput map[string]interface{}
	jsonStrOutput, err := json.Marshal(output)
	if err != nil {
		log.Printf("<%v>-<%v> There was a problem preparing the output object %v ", rSignature.OwnerID, rSignature.Source, err)
		return err
	}
	// json.Unmarshal(jsonStrOutput, &completeOutput)
	// jsonStrDynamicMap, err := json.Marshal(dynamicMap)
	// if err != nil {
	// 	log.Printf("<%v>-<%v> There was a problem preparing the dynamic map %v ", rSignature.OwnerID, rSignature.Source, err)
	// 	return err
	// }
	// json.Unmarshal(jsonStrDynamicMap, &completeOutput)
	// jsonStr, err := json.Marshal(completeOutput)
	// if err != nil {
	// 	log.Printf("<%v>-<%v> There was a problem preparing the complete output %v ", rSignature.OwnerID, rSignature.Source, err)
	// 	return err
	// }
	logDebug(fmt.Sprintf("<%v>-<%v> ABM pushing to Endpoint: %v", rSignature.OwnerID, rSignature.Source, sorSetup.Endpoint))
	logDebug(fmt.Sprintf("<%v>-<%v> Outbound message: %v", rSignature.OwnerID, rSignature.Source, string(jsonStrOutput)))
	req, err := http.NewRequest("POST", sorSetup.Endpoint,
		bytes.NewBuffer(jsonStrOutput))
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("<%v>-<%v> [ABM OUTPUT]: %v ", rSignature.OwnerID, rSignature.Source, err)
		return err
	}
	defer resp.Body.Close()
	decoder := json.NewDecoder(resp.Body)
	var sr APIResponse
	err = decoder.Decode(&sr)
	if err != nil {
		log.Printf("<%v>-<%v> There was a problem decoding the response %v", rSignature.OwnerID, rSignature.Source, sr.Message)
		return err
	}
	if sr.Success != true {
		return fmt.Errorf("<%v>-<%v> response not succesfull %v", rSignature.OwnerID, rSignature.Source, sr.Message)
	}
	logDebug(fmt.Sprintf("<%v>-<%v> Succesfull response: Status <%v> Message <%v>", rSignature.OwnerID, rSignature.Source, sr.Success, sr.Message))
	log.Printf("<%v>-<%v> Succesfull response: Status <%v>, 360id %v", rSignature.OwnerID, rSignature.Source, sr.Success, request360.ID)

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
	text = string(s.OwnerID) + s.Source + s.EventType + s.EventID + s.RecordID
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
		if strings.ToLower(b.Key) == strings.ToLower(a) {
			return b
		}
	}
	return MatchKey360{}
}
func getMKSource(a string, mkm []MatchKeyMap) string {
	fmt.Printf(a)
	for _, mk := range mkm {
		if strings.ToLower(mk.MatchKey) == strings.ToLower(a) {
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
