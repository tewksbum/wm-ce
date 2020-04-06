package listrakpost

type PubSubMessage struct {
	Data       []byte            `json:"data"`
	Attributes map[string]string `json:"attributes"`
}

// type Input struct {
// 	EventID string `json:"eventId"`
// 	Count   string `json:"count"`
// }

type ContactInfo struct {
	FirstName   string `json:"firstName"`
	LastName    string `json:"lastName"`
	Address1    string `json:"address1"`
	Address2    string `json:"address2"`
	City        string `json:"city"`
	State       string `json:"state"`
	Zip         string `json:"zip"`
	Country     string `json:"country"`
	RoleType    string `json:"roleType"`
	Email       string `json:"email"`
	ContactID   string `json:"contactID"`
	SchoolCode  string `json:"schoolCode"`
	SchoolColor string `json:"schoolColor"`
	SchoolName  string `json:"schoolName"`
}

type Input struct {
	Contacts []ContactInfo `json:"contacts"`
}

type AuthResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

type SegmentationFieldValue struct {
	SegmentationFieldId string `json:"segmentationFieldId"`
	Value               string `json:"value"`
}

type ContactOutput struct {
	EmailAddress            string                   `json:"emailAddress"`
	SubscriptionState       string                   `json:"subscriptionState"`
	ExternalContactID       string                   `json:"externalContactID"`
	SegmentationFieldValues []SegmentationFieldValue `json:"segmentationFieldValues"`
}
