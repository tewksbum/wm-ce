package listrakpost

type PubSubMessage struct {
	Data       []byte            `json:"data"`
	Attributes map[string]string `json:"attributes"`
}

type ContactInfo struct {
	FirstName   string `json:"firstName"`
	LastName    string `json:"lastName"`
	Address1    string `json:"address1"`
	Address2    string `json:"address2"`
	City        string `json:"city"`
	State       string `json:"state"`
	Zip         string `json:"zip"`
	Country     string `json:"country"`
	Role        string `json:"role"`
	Email       string `json:"email"`
	ContactID   string `json:"contactID"`
	SchoolCode  string `json:"schoolCode"`
	SchoolColor string `json:"schoolColor"`
	SchoolName  string `json:"schoolName"`
	FbID        string `json:"fbId"`
	Instagram   string `json:"instagram"`
	Social      string `json:"social"`
	Why         string `json:"why"`
	PortalLink  string `json:"portalLink"`
}

type AuthResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"`
}

type SegmentationField struct {
	ID    int    `json:"segmentationFieldId"`
	Value string `json:"value"`
}

type Output struct {
	EmailAddress      string              `json:"emailAddress"`
	SubscriptionState string              `json:"subscriptionState"`
	ExternalContactID string              `json:"externalContactID"`
	Segments          []SegmentationField `json:"segmentationFieldValues"`
}

type Secrets struct {
	Listtrack struct {
		ClientID     string `json:"client_id"`
		ClientSecret string `json:"client_secret"`
	} `json:"listtrack"`
}
