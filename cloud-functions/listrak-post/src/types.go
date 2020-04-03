package listrakpost

type PubSubMessage struct {
	Data       []byte            `json:"data"`
	Attributes map[string]string `json:"attributes"`
}

type Input struct {
	EventID string `json:"eventId"`
	Count   string `json:"count"`
}

type School struct {
	SchoolCode  string `json:"schoolCode"`
	SchoolColor string `json:"schoolColor"`
	SchoolName  string `json:"schoolName"`
}

type ContactInfo struct {
	FirstName string `json:"firstName"`
	LastName  string `json:"lastName"`
	Address1  string `json:"address1"`
	Address2  string `json:"address2"`
	City      string `json:"city"`
	State     string `json:"state"`
	Zip       string `json:"zip"`
	Country   string `json:"country"`
	RoleType  string `json:"roleType"`
	Email     string `json:"email"`
	ContactID string `json:"contactID"`
}

type ContactOutput struct {
	School      School      `json:"school"`
	ContactInfo ContactInfo `json:"contactInfo"`
}
