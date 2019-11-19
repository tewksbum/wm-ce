package wemade

// Permissions
var (
	PermissionCreate string = "create"
	PermissionRead   string = "read"
	PermissionUpdate string = "update"
	PermissionDelete string = "delete"
	PermissionList   string = "list"
)

const (
	// EntityCustomer Customer kind - table
	EntityCustomer string = "Customer"
	// FilterCustomersAccessKey filter for customers by access key
	FilterCustomersAccessKey string = "AccessKey = "
	// FilterCustomersByKey filter for customers by __key__
	FilterCustomersByKey string = "__key__ = "
	// FilterCustomersByExternalID filter for customers by __key__
	FilterCustomersByExternalID string = "ExternalID = "
)

// Error messages
var (
	ErrNoPermission         string = "Client has no permission for [%s]"
	ErrDecodingRequest      string = "Error decoding request %#v"
	ErrInternalErrorOcurred string = "Internal error occurred %#v"
	ErrRecordNotFound       string = "%s not found"
	ErrAccountNotEnabled    string = "Account not enabled"
	ErrStatusNoContent      string = "Method [%s] is not allowed"
)
