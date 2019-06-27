package wemade

import (
	"fmt"
	"net/http"

	"github.com/google/uuid"
)

const (
	CookieName = "wemade-browserid"
)

func contains(strings []string, val string) bool {
	for _, check := range strings {
		if check == val {
			return true
		}
	}

	return false
}

func ReadCookie(req *http.Request) (browserID *string) {
	cookie, err := req.Cookie(CookieName)
	if err != nil {
		return nil
	}
	return &cookie.Value
}

func SetCookie(w http.ResponseWriter) (browserID *string) {
	cookie := &http.Cookie{
		Name:  CookieName,
		Value: uuid.Must(uuid.NewRandom()).String(),
	}
	http.SetCookie(w, cookie)
	return &cookie.Value
}

func Main(w http.ResponseWriter, r *http.Request) {
	browserID := ReadCookie(r)
	if browserID == nil || *browserID == "" {
		fmt.Println("First time")
		browserID = SetCookie(w)
	} else {
		fmt.Println("Return")
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Origin", r.Header.Get("Origin"))

	respJson := fmt.Sprintf("{\"browserID\": \"%s\"}", *browserID)
	w.Write([]byte(respJson))
}
