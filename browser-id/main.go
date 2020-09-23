package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/google/uuid"
)

const (
	// CookieName = "wemade-browserid"
	CookieName = "_wm-id"
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

func SetCookie(w http.ResponseWriter, r *http.Request) (browserID *string) {
	cookie := &http.Cookie{
		Name:     CookieName,
		Value:    "WM-" + r.Host + "-" + uuid.Must(uuid.NewRandom()).String() + "." + time.Now().Format("2006-01-02.15:04"),
		SameSite: http.SameSiteNoneMode,
		Secure:   true,
	}
	http.SetCookie(w, cookie)
	return &cookie.Value
}

func home(w http.ResponseWriter, r *http.Request) {
	browserID := ReadCookie(r)
	if browserID == nil || *browserID == "" {
		browserID = SetCookie(w, r)
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
	w.Header().Set("Access-Control-Allow-Credentials", "true")
	w.Header().Set("Access-Control-Allow-Origin", r.Header.Get("Origin"))

	respJson := fmt.Sprintf("{\"browserID\": \"%s\"}", *browserID)
	w.Write([]byte(respJson))
}

func main() {
	fmt.Println("Starting on port " + os.Getenv("PORT"))
	http.HandleFunc("/v1/browser-id", home)
	log.Fatal(http.ListenAndServe(":"+os.Getenv("PORT"), nil))
}
