package main

import (
	"io"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

const objectsPrefix = "/v1/objects/"
const storageRoot = "./data/blobstore"

func objectPathFromKey(key string) (string, error) {
	cleanKey := filepath.Clean(key)

	fullPath := filepath.Join(storageRoot, cleanKey)

	return fullPath, nil
}

func handlePutObject(w http.ResponseWriter, r )

func getKeyFromPath(path string) (string, bool) {
	if !strings.HasPrefix(path, objectsPrefix) {
		return "", false
	}

	key := strings.TrimPrefix(path, objectsPrefix)

	if key == "" || strings.Contains(key, "..") {
		return "", false
	}

	if strings.HasPrefix(key, "/") {
		key = key[1:]
		if key == "" {
			return "", false
		}
	}

	return key, true
}
func handleObject(w http.ResponseWriter, r *http.Request) {
	key, ok := getKeyFromPath(r.URL.Path)
	if !ok {
		http.Error(w, "invalid object key", http.StatusBadRequest)
		return
	}
	switch r.Method {
	case http.MethodPut:
		// PUT /v1/objects/{key}
		w.WriteHeader(http.StatusNotImplemented)
		_, _ = w.Write([]byte("PUT not implemented yet for key: " + key + "\n"))

	case http.MethodGet:
		// GET /v1/objects/{key}
		w.WriteHeader(http.StatusNotImplemented)
		_, _ = w.Write([]byte("GET not implemented yet for key: " + key + "\n"))

	default:
		// Any other method: 405 Method Not Allowed
		w.WriteHeader(http.StatusMethodNotAllowed)
		_, _ = w.Write([]byte("method not allowed\n"))
	}
}

func main() {
	mux := http.NewServeMux()

	// All blob operations are under /v1/objects/.
	mux.HandleFunc("/v1/objects/", handleObject)

	server := &http.Server{
		Addr:    ":7070",
		Handler: mux,
	}

	log.Println("blob server listening on :7070")

	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Fatalf("server error: %v", err)
	}
}
