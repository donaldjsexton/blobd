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

// -------------------------------
// PATH HELPERS
// -------------------------------

func objectPathFromKey(key string) string {
	cleanKey := filepath.Clean(key)
	return filepath.Join(storageRoot, cleanKey)
}

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

// -------------------------------
// PUT IMPLEMENTATION
// -------------------------------

func handlePutObject(w http.ResponseWriter, r *http.Request, key string) {
	targetPath := objectPathFromKey(key)

	// immutability check
	if _, err := os.Stat(targetPath); err == nil {
		http.Error(w, "object already exists", http.StatusConflict)
		return
	} else if !os.IsNotExist(err) {
		log.Printf("stat error for %q: %v", targetPath, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// ensure directory exists
	dir := filepath.Dir(targetPath)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		log.Printf("mkdir error %q: %v", dir, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}

	// temp file
	tmpFile, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		log.Printf("CreateTemp error: %v", err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	defer func() {
		_ = tmpFile.Close()
		_ = os.Remove(tmpFile.Name())
	}()

	// write body -> temp file
	if _, err := io.Copy(tmpFile, r.Body); err != nil {
		log.Printf("io.Copy error: %v", err)
		http.Error(w, "write error", http.StatusInternalServerError)
		return
	}

	// flush to disk
	if err := tmpFile.Sync(); err != nil {
		log.Printf("Sync error: %v", err)
		http.Error(w, "write error", http.StatusInternalServerError)
		return
	}

	// close before rename
	if err := tmpFile.Close(); err != nil {
		log.Printf("Close error: %v", err)
		http.Error(w, "write error", http.StatusInternalServerError)
		return
	}

	// atomic rename
	if err := os.Rename(tmpFile.Name(), targetPath); err != nil {
		log.Printf("Rename error: %v", err)
		http.Error(w, "write error", http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write([]byte("created\n"))
}

// -------------------------------
// GET IMPLEMENTATION
// -------------------------------

func handleGetObject(w http.ResponseWriter, r *http.Request, key string) {
	targetPath := objectPathFromKey(key)

	f, err := os.Open(targetPath)
	if os.IsNotExist(err) {
		http.Error(w, "object not found", http.StatusNotFound)
		return
	}
	if err != nil {
		log.Printf("open error for %q: %v", targetPath, err)
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	defer f.Close()

	// Stream file contents to the client.
	// We intentionally do not load the whole blob into memory.
	if _, err := io.Copy(w, f); err != nil {
		log.Printf("io.Copy (GET) error for %q: %v", targetPath, err)
		// Can't reliably recover at this point, but we log it.
	}
}

// -------------------------------
// ROUTER
// -------------------------------

func handleObject(w http.ResponseWriter, r *http.Request) {
	key, ok := getKeyFromPath(r.URL.Path)
	if !ok {
		http.Error(w, "invalid object key", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodPut:
		handlePutObject(w, r, key)

	case http.MethodGet:
		handleGetObject(w, r, key)

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// -------------------------------
// MAIN
// -------------------------------

func main() {
	mux := http.NewServeMux()
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
