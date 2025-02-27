package main

import (
	"errors"
	"fmt"
	"github.com/gorilla/mux"
	"io"
	"log"
	"net/http"
)

var store = make(map[string]string)
var ErrorNoSuchKey = errors.New("no such key")

func Put(key, value string) error {
	store[key] = value

	return nil
}

func Get(key string) (string, error) {
	value, ok := store[key]

	if !ok {
		return "", ErrorNoSuchKey
	}

	return value, nil
}

func Delete(key string) error {
	delete(store, key)

	return nil
}

// putHandler expects to be called with a PUT request for the
// "v1/key/{key}" resource

func putHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w,
			err.Error(),
			http.StatusInternalServerError)
		return
	}

	defer r.Body.Close()

	err = Put(key, string(value))
	if err != nil {
		http.Error(w,
			err.Error(),
			http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusCreated)
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := Get(key)
	if errors.Is(err, ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	fmt.Fprint(w, value) // Write the value to the response
}

func main() {
	r := mux.NewRouter()

	// Register putHandler as the handler function for PUT requests matching
	// "v1/key/{key}"
	r.HandleFunc("/v1/key/{key}", putHandler).Methods("PUT")
	r.HandleFunc("/v1/key/{key}", getHandler).Methods("GET")

	log.Fatal(http.ListenAndServe(":8080", r))
}
