package main

import (
	"bytes"
	"fmt"
	"log"
	"net/http"
)

type sse struct {
	events       chan []byte
	addClient    chan chan []byte
	removeClient chan chan []byte
	clients      map[chan []byte]bool
}

func newSSE() *sse {
	return &sse{
		events:       make(chan []byte, 0),
		addClient:    make(chan chan []byte, 0),
		removeClient: make(chan chan []byte, 0),
		clients:      make(map[chan []byte]bool, 0),
	}
}

func (sse *sse) Loop() {
	for {
		select {
		case s := <-sse.addClient:
			sse.clients[s] = true
			log.Println("Added sse client.", len(sse.clients))
		case s := <-sse.removeClient:
			delete(sse.clients, s)
			log.Println("Removed sse client.", len(sse.clients))
		case event := <-sse.events:
			for client := range sse.clients {
				client <- event
			}
		}
	}
}

var ssecounter = 0

func (sse *sse) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Add("Content-Type", "text/event-stream")
	w.Header().Add("Cache-Control", "no-cache")
	w.Header().Add("Connection", "keep-alive")
	w.(http.Flusher).Flush()

	ssecounter++
	id := ssecounter
	header := r.Header["User-Agent"]

	events := make(chan []byte)
	sse.addClient <- events

	defer func() {
		sse.removeClient <- events
	}()

	go func() {
		<-w.(http.CloseNotifier).CloseNotify()
		sse.removeClient <- events
	}()

	newline := []byte("\n")
	prefix := []byte("\ndata: ")
	for event := range events {
		// When we see a newline we need to add the prefix again.
		event = bytes.Replace(event, newline, prefix, -1)
		if _, err := fmt.Fprintf(w, "data: %s\n\n", event); err != nil {
			log.Println("Error writing to SSE", id, header, err)
			break
		}
		w.(http.Flusher).Flush()
	}
}
