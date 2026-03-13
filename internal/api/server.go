package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/agentlog/agentlog/internal/broker"
	"github.com/agentlog/agentlog/internal/log"
	"github.com/agentlog/agentlog/pkg/events"
)

type Server struct {
	engine *log.FileEventLog
	broker *broker.Broker
}

func NewServer(engine *log.FileEventLog, b *broker.Broker) *Server {
	return &Server{
		engine: engine,
		broker: b,
	}
}

func (s *Server) HandlePublish(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	if topic == "" {
		http.Error(w, "missing topic", http.StatusBadRequest)
		return
	}

	var event events.Event
	if err := json.NewDecoder(r.Body).Decode(&event); err != nil {
		http.Error(w, "invalid json", http.StatusBadRequest)
		return
	}

	if event.Timestamp == 0 {
		event.Timestamp = time.Now().Unix()
	}

	offset, err := s.engine.Append(topic, event)
	if err != nil {
		http.Error(w, "internal error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	event.Offset = offset

	// Broadcast to active subscribers
	s.broker.Broadcast(topic, event)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{"offset": offset, "status": "published"})
}

func (s *Server) HandleReplay(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")
	offsetStr := r.URL.Query().Get("offset")
	offset, _ := strconv.ParseInt(offsetStr, 10, 64)

	eventsList, err := s.engine.ReadFrom(topic, offset)
	if err != nil {
		http.Error(w, "internal error: "+err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/x-ndjson")
	for _, e := range eventsList {
		data, _ := json.Marshal(e)
		w.Write(data)
		w.Write([]byte("\n"))
	}
}

func (s *Server) HandleSubscribe(w http.ResponseWriter, r *http.Request) {
	topic := r.PathValue("topic")

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming unsupported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	// Allow CORS for simple tests
	w.Header().Set("Access-Control-Allow-Origin", "*")

	ch := s.broker.Subscribe(topic)
	defer s.broker.Unsubscribe(topic, ch)

	// Send an initial connected message (optional, but good for debugging)
	fmt.Fprintf(w, ": connected to topic %s\n\n", topic)
	flusher.Flush()

	ctx := r.Context()
	for {
		select {
		case <-ctx.Done():
			return
		case e := <-ch:
			data, _ := json.Marshal(e)
			// SSE format requires "data: {}\n\n"
			fmt.Fprintf(w, "data: %s\n\n", data)
			flusher.Flush()
		}
	}
}

func (s *Server) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("POST /topics/{topic}/events", s.HandlePublish)
	mux.HandleFunc("GET /topics/{topic}/replay", s.HandleReplay)
	mux.HandleFunc("GET /topics/{topic}/subscribe", s.HandleSubscribe)
}
