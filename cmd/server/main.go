package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/agentlog/agentlog/internal/api"
	"github.com/agentlog/agentlog/internal/broker"
	engine "github.com/agentlog/agentlog/internal/log"
)

func main() {
	port := flag.Int("port", 8080, "port to listen on")
	dataDir := flag.String("data", "data", "data directory")
	flag.Parse()

	topicsDir := filepath.Join(*dataDir, "topics")
	if err := os.MkdirAll(topicsDir, 0755); err != nil {
		log.Fatalf("failed to create topics dir: %v", err)
	}
	
	offsetsDir := filepath.Join(*dataDir, "offsets")
	if err := os.MkdirAll(offsetsDir, 0755); err != nil {
		log.Fatalf("failed to create offsets dir: %v", err)
	}

	logEngine, err := engine.NewFileEventLog(topicsDir)
	if err != nil {
		log.Fatalf("failed to init log engine: %v", err)
	}

	eventBroker := broker.NewBroker()

	server := api.NewServer(logEngine, eventBroker)

	mux := http.NewServeMux()
	server.RegisterRoutes(mux)

	fmt.Printf("Starting agentlog server on :%d\n", *port)
	fmt.Printf("Data directory: %s\n", *dataDir)
	
	if err := http.ListenAndServe(fmt.Sprintf(":%d", *port), mux); err != nil {
		log.Fatalf("server failed: %v", err)
	}
}
