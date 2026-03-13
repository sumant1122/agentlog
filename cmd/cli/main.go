package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"
	"strings"

	"github.com/agentlog/agentlog/pkg/events"
)

const serverURL = "http://localhost:8080"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	cmd := os.Args[1]
	switch cmd {
	case "tail":
		if len(os.Args) < 3 {
			fmt.Println("Usage: agentlog tail <topic>")
			return
		}
		tail(os.Args[2])
	case "publish":
		if len(os.Args) < 4 {
			fmt.Println("Usage: agentlog publish <topic> <type> [payload]")
			return
		}
		payloadStr := "{}"
		if len(os.Args) > 4 {
			payloadStr = os.Args[4]
		}
		publish(os.Args[2], os.Args[3], payloadStr)
	case "replay":
		if len(os.Args) < 3 {
			fmt.Println("Usage: agentlog replay <topic> [--offset N]")
			return
		}
		topic := os.Args[2]
		offset := int64(0)
		if len(os.Args) > 4 && os.Args[3] == "--offset" {
			offset, _ = strconv.ParseInt(os.Args[4], 10, 64)
		}
		replay(topic, offset)
	default:
		printUsage()
	}
}

func printUsage() {
	fmt.Println("agentlog CLI")
	fmt.Println("Commands:")
	fmt.Println("  tail <topic>                         Stream events live")
	fmt.Println("  publish <topic> <type> [payload]   Publish an event")
	fmt.Println("  replay <topic> [--offset N]          Replay events from log")
}

func tail(topic string) {
	url := fmt.Sprintf("%s/topics/%s/subscribe", serverURL, topic)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Server returned error: %d\n", resp.StatusCode)
		return
	}

	reader := bufio.NewReader(resp.Body)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Read error: %v\n", err)
			return
		}
		
		line = strings.TrimSpace(line)
		
		// Skip empty lines or comments
		if len(line) == 0 || line[0] == ':' {
			continue
		}

		// SSE format
		if strings.HasPrefix(line, "data: ") {
			jsonData := line[6:] // strip "data: "
			fmt.Println(jsonData)
		}
	}
}

func publish(topic, typ, payloadStr string) {
	var payload interface{}
	if err := json.Unmarshal([]byte(payloadStr), &payload); err != nil {
		// If it's not JSON, just treat it as string
		payload = payloadStr
	}

	event := events.Event{
		Producer: "cli",
		Type:     typ,
		Payload:  payload,
	}

	data, _ := json.Marshal(event)
	url := fmt.Sprintf("%s/topics/%s/events", serverURL, topic)

	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	fmt.Printf("Published to %s -> %s\n", topic, strings.TrimSpace(string(body)))
}

func replay(topic string, offset int64) {
	url := fmt.Sprintf("%s/topics/%s/replay?offset=%d", serverURL, topic, offset)
	resp, err := http.Get(url)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		fmt.Printf("Server returned error: %d\n", resp.StatusCode)
		return
	}

	// Just stream out the JSONL lines directly
	_, err = io.Copy(os.Stdout, resp.Body)
	if err != nil {
		fmt.Printf("Error reading stream: %v\n", err)
	}
}
