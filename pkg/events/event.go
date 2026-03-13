package events

type Event struct {
	Offset    int64       `json:"offset"`
	Timestamp int64       `json:"timestamp"`
	Producer  string      `json:"producer"`
	Type      string      `json:"type"`
	TraceID   string      `json:"trace_id"`
	Payload   interface{} `json:"payload"`
}
