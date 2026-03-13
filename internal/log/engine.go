package log

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"

	"github.com/agentlog/agentlog/pkg/events"
)

type EventLog interface {
	Append(topic string, event events.Event) (int64, error)
	ReadFrom(topic string, offset int64) ([]events.Event, error)
}

type FileEventLog struct {
	dir string
	mu  sync.Mutex
	// Cache of offsets for fast append
	offsets map[string]int64
	// RWMutex per topic to allow concurrent reads vs appends
	topicLocks map[string]*sync.RWMutex
}

func NewFileEventLog(dir string) (*FileEventLog, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &FileEventLog{
		dir:        dir,
		offsets:    make(map[string]int64),
		topicLocks: make(map[string]*sync.RWMutex),
	}, nil
}

func (l *FileEventLog) getLock(topic string) *sync.RWMutex {
	l.mu.Lock()
	defer l.mu.Unlock()
	if lock, exists := l.topicLocks[topic]; exists {
		return lock
	}
	lock := &sync.RWMutex{}
	l.topicLocks[topic] = lock
	return lock
}

func (l *FileEventLog) filePath(topic string) string {
	return filepath.Join(l.dir, fmt.Sprintf("%s.jsonl", topic))
}

func (l *FileEventLog) currentOffset(topic string) (int64, error) {
	l.mu.Lock()
	if offset, exists := l.offsets[topic]; exists {
		l.mu.Unlock()
		return offset, nil
	}
	l.mu.Unlock()

	path := l.filePath(topic)
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}
	defer file.Close()

	var lastOffset int64 = 0
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var e events.Event
		if err := json.Unmarshal(scanner.Bytes(), &e); err == nil {
			lastOffset = e.Offset
		}
	}

	l.mu.Lock()
	l.offsets[topic] = lastOffset
	l.mu.Unlock()

	return lastOffset, scanner.Err()
}

func (l *FileEventLog) Append(topic string, event events.Event) (int64, error) {
	lock := l.getLock(topic)
	lock.Lock()
	defer lock.Unlock()

	curr, err := l.currentOffset(topic)
	if err != nil {
		return 0, err
	}

	event.Offset = curr + 1

	data, err := json.Marshal(event)
	if err != nil {
		return 0, err
	}
	data = append(data, '\n')

	path := l.filePath(topic)
	file, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	if _, err := file.Write(data); err != nil {
		return 0, err
	}

	l.mu.Lock()
	l.offsets[topic] = event.Offset
	l.mu.Unlock()

	return event.Offset, nil
}

func (l *FileEventLog) ReadFrom(topic string, offset int64) ([]events.Event, error) {
	lock := l.getLock(topic)
	lock.RLock()
	defer lock.RUnlock()

	path := l.filePath(topic)
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return []events.Event{}, nil
		}
		return nil, err
	}
	defer file.Close()

	var result []events.Event
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		var e events.Event
		if err := json.Unmarshal(scanner.Bytes(), &e); err != nil {
			continue
		}
		if e.Offset >= offset {
			result = append(result, e)
		}
	}

	return result, scanner.Err()
}
