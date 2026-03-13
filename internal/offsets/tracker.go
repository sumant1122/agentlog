package offsets

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

type Tracker struct {
	dir string
	mu  sync.Mutex
}

func NewTracker(dir string) (*Tracker, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, err
	}
	return &Tracker{dir: dir}, nil
}

func (t *Tracker) filePath(consumerGroup string) string {
	return filepath.Join(t.dir, fmt.Sprintf("%s.offset", consumerGroup))
}

func (t *Tracker) GetOffset(consumerGroup string) (int64, error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	path := t.filePath(consumerGroup)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return 0, nil
		}
		return 0, err
	}

	val := strings.TrimSpace(string(data))
	offset, err := strconv.ParseInt(val, 10, 64)
	if err != nil {
		return 0, err
	}

	return offset, nil
}

func (t *Tracker) CommitOffset(consumerGroup string, offset int64) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	path := t.filePath(consumerGroup)
	val := fmt.Sprintf("%d", offset)
	return os.WriteFile(path, []byte(val), 0644)
}
