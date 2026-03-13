package broker

import (
	"sync"

	"github.com/agentlog/agentlog/pkg/events"
)

type Broker struct {
	mu          sync.RWMutex
	subscribers map[string]map[chan events.Event]struct{}
}

func NewBroker() *Broker {
	return &Broker{
		subscribers: make(map[string]map[chan events.Event]struct{}),
	}
}

func (b *Broker) Subscribe(topic string) chan events.Event {
	b.mu.Lock()
	defer b.mu.Unlock()

	if _, exists := b.subscribers[topic]; !exists {
		b.subscribers[topic] = make(map[chan events.Event]struct{})
	}

	ch := make(chan events.Event, 100)
	b.subscribers[topic][ch] = struct{}{}
	return ch
}

func (b *Broker) Unsubscribe(topic string, ch chan events.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if subs, exists := b.subscribers[topic]; exists {
		delete(subs, ch)
		close(ch)
	}
}

func (b *Broker) Broadcast(topic string, event events.Event) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if subs, exists := b.subscribers[topic]; exists {
		for ch := range subs {
			select {
			case ch <- event:
			default:
				// If channel is full, skip
			}
		}
	}
}
