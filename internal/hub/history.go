package hub

import (
	"sync"

	"tcp-chat/internal/domain"
)

type MessageHistory struct {
	mu       sync.Mutex
	messages []domain.ChatMessage
	head     int
	count    int
	size     int
}

func NewMessageHistory(size int) *MessageHistory {
	return &MessageHistory{
		messages: make([]domain.ChatMessage, size),
		size:     size,
	}
}

func (h *MessageHistory) Add(msg domain.ChatMessage) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.messages[h.head] = msg
	h.head = (h.head + 1) % h.size
	if h.count < h.size {
		h.count++
	}
}

func (h *MessageHistory) GetRecent() []domain.ChatMessage {
	h.mu.Lock()
	defer h.mu.Unlock()
	recent := make([]domain.ChatMessage, 0, h.count)
	start := (h.head - h.count + h.size) % h.size
	for i := 0; i < h.count; i++ {
		recent = append(recent, h.messages[(start+i)%h.size])
	}
	return recent
}
