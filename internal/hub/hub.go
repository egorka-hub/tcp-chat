package hub

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/egorka-hub/tcp-chat/internal/domain"
)

const (
	BroadcastBufferSize    = 100
	ReadTimeout            = 30 * time.Second
	DefaultShutdownTimeout = 10 * time.Second
	ForceCloseTimeout      = 2 * time.Second
	RejectWriteTimeout     = time.Second
)

type Hub struct {
	clients         map[string]*domain.Client
	broadcast       chan domain.ChatMessage
	register        chan *domain.Client
	unregister      chan *domain.Client
	clientsRequests chan clientsRequest
	countRequests   chan countRequest
	closeRequests   chan closeAllRequest
	history         *MessageHistory
	logger          *log.Logger

	statsMu   sync.Mutex
	stats     domain.ServerStats
	startTime time.Time

	closeMu sync.RWMutex
	closed  bool

	wg    sync.WaitGroup
	slots chan struct{}
}

type clientsRequest struct {
	response chan []string
}

type countRequest struct {
	response chan int
}

type closeAllRequest struct {
	response chan int
}

func New(logger *log.Logger, maxConnections, historySize int) *Hub {
	return &Hub{
		clients:         make(map[string]*domain.Client),
		broadcast:       make(chan domain.ChatMessage, BroadcastBufferSize),
		register:        make(chan *domain.Client),
		unregister:      make(chan *domain.Client),
		clientsRequests: make(chan clientsRequest),
		countRequests:   make(chan countRequest),
		closeRequests:   make(chan closeAllRequest),
		history:         NewMessageHistory(historySize),
		logger:          logger,
		startTime:       time.Now(),
		slots:           make(chan struct{}, maxConnections),
	}
}

func (h *Hub) Logger() *log.Logger {
	return h.logger
}

func (h *Hub) LogError(format string, args ...any) {
	h.incErrors()
	h.logger.Printf("ERROR "+format, args...)
}

func (h *Hub) Run() {
	broadcast := h.broadcast
	for {
		select {
		case client := <-h.register:
			h.clients[client.ID] = client
			h.setActiveConnections(len(h.clients))
			h.logger.Printf("INFO  Client %s registered", client.ID)

		case client := <-h.unregister:
			delete(h.clients, client.ID)
			h.setActiveConnections(len(h.clients))
			h.logger.Printf("INFO  Client %s unregistered", client.ID)

		case msg, ok := <-broadcast:
			if !ok {
				broadcast = nil
				continue
			}
			if msg.MessageType == domain.MessageTypeUser {
				h.history.Add(msg)
			}
			h.broadcastMessage(msg)

		case req := <-h.countRequests:
			req.response <- len(h.clients)

		case req := <-h.clientsRequests:
			ids := make([]string, 0, len(h.clients))
			for id := range h.clients {
				ids = append(ids, id)
			}
			req.response <- ids

		case req := <-h.closeRequests:
			for _, client := range h.clients {
				client.Conn.Close()
			}
			req.response <- len(h.clients)
		}
	}
}

func (h *Hub) broadcastMessage(msg domain.ChatMessage) {
	text := domain.FormatMessage(msg) + "\n"

	for _, client := range h.clients {
		if client.ID == msg.ClientID {
			continue
		}
		if _, err := client.Conn.Write([]byte(text)); err != nil {
			h.LogError("Write error to %s: %v", client.ID, err)
		}
	}
}

func (h *Hub) send(msg domain.ChatMessage) bool {
	h.closeMu.RLock()
	defer h.closeMu.RUnlock()
	if h.closed {
		return false
	}
	h.broadcast <- msg
	return true
}

func (h *Hub) GetActiveClients() []string {
	req := clientsRequest{response: make(chan []string)}
	h.clientsRequests <- req
	return <-req.response
}

func (h *Hub) GetClientCount() int {
	req := countRequest{response: make(chan int)}
	h.countRequests <- req
	return <-req.response
}

func (h *Hub) closeAllConnections() int {
	req := closeAllRequest{response: make(chan int)}
	h.closeRequests <- req
	return <-req.response
}

func (h *Hub) IsShuttingDown() bool {
	h.closeMu.RLock()
	defer h.closeMu.RUnlock()
	return h.closed
}

func (h *Hub) Shutdown(ctx context.Context) error {
	h.logger.Printf("INFO  Notifying %d clients...", h.GetClientCount())

	grace := DefaultShutdownTimeout
	if deadline, ok := ctx.Deadline(); ok {
		grace = time.Until(deadline).Round(time.Second)
	}
	h.send(domain.NewSystemMessage(fmt.Sprintf("Server shutting down in %d seconds", int(grace.Seconds()))))

	h.closeMu.Lock()
	h.closed = true
	close(h.broadcast)
	h.closeMu.Unlock()

	done := make(chan struct{})
	go func() {
		h.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		h.logger.Printf("INFO  All clients disconnected")
		return nil
	case <-ctx.Done():
	}

	h.logger.Printf("WARN  Grace period over, closing %d remaining connections", h.GetClientCount())
	h.closeAllConnections()

	select {
	case <-done:
		h.logger.Printf("INFO  All clients disconnected")
		return nil
	case <-time.After(ForceCloseTimeout):
		return errors.New("client goroutines did not finish after closing connections")
	}
}
