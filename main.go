package main

import (
	"bufio"
	"fmt"
	"net"
	"sync/atomic"
	"time"
)

const (
	MessageTypeUser     = "user"
	MessageTypeSystem   = "system"
	BroadcastBufferSize = 100
)

type ChatMessage struct {
	Timestamp   time.Time
	ClientID    string
	Content     string
	MessageType string
}

type Client struct {
	ID       string
	Conn     net.Conn
	JoinTime time.Time
}

type Hub struct {
	clients         map[string]*Client
	broadcast       chan ChatMessage
	register        chan *Client
	unregister      chan *Client
	clientsRequests chan ClientsRequest
	countRequests   chan CountRequest
}

type ClientsRequest struct {
	Response chan []string
}

type CountRequest struct {
	Response chan int
}

func StartEchoServer(port string, hub *Hub) error {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}
	defer listener.Close()
	fmt.Println("TCP Chat Server listening on", port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Println("accept error:", err)
			continue
		}

		client := &Client{
			ID:       GenerateClientID(),
			Conn:     conn,
			JoinTime: time.Now(),
		}

		go handleClient(hub, client)
	}
}

func FormatMessage(msg ChatMessage) string {
	timestamp := msg.Timestamp.Format("15:04:05")

	switch msg.MessageType {
	case MessageTypeUser:
		return fmt.Sprintf("[%s] <%s>: %s", timestamp, msg.ClientID, msg.Content)
	case MessageTypeSystem:
		return fmt.Sprintf("[%s] *** %s", timestamp, msg.Content)
	default:
		return fmt.Sprintf("[%s] %s", timestamp, msg.Content)
	}
}

func ParseIncomingMessage(raw string, senderID string) ChatMessage {
	timestamp := time.Now()

	return ChatMessage{
		Timestamp:   timestamp,
		ClientID:    senderID,
		Content:     raw,
		MessageType: MessageTypeUser,
	}
}

func handleClient(hub *Hub, client *Client) {
	defer func() {
		hub.unregister <- client
		fmt.Printf("Client %s disconnected. Total: %d\n", client.ID, hub.GetClientCount())
		client.Conn.Close()
	}()

	hub.register <- client
	fmt.Printf("Client %s connected. Total: %d\n", client.ID, hub.GetClientCount())

	scanner := bufio.NewScanner(client.Conn)

	for scanner.Scan() {
		msg := ParseIncomingMessage(scanner.Text(), client.ID)
		fmt.Printf("Client %s: %s\n", client.ID, msg.Content)
		hub.broadcast <- msg
	}
}

var clientCounter atomic.Int64

func GenerateClientID() string {
	n := clientCounter.Add(1)
	return fmt.Sprintf("User_%03d", n)
}

func (h *Hub) Run() {
	for {
		select {
		case client := <-h.register:
			h.clients[client.ID] = client
			fmt.Printf("Client %s registered\n", client.ID)
		case client := <-h.unregister:
			delete(h.clients, client.ID)
			fmt.Printf("Client %s unregistered\n", client.ID)

		case msg := <-h.broadcast:
			h.BroadcastMessage(msg)

		case req := <-h.countRequests:
			req.Response <- len(h.clients)

		case req := <-h.clientsRequests:
			ids := make([]string, 0, len(h.clients))
			for id := range h.clients {
				ids = append(ids, id)
			}
			req.Response <- ids
		}
	}
}

func (h *Hub) BroadcastMessage(msg ChatMessage) {
	text := FormatMessage(msg) + "\n"

	for _, client := range h.clients {
		if client.ID == msg.ClientID {
			continue
		}
		_, err := client.Conn.Write([]byte(text))
		if err != nil {
			fmt.Printf("Write error to %s: %v\n", client.ID, err)
			continue
		}
	}

}

func NewHub() *Hub {
	return &Hub{
		clients:         make(map[string]*Client),
		broadcast:       make(chan ChatMessage, BroadcastBufferSize),
		register:        make(chan *Client),
		unregister:      make(chan *Client),
		clientsRequests: make(chan ClientsRequest),
		countRequests:   make(chan CountRequest),
	}
}

func (h *Hub) GetActiveClients() []string {
	req := ClientsRequest{Response: make(chan []string)}
	h.clientsRequests <- req
	return <-req.Response
}

func (h *Hub) GetClientCount() int {
	req := CountRequest{Response: make(chan int)}
	h.countRequests <- req
	return <-req.Response
}

func main() {
	const port = ":8080"

	hub := NewHub()
	go hub.Run()

	err := StartEchoServer(port, hub)
	if err != nil {
		fmt.Println("server error:", err)
		return
	}
}
