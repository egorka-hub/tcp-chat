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
	clients    map[string]*Client
	broadcast  chan ChatMessage
	register   chan *Client
	unregister chan *Client
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

		fmt.Printf("Client %s connected, starting goroutine\n", client.ID)
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
		client.Conn.Close()
	}()

	hub.register <- client
	defer fmt.Printf("Client %s disconnected\n", client.ID)

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
		clients:    make(map[string]*Client),
		broadcast:  make(chan ChatMessage, BroadcastBufferSize),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}
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
