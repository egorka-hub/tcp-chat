package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"sync/atomic"
	"time"
)

const (
	MessageTypeUser     = "user"
	MessageTypeSystem   = "system"
	BroadcastBufferSize = 100
	ReadTimeout         = 30 * time.Second
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

func ParseIncomingMessage(raw string, senderID string) ChatMessage {
	timestamp := time.Now()

	return ChatMessage{
		Timestamp:   timestamp,
		ClientID:    senderID,
		Content:     raw,
		MessageType: MessageTypeUser,
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

var clientCounter atomic.Int64

func GenerateClientID() string {
	n := clientCounter.Add(1)
	return fmt.Sprintf("User_%03d", n)
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

func (h *Hub) setupClientConnection(conn net.Conn) *Client {
	client := &Client{
		ID:       GenerateClientID(),
		Conn:     conn,
		JoinTime: time.Now(),
	}

	err := conn.SetReadDeadline(time.Now().Add(ReadTimeout))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error setting deadline: %v\n", err)
		conn.Close()
		return nil
	}

	ip := conn.RemoteAddr().String()
	fmt.Printf("Client %s connected from %s\n", client.ID, ip)

	_, err = conn.Write([]byte("Welcome to the chat, " + client.ID + "\n"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing welcome message: %v\n", err)
		conn.Close()
		return nil
	}
	fmt.Printf("Welcome message sent to %s\n", client.ID)

	return client
}

func handleClientMessages(hub *Hub, client *Client) {
	hub.register <- client
	fmt.Printf("Client %s connected. Total: %d\n", client.ID, hub.GetClientCount())
	defer hub.cleanupClient(client)

	scanner := bufio.NewScanner(client.Conn)

	for scanner.Scan() {
		err := client.Conn.SetReadDeadline(time.Now().Add(ReadTimeout))
		if err != nil {
			return
		}
		msg := ParseIncomingMessage(scanner.Text(), client.ID)
		fmt.Printf("Client %s: %s\n", client.ID, msg.Content)
		hub.broadcast <- msg
	}

	var netErr net.Error
	err := scanner.Err()
	if errors.As(err, &netErr) && netErr.Timeout() {
		fmt.Printf("Client %s disconnected (timeout)\n", client.ID)
	} else if err != nil {
		fmt.Printf("Client %s disconnected (error: %v)\n", client.ID, err)
	} else {
		fmt.Printf("Client %s disconnected\n", client.ID)
	}
}

func (h *Hub) cleanupClient(client *Client) {
	h.unregister <- client

	msg := ChatMessage{
		Timestamp:   time.Now(),
		ClientID:    client.ID,
		Content:     client.ID + " left the chat",
		MessageType: MessageTypeSystem,
	}

	h.broadcast <- msg

	client.Conn.Close()

	fmt.Printf("Cleanup completed for %s\n", client.ID)
}

func StartServer(port string, hub *Hub) error {
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

		client := hub.setupClientConnection(conn)
		if client == nil {
			continue
		}

		go handleClientMessages(hub, client)
	}
}

func main() {
	const port = ":8080"

	hub := NewHub()
	go hub.Run()

	err := StartServer(port, hub)
	if err != nil {
		fmt.Println("server error:", err)
		return
	}
}
