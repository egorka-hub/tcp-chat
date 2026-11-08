package main

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	MessageTypeUser     = "user"
	MessageTypeSystem   = "system"
	BroadcastBufferSize = 100
	ReadTimeout         = 30 * time.Second
	ServerPort          = ":8080"
	MaxHistorySize      = 50
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
	history         *MessageHistory
}

type ClientsRequest struct {
	Response chan []string
}

type CountRequest struct {
	Response chan int
}

type MessageHistory struct {
	mu       sync.Mutex
	messages []ChatMessage
	head     int
	count    int
	size     int
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
		history:         NewMessageHistory(MaxHistorySize),
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
			if msg.MessageType == MessageTypeUser {
				h.history.Add(msg)
			}
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

	err = h.SendHistory(conn)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing history: %v\n", err)
		conn.Close()
		return nil
	}

	return client
}

func (h *Hub) SendHistory(conn net.Conn) error {
	recent := h.history.GetRecent()
	if len(recent) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString("--- Recent messages ---\n")
	for _, msg := range recent {
		sb.WriteString(FormatMessage(msg) + "\n")
	}
	sb.WriteString("--- End of history ---\n")

	_, err := conn.Write([]byte(sb.String()))
	return err
}

func handleClientMessages(hub *Hub, client *Client) {
	hub.register <- client
	defer hub.cleanupClient(client)

	count := hub.GetClientCount()
	fmt.Printf("Client %s connected. Total: %d\n", client.ID, count)

	welcome := fmt.Sprintf("Welcome, %s! %d users online. Type /help for commands.\n", client.ID, count)
	_, err := client.Conn.Write([]byte(welcome))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error writing welcome message to %s: %v\n", client.ID, err)
		return
	}

	scanner := bufio.NewScanner(client.Conn)

	for scanner.Scan() {
		err := client.Conn.SetReadDeadline(time.Now().Add(ReadTimeout))
		if err != nil {
			return
		}
		if strings.HasPrefix(scanner.Text(), "/") {
			hub.HandleCommand(client, scanner.Text())
			continue
		}
		msg := ParseIncomingMessage(scanner.Text(), client.ID)
		fmt.Printf("Client %s: %s\n", client.ID, msg.Content)
		hub.broadcast <- msg
	}

	var netErr net.Error
	err = scanner.Err()
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

func (h *Hub) SendUserList(client *Client) {
	ids := h.GetActiveClients()
	sort.Strings(ids)
	text := fmt.Sprintf("Online users (%d): %s", len(ids), strings.Join(ids, ", "))

	msg := ChatMessage{
		Timestamp:   time.Now(),
		Content:     text,
		MessageType: MessageTypeSystem,
	}
	formatted := FormatMessage(msg) + "\n"
	_, err := client.Conn.Write([]byte(formatted))
	if err != nil {
		fmt.Printf("Write error to %s: %v\n", client.ID, err)
	}
}

func NewMessageHistory(size int) *MessageHistory {
	return &MessageHistory{
		messages: make([]ChatMessage, size),
		size:     size,
	}
}

func (h *MessageHistory) Add(msg ChatMessage) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.messages[h.head] = msg
	h.head = (h.head + 1) % h.size
	if h.count < h.size {
		h.count++
	}
}

func (h *MessageHistory) GetRecent() []ChatMessage {
	h.mu.Lock()
	defer h.mu.Unlock()
	recent := make([]ChatMessage, 0, h.count)
	start := (h.head - h.count + h.size) % h.size
	for i := 0; i < h.count; i++ {
		index := (start + i) % h.size
		recent = append(recent, h.messages[index])
	}
	return recent
}

func (h *Hub) HandleCommand(client *Client, command string) {
	cmd := strings.Fields(command)[0]
	var text string

	switch cmd {
	case "/users":
		h.SendUserList(client)
		return
	case "/help":
		text = "Commands: /users, /help, /time, /quit"
	case "/time":
		text = "Server time: " + time.Now().Format("15:04:05")
	case "/quit":
		text = "Goodbye!"
	default:
		text = "Unknown command: " + cmd + ". Type /help for commands."
	}

	msg := ChatMessage{
		Timestamp:   time.Now(),
		Content:     text,
		MessageType: MessageTypeSystem,
	}
	formatted := FormatMessage(msg) + "\n"
	_, err := client.Conn.Write([]byte(formatted))
	if err != nil {
		fmt.Printf("Write error to %s: %v\n", client.ID, err)
	}

	if cmd == "/quit" {
		client.Conn.Close()
	}
}

func main() {
	hub := NewHub()
	go hub.Run()

	err := StartServer(ServerPort, hub)
	if err != nil {
		fmt.Println("server error:", err)
		return
	}
}
