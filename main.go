package main

import (
	"bufio"
	"fmt"
	"net"
	"sync/atomic"
	"time"
)

const (
	MessageTypeUser   = "user"
	MessageTypeSystem = "system"
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

func StartEchoServer(port string) error {
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
		go handleClient(client.Conn, client.ID)
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

func handleClient(conn net.Conn, clientID string) {
	defer conn.Close()

	scanner := bufio.NewScanner(conn)

	for scanner.Scan() {
		msg := ParseIncomingMessage(scanner.Text(), clientID)
		fmt.Printf("Client %s: %s\n", clientID, msg.Content)
		formatted := FormatMessage(msg)
		conn.Write([]byte(formatted + "\n"))
	}

	fmt.Printf("Client %s disconnected\n", clientID)
}

var clientCounter atomic.Int64

func GenerateClientID() string {
	n := clientCounter.Add(1)
	return fmt.Sprintf("User_%03d", n)
}

func main() {
	const port = ":8080"

	err := StartEchoServer(port)
	if err != nil {
		fmt.Println("server error:", err)
		return
	}
}
