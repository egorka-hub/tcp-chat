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
	fmt.Println("TCP Echo Server listening on", port)

	for {
		fmt.Println("Waiting for connection...")
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
		fmt.Println(client.ID, "connected")

		if err := HandleClient(client); err != nil {
			fmt.Println(client.ID, "error:", err)
		}
		fmt.Println(client.ID, "disconnected")
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

func HandleClient(client *Client) error {
	defer client.Conn.Close()

	scanner := bufio.NewScanner(client.Conn)

	for scanner.Scan() {
		msg := ParseIncomingMessage(scanner.Text(), client.ID)
		formatted := FormatMessage(msg)
		if _, err := client.Conn.Write([]byte(formatted + "\n")); err != nil {
			return err
		}
	}

	return scanner.Err()
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
