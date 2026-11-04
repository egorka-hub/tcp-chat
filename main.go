package main

import (
	"bufio"
	"fmt"
	"net"
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

func StartEchoServer(port string) error {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}
	defer listener.Close()
	fmt.Println("TCP Echo Server listening on", port)

	fmt.Println("Waiting for connection...")
	conn, err := listener.Accept()
	if err != nil {
		return err
	}
	defer conn.Close()

	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		msg := ParseIncomingMessage(scanner.Text(), "User_001")
		formatted := FormatMessage(msg)
		conn.Write([]byte(formatted + "\n"))
	}

	return nil
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

func main() {
	const port = ":8080"

	err := StartEchoServer(port)
	if err != nil {
		fmt.Println("server error:", err)
		return
	}
}
