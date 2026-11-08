package domain

import (
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

type Client struct {
	ID       string
	Conn     net.Conn
	JoinTime time.Time
}

type ServerStats struct {
	ActiveConnections      int   `json:"active_connections"`
	TotalMessagesProcessed int64 `json:"total_messages_processed"`
	UptimeSeconds          int64 `json:"uptime_seconds"`
	ErrorCount             int   `json:"error_count"`
}

func ParseIncomingMessage(raw string, senderID string) ChatMessage {
	return ChatMessage{
		Timestamp:   time.Now(),
		ClientID:    senderID,
		Content:     raw,
		MessageType: MessageTypeUser,
	}
}

func NewSystemMessage(text string) ChatMessage {
	return ChatMessage{
		Timestamp:   time.Now(),
		Content:     text,
		MessageType: MessageTypeSystem,
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
