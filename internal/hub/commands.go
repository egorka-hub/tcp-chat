package hub

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"tcp-chat/internal/domain"
)

func (h *Hub) HandleCommand(client *domain.Client, command string) {
	cmd := strings.Fields(command)[0]
	var text string

	switch cmd {
	case "/users":
		h.sendUserList(client)
		return
	case "/help":
		text = "Commands: /users, /help, /time, /stats, /quit"
	case "/time":
		text = "Server time: " + time.Now().Format("15:04:05")
	case "/stats":
		s := h.GetStats()
		text = fmt.Sprintf("Active: %d, Messages: %d, Uptime: %ds, Errors: %d",
			s.ActiveConnections, s.TotalMessagesProcessed, s.UptimeSeconds, s.ErrorCount)
	case "/quit":
		text = "Goodbye!"
	default:
		text = "Unknown command: " + cmd + ". Type /help for commands."
	}

	h.writeSystemMessage(client, text)

	if cmd == "/quit" {
		client.Conn.Close()
	}
}

func (h *Hub) sendUserList(client *domain.Client) {
	ids := h.GetActiveClients()
	sort.Strings(ids)
	h.writeSystemMessage(client, fmt.Sprintf("Online users (%d): %s", len(ids), strings.Join(ids, ", ")))
}

func (h *Hub) writeSystemMessage(client *domain.Client, text string) {
	formatted := domain.FormatMessage(domain.NewSystemMessage(text)) + "\n"
	if _, err := client.Conn.Write([]byte(formatted)); err != nil {
		h.LogError("Write error to %s: %v", client.ID, err)
	}
}
