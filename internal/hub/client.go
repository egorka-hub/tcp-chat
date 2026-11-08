package hub

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"tcp-chat/internal/domain"
)

var clientCounter atomic.Int64

func generateClientID() string {
	return fmt.Sprintf("User_%03d", clientCounter.Add(1))
}

func (h *Hub) HandleConnection(conn net.Conn) {
	if !h.acquireSlot() {
		h.rejectConnection(conn)
		return
	}

	client := h.setupClientConnection(conn)
	if client == nil {
		h.releaseSlot()
		return
	}

	h.wg.Add(1)
	go h.serveClient(client)
}

func (h *Hub) acquireSlot() bool {
	select {
	case h.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

func (h *Hub) releaseSlot() {
	<-h.slots
}

func (h *Hub) rejectConnection(conn net.Conn) {
	defer conn.Close()
	h.logger.Printf("WARN  Connection from %s rejected: server is full (%d/%d)",
		conn.RemoteAddr(), len(h.slots), cap(h.slots))
	conn.SetWriteDeadline(time.Now().Add(RejectWriteTimeout))
	conn.Write([]byte("Server is full, try again later.\n"))
}

func (h *Hub) setupClientConnection(conn net.Conn) *domain.Client {
	client := &domain.Client{
		ID:       generateClientID(),
		Conn:     conn,
		JoinTime: time.Now(),
	}

	if err := conn.SetReadDeadline(time.Now().Add(ReadTimeout)); err != nil {
		h.LogError("Client %s: error setting deadline: %v", client.ID, err)
		conn.Close()
		return nil
	}

	h.logger.Printf("INFO  Client %s connected from %s", client.ID, conn.RemoteAddr())

	if err := h.sendHistory(conn); err != nil {
		h.LogError("Client %s: error writing history: %v", client.ID, err)
		conn.Close()
		return nil
	}

	return client
}

func (h *Hub) sendHistory(conn net.Conn) error {
	recent := h.history.GetRecent()
	if len(recent) == 0 {
		return nil
	}

	var sb strings.Builder
	sb.WriteString("--- Recent messages ---\n")
	for _, msg := range recent {
		sb.WriteString(domain.FormatMessage(msg) + "\n")
	}
	sb.WriteString("--- End of history ---\n")

	_, err := conn.Write([]byte(sb.String()))
	return err
}

func (h *Hub) serveClient(client *domain.Client) {
	defer h.wg.Done()
	defer h.releaseSlot()

	h.register <- client
	defer func() {
		if r := recover(); r != nil {
			h.LogError("Client %s panic recovered: %v", client.ID, r)
		}
		h.cleanupClient(client)
	}()

	count := h.GetClientCount()
	h.logger.Printf("INFO  Client %s joined. Total: %d", client.ID, count)

	welcome := fmt.Sprintf("Welcome, %s! %d users online. Type /help for commands.\n", client.ID, count)
	if _, err := client.Conn.Write([]byte(welcome)); err != nil {
		h.LogError("Client %s: error writing welcome message: %v", client.ID, err)
		return
	}

	scanner := bufio.NewScanner(client.Conn)
	for scanner.Scan() {
		if err := client.Conn.SetReadDeadline(time.Now().Add(ReadTimeout)); err != nil {
			h.LogError("Client %s: error setting deadline: %v", client.ID, err)
			return
		}
		h.incMessages()

		line := scanner.Text()
		if strings.HasPrefix(line, "/") {
			h.HandleCommand(client, line)
			continue
		}
		msg := domain.ParseIncomingMessage(line, client.ID)
		h.logger.Printf("INFO  Client %s: %s", client.ID, msg.Content)
		h.send(msg)
	}

	h.logDisconnect(client, scanner.Err())
}

func (h *Hub) logDisconnect(client *domain.Client, err error) {
	var netErr net.Error
	switch {
	case errors.As(err, &netErr) && netErr.Timeout():
		h.logger.Printf("WARN  Client %s disconnected (timeout)", client.ID)
	case errors.Is(err, net.ErrClosed):
		h.logger.Printf("INFO  Client %s disconnected (connection closed)", client.ID)
	case err != nil:
		h.LogError("Client %s connection error: %v", client.ID, err)
	default:
		h.logger.Printf("INFO  Client %s disconnected", client.ID)
	}
}

func (h *Hub) cleanupClient(client *domain.Client) {
	h.unregister <- client

	msg := domain.NewSystemMessage(client.ID + " left the chat")
	msg.ClientID = client.ID
	h.send(msg)

	client.Conn.Close()

	h.logger.Printf("WARN  Cleaning up %s, server continues", client.ID)
}
