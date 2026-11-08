package main

import (
	"bufio"
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"text/tabwriter"
	"time"
)

const (
	MessageTypeUser     = "user"
	MessageTypeSystem   = "system"
	BroadcastBufferSize = 100
	ReadTimeout         = 30 * time.Second
	ShutdownTimeout     = 10 * time.Second
	ForceCloseTimeout   = 2 * time.Second
	RejectWriteTimeout  = time.Second

	DefaultPort           = "8080"
	DefaultMaxConnections = 500
	DefaultLogLevel       = "info"
	DefaultHistorySize    = 50
)

var validLogLevels = []string{"debug", "info", "warn", "error"}

type ServerConfig struct {
	Port               string
	MaxConnections     int
	LogLevel           string
	MessageHistorySize int
}

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
	closeRequests   chan CloseAllRequest
	history         *MessageHistory
	logger          *log.Logger

	statsMu   sync.Mutex
	stats     ServerStats
	startTime time.Time

	closeMu sync.RWMutex
	closed  bool

	wg    sync.WaitGroup
	slots chan struct{}
}

type ClientsRequest struct {
	Response chan []string
}

type CountRequest struct {
	Response chan int
}

type CloseAllRequest struct {
	Response chan int
}

type MessageHistory struct {
	mu       sync.Mutex
	messages []ChatMessage
	head     int
	count    int
	size     int
}

type ServerStats struct {
	ActiveConnections      int
	TotalMessagesProcessed int64
	UptimeSeconds          int64
	ErrorCount             int
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

func NewHub(logger *log.Logger, config ServerConfig) *Hub {
	return &Hub{
		clients:         make(map[string]*Client),
		broadcast:       make(chan ChatMessage, BroadcastBufferSize),
		register:        make(chan *Client),
		unregister:      make(chan *Client),
		clientsRequests: make(chan ClientsRequest),
		countRequests:   make(chan CountRequest),
		closeRequests:   make(chan CloseAllRequest),
		history:         NewMessageHistory(config.MessageHistorySize),
		logger:          logger,
		startTime:       time.Now(),
		slots:           make(chan struct{}, config.MaxConnections),
	}
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

func (h *Hub) setActiveConnections(n int) {
	h.statsMu.Lock()
	defer h.statsMu.Unlock()
	h.stats.ActiveConnections = n
}

func (h *Hub) incMessages() {
	h.statsMu.Lock()
	defer h.statsMu.Unlock()
	h.stats.TotalMessagesProcessed++
}

func (h *Hub) incErrors() {
	h.statsMu.Lock()
	defer h.statsMu.Unlock()
	h.stats.ErrorCount++
}

func (h *Hub) GetStats() ServerStats {
	h.statsMu.Lock()
	defer h.statsMu.Unlock()
	stats := h.stats
	stats.UptimeSeconds = int64(time.Since(h.startTime).Seconds())
	return stats
}

func (h *Hub) logError(format string, args ...any) {
	h.incErrors()
	h.logger.Printf("ERROR "+format, args...)
}

func (h *Hub) send(msg ChatMessage) bool {
	h.closeMu.RLock()
	defer h.closeMu.RUnlock()
	if h.closed {
		return false
	}
	h.broadcast <- msg
	return true
}

func (h *Hub) closeAllConnections() int {
	req := CloseAllRequest{Response: make(chan int)}
	h.closeRequests <- req
	return <-req.Response
}

func (h *Hub) Run() {
	broadcast := h.broadcast
	for {
		select {
		case client := <-h.register:
			h.clients[client.ID] = client
			h.setActiveConnections(len(h.clients))
			h.logger.Printf("INFO  Client %s registered", client.ID)
		case client := <-h.unregister:
			delete(h.clients, client.ID)
			h.setActiveConnections(len(h.clients))
			h.logger.Printf("INFO  Client %s unregistered", client.ID)

		case msg, ok := <-broadcast:
			if !ok {
				broadcast = nil
				continue
			}
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

		case req := <-h.closeRequests:
			for _, client := range h.clients {
				client.Conn.Close()
			}
			req.Response <- len(h.clients)
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
			h.logError("Write error to %s: %v", client.ID, err)
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
		h.logError("Client %s: error setting deadline: %v", client.ID, err)
		conn.Close()
		return nil
	}

	ip := conn.RemoteAddr().String()
	h.logger.Printf("INFO  Client %s connected from %s", client.ID, ip)

	err = h.SendHistory(conn)
	if err != nil {
		h.logError("Client %s: error writing history: %v", client.ID, err)
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
	defer hub.wg.Done()
	defer hub.releaseSlot()

	hub.register <- client
	defer func() {
		if r := recover(); r != nil {
			hub.logError("Client %s panic recovered: %v", client.ID, r)
		}
		hub.cleanupClient(client)
	}()

	count := hub.GetClientCount()
	hub.logger.Printf("INFO  Client %s joined. Total: %d", client.ID, count)

	welcome := fmt.Sprintf("Welcome, %s! %d users online. Type /help for commands.\n", client.ID, count)
	_, err := client.Conn.Write([]byte(welcome))
	if err != nil {
		hub.logError("Client %s: error writing welcome message: %v", client.ID, err)
		return
	}

	scanner := bufio.NewScanner(client.Conn)

	for scanner.Scan() {
		err := client.Conn.SetReadDeadline(time.Now().Add(ReadTimeout))
		if err != nil {
			hub.logError("Client %s: error setting deadline: %v", client.ID, err)
			return
		}
		hub.incMessages()
		if strings.HasPrefix(scanner.Text(), "/") {
			hub.HandleCommand(client, scanner.Text())
			continue
		}
		msg := ParseIncomingMessage(scanner.Text(), client.ID)
		hub.logger.Printf("INFO  Client %s: %s", client.ID, msg.Content)
		hub.send(msg)
	}

	var netErr net.Error
	err = scanner.Err()
	if errors.As(err, &netErr) && netErr.Timeout() {
		hub.logger.Printf("WARN  Client %s disconnected (timeout)", client.ID)
	} else if errors.Is(err, net.ErrClosed) {
		hub.logger.Printf("INFO  Client %s disconnected (connection closed)", client.ID)
	} else if err != nil {
		hub.logError("Client %s connection error: %v", client.ID, err)
	} else {
		hub.logger.Printf("INFO  Client %s disconnected", client.ID)
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

	h.send(msg)

	client.Conn.Close()

	h.logger.Printf("WARN  Cleaning up %s, server continues", client.ID)
}

func StartServer(ctx context.Context, port string, hub *Hub) error {
	listener, err := net.Listen("tcp", port)
	if err != nil {
		return err
	}
	defer listener.Close()
	hub.logger.Printf("INFO  Server starting on port %s", port)

	go func() {
		<-ctx.Done()
		listener.Close()
	}()

	for {
		conn, err := listener.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			hub.logError("Accept error: %v", err)
			continue
		}

		if !hub.acquireSlot() {
			hub.rejectConnection(conn)
			continue
		}

		client := hub.setupClientConnection(conn)
		if client == nil {
			hub.releaseSlot()
			continue
		}

		hub.wg.Add(1)
		go handleClientMessages(hub, client)
	}
}

func setupSignalHandling() chan os.Signal {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	return sigCh
}

func (h *Hub) Shutdown(ctx context.Context) error {
	count := h.GetClientCount()
	h.logger.Printf("INFO  Notifying %d clients...", count)

	grace := ShutdownTimeout
	if deadline, ok := ctx.Deadline(); ok {
		grace = time.Until(deadline).Round(time.Second)
	}
	h.send(ChatMessage{
		Timestamp:   time.Now(),
		Content:     fmt.Sprintf("Server shutting down in %d seconds", int(grace.Seconds())),
		MessageType: MessageTypeSystem,
	})

	h.closeMu.Lock()
	h.closed = true
	close(h.broadcast)
	h.closeMu.Unlock()

	done := make(chan struct{})
	go func() {
		h.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		h.logger.Printf("INFO  All clients disconnected")
		return nil
	case <-ctx.Done():
	}

	h.logger.Printf("WARN  Grace period over, closing %d remaining connections", h.GetClientCount())
	h.closeAllConnections()

	select {
	case <-done:
		h.logger.Printf("INFO  All clients disconnected")
		return nil
	case <-time.After(ForceCloseTimeout):
		return errors.New("client goroutines did not finish after closing connections")
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
		h.logError("Write error to %s: %v", client.ID, err)
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

	msg := ChatMessage{
		Timestamp:   time.Now(),
		Content:     text,
		MessageType: MessageTypeSystem,
	}
	formatted := FormatMessage(msg) + "\n"
	_, err := client.Conn.Write([]byte(formatted))
	if err != nil {
		h.logError("Write error to %s: %v", client.ID, err)
	}

	if cmd == "/quit" {
		client.Conn.Close()
	}
}

func setupLogging(level string) *log.Logger {
	return log.New(os.Stdout, "[TCP-CHAT] ", log.LstdFlags)
}

func parseCommandLineArgs() ServerConfig {
	port := flag.String("port", DefaultPort, "Server port")
	maxConnections := flag.Int("max-connections", DefaultMaxConnections, "Maximum connections")
	logLevel := flag.String("log-level", DefaultLogLevel, "Log level: "+strings.Join(validLogLevels, ", "))
	historySize := flag.Int("history-size", DefaultHistorySize, "Number of messages kept in history")

	flag.Usage = printUsage
	flag.Parse()

	return ServerConfig{
		Port:               strings.TrimPrefix(*port, ":"),
		MaxConnections:     *maxConnections,
		LogLevel:           strings.ToLower(*logLevel),
		MessageHistorySize: *historySize,
	}
}

func printUsage() {
	out := flag.CommandLine.Output()
	fmt.Fprintln(out, "Usage:")

	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	for _, name := range []string{"port", "max-connections", "log-level", "history-size"} {
		f := flag.Lookup(name)
		typeName, usage := flag.UnquoteUsage(f)
		def := f.DefValue
		if typeName == "string" {
			def = strconv.Quote(def)
		}
		fmt.Fprintf(w, "  --%s %s\t%s (default %s)\n", f.Name, typeName, usage, def)
	}
	w.Flush()
}

func validateConfig(config ServerConfig) error {
	port, err := strconv.Atoi(config.Port)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("invalid port %q: must be a number from 1 to 65535", config.Port)
	}
	if config.MaxConnections < 1 {
		return fmt.Errorf("invalid max-connections %d: must be at least 1", config.MaxConnections)
	}
	if config.MessageHistorySize < 1 {
		return fmt.Errorf("invalid history-size %d: must be at least 1", config.MessageHistorySize)
	}
	for _, level := range validLogLevels {
		if config.LogLevel == level {
			return nil
		}
	}
	return fmt.Errorf("invalid log-level %q: must be one of %s", config.LogLevel, strings.Join(validLogLevels, ", "))
}

func printStartupBanner(config ServerConfig) {
	fmt.Println("╔══════════════════════════════════════╗")
	fmt.Println("║           TCP Chat Server            ║")
	fmt.Println("╚══════════════════════════════════════╝")
	fmt.Printf("Port:            %s\n", config.Port)
	fmt.Printf("Max Connections: %d\n", config.MaxConnections)
	fmt.Printf("Log Level:       %s\n", strings.ToUpper(config.LogLevel))
	fmt.Printf("History Size:    %d\n", config.MessageHistorySize)
	fmt.Println()
	fmt.Printf("Connect using: telnet localhost %s\n", config.Port)
	fmt.Printf("           or: nc localhost %s\n", config.Port)
	fmt.Println("Press Ctrl+C to stop the server")
	fmt.Println()
}

func main() {
	config := parseCommandLineArgs()
	if err := validateConfig(config); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		flag.Usage()
		os.Exit(2)
	}

	logger := setupLogging(config.LogLevel)
	printStartupBanner(config)

	hub := NewHub(logger, config)
	go hub.Run()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := setupSignalHandling()

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- StartServer(ctx, ":"+config.Port, hub)
	}()

	select {
	case sig := <-sigCh:
		logger.Printf("INFO  Shutdown signal received (%v)", sig)
	case err := <-serverErr:
		logger.Printf("ERROR Server error: %v", err)
		return
	}

	signal.Stop(sigCh)

	cancel()
	if err := <-serverErr; err != nil {
		logger.Printf("ERROR Server error: %v", err)
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), ShutdownTimeout)
	defer cancelShutdown()

	if err := hub.Shutdown(shutdownCtx); err != nil {
		logger.Printf("ERROR Shutdown failed: %v", err)
		return
	}
	logger.Printf("INFO  Server stopped gracefully")
}
