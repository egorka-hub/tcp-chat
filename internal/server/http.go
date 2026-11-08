package server

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/egorka-hub/tcp-chat/internal/domain"
	"github.com/egorka-hub/tcp-chat/internal/hub"
)

const (
	MonitorStopTimeout = 2 * time.Second
	MonitorReadTimeout = 5 * time.Second
)

type HealthResponse struct {
	Status            string `json:"status"`
	ActiveConnections int    `json:"active_connections"`
	UptimeSeconds     int64  `json:"uptime_seconds"`
}

type StatsResponse struct {
	domain.ServerStats
	MessageRate float64 `json:"message_rate"`
}

func StartHTTPMonitoring(h *hub.Hub, port string) (*http.Server, error) {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", HandleHealthEndpoint(h))
	mux.HandleFunc("GET /stats", HandleStatsEndpoint(h))

	addr := ":" + strings.TrimPrefix(port, ":")
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: MonitorReadTimeout,
	}
	h.Logger().Printf("INFO  Monitoring server listening on %s", addr)

	go func() {
		if err := srv.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			h.LogError("Monitoring server error: %v", err)
		}
	}()

	return srv, nil
}

func StopHTTPMonitoring(h *hub.Hub, srv *http.Server) {
	if srv == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), MonitorStopTimeout)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		h.LogError("Monitoring server shutdown error: %v", err)
		return
	}
	h.Logger().Printf("INFO  Monitoring server stopped")
}

func HandleHealthEndpoint(h *hub.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stats := h.GetStats()
		status := "healthy"
		if h.IsShuttingDown() {
			status = "shutting_down"
		}
		writeJSON(h, w, HealthResponse{
			Status:            status,
			ActiveConnections: stats.ActiveConnections,
			UptimeSeconds:     stats.UptimeSeconds,
		}, false)
	}
}

func HandleStatsEndpoint(h *hub.Hub) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		stats := h.GetStats()
		uptime := math.Max(h.Uptime().Seconds(), 1)
		rate := float64(stats.TotalMessagesProcessed) / uptime
		writeJSON(h, w, StatsResponse{
			ServerStats: stats,
			MessageRate: math.Round(rate*100) / 100,
		}, true)
	}
}

func writeJSON(h *hub.Hub, w http.ResponseWriter, v any, indent bool) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	enc := json.NewEncoder(w)
	if indent {
		enc.SetIndent("", "  ")
	}
	if err := enc.Encode(v); err != nil {
		h.LogError("Monitoring response write error: %v", err)
	}
}
