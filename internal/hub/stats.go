package hub

import (
	"time"

	"github.com/egorka-hub/tcp-chat/internal/domain"
)

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

func (h *Hub) Uptime() time.Duration {
	return time.Since(h.startTime)
}

func (h *Hub) GetStats() domain.ServerStats {
	h.statsMu.Lock()
	defer h.statsMu.Unlock()
	stats := h.stats
	stats.UptimeSeconds = int64(h.Uptime().Seconds())
	return stats
}
