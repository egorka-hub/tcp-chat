package server

import (
	"context"
	"net"

	"github.com/egorka-hub/tcp-chat/internal/hub"
)

func StartTCPServer(ctx context.Context, addr string, h *hub.Hub) error {
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer listener.Close()
	h.Logger().Printf("INFO  TCP Chat Server listening on %s", addr)

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
			h.LogError("Accept error: %v", err)
			continue
		}
		h.HandleConnection(conn)
	}
}
