package app

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"tcp-chat/internal/config"
	"tcp-chat/internal/hub"
	"tcp-chat/internal/server"
)

const ShutdownTimeout = 10 * time.Second

func Run() {
	cfg := config.ParseCommandLineArgs()
	if err := cfg.Validate(); err != nil {
		fmt.Fprintln(os.Stderr, "Error:", err)
		config.PrintUsage()
		os.Exit(2)
	}

	logger := setupLogging(cfg.LogLevel)
	if err := run(cfg, logger); err != nil {
		logger.Printf("ERROR %v", err)
		os.Exit(1)
	}
}

func run(cfg config.ServerConfig, logger *log.Logger) error {
	printStartupBanner(cfg)

	h := hub.New(logger, cfg.MaxConnections, cfg.MessageHistorySize)
	go h.Run()

	monitor, err := server.StartHTTPMonitoring(h, cfg.MonitorPort)
	if err != nil {
		h.LogError("Monitoring server failed to start: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := setupSignalHandling()

	serverErr := make(chan error, 1)
	go func() {
		serverErr <- server.StartTCPServer(ctx, ":"+cfg.Port, h)
	}()

	select {
	case sig := <-sigCh:
		logger.Printf("INFO  Shutdown signal received (%v)", sig)
	case err := <-serverErr:
		server.StopHTTPMonitoring(h, monitor)
		return fmt.Errorf("server error: %w", err)
	}

	signal.Stop(sigCh)

	cancel()
	if err := <-serverErr; err != nil {
		h.LogError("Server error: %v", err)
	}

	shutdownCtx, cancelShutdown := context.WithTimeout(context.Background(), ShutdownTimeout)
	defer cancelShutdown()

	shutdownErr := h.Shutdown(shutdownCtx)
	server.StopHTTPMonitoring(h, monitor)
	if shutdownErr != nil {
		return fmt.Errorf("shutdown failed: %w", shutdownErr)
	}
	logger.Printf("INFO  Server stopped gracefully")
	return nil
}

func setupLogging(level string) *log.Logger {
	return log.New(os.Stdout, "[TCP-CHAT] ", log.LstdFlags)
}

func setupSignalHandling() chan os.Signal {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	return sigCh
}

func printStartupBanner(cfg config.ServerConfig) {
	fmt.Println("╔══════════════════════════════════════╗")
	fmt.Println("║           TCP Chat Server            ║")
	fmt.Println("╚══════════════════════════════════════╝")
	fmt.Printf("Port:            %s\n", cfg.Port)
	fmt.Printf("Monitor Port:    %s\n", cfg.MonitorPort)
	fmt.Printf("Max Connections: %d\n", cfg.MaxConnections)
	fmt.Printf("Log Level:       %s\n", strings.ToUpper(cfg.LogLevel))
	fmt.Printf("History Size:    %d\n", cfg.MessageHistorySize)
	fmt.Println()
	fmt.Printf("Connect using: telnet localhost %s\n", cfg.Port)
	fmt.Printf("           or: nc localhost %s\n", cfg.Port)
	fmt.Printf("Health check:  curl http://localhost:%s/health\n", cfg.MonitorPort)
	fmt.Printf("Statistics:    curl http://localhost:%s/stats\n", cfg.MonitorPort)
	fmt.Println("Press Ctrl+C to stop the server")
	fmt.Println()
}
