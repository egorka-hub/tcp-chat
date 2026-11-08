package config

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"text/tabwriter"
)

const (
	DefaultPort           = "8080"
	DefaultMonitorPort    = "8081"
	DefaultMaxConnections = 500
	DefaultLogLevel       = "info"
	DefaultHistorySize    = 50
)

var ValidLogLevels = []string{"debug", "info", "warn", "error"}

type ServerConfig struct {
	Port               string
	MonitorPort        string
	MaxConnections     int
	LogLevel           string
	MessageHistorySize int
}

func ParseCommandLineArgs() ServerConfig {
	port := flag.String("port", DefaultPort, "Server port")
	monitorPort := flag.String("monitor-port", DefaultMonitorPort, "HTTP monitoring port")
	maxConnections := flag.Int("max-connections", DefaultMaxConnections, "Maximum connections")
	logLevel := flag.String("log-level", DefaultLogLevel, "Log level: "+strings.Join(ValidLogLevels, ", "))
	historySize := flag.Int("history-size", DefaultHistorySize, "Number of messages kept in history")

	flag.Usage = PrintUsage
	flag.Parse()

	return ServerConfig{
		Port:               strings.TrimPrefix(*port, ":"),
		MonitorPort:        strings.TrimPrefix(*monitorPort, ":"),
		MaxConnections:     *maxConnections,
		LogLevel:           strings.ToLower(*logLevel),
		MessageHistorySize: *historySize,
	}
}

func PrintUsage() {
	out := flag.CommandLine.Output()
	fmt.Fprintln(out, "Usage:")

	w := tabwriter.NewWriter(out, 0, 0, 2, ' ', 0)
	for _, name := range []string{"port", "monitor-port", "max-connections", "log-level", "history-size"} {
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

func (c ServerConfig) Validate() error {
	if err := validatePort("port", c.Port); err != nil {
		return err
	}
	if err := validatePort("monitor-port", c.MonitorPort); err != nil {
		return err
	}
	if c.Port == c.MonitorPort {
		return fmt.Errorf("port and monitor-port must differ, both are %s", c.Port)
	}
	if c.MaxConnections < 1 {
		return fmt.Errorf("invalid max-connections %d: must be at least 1", c.MaxConnections)
	}
	if c.MessageHistorySize < 1 {
		return fmt.Errorf("invalid history-size %d: must be at least 1", c.MessageHistorySize)
	}
	for _, level := range ValidLogLevels {
		if c.LogLevel == level {
			return nil
		}
	}
	return fmt.Errorf("invalid log-level %q: must be one of %s", c.LogLevel, strings.Join(ValidLogLevels, ", "))
}

func validatePort(name, value string) error {
	port, err := strconv.Atoi(value)
	if err != nil || port < 1 || port > 65535 {
		return fmt.Errorf("invalid %s %q: must be a number from 1 to 65535", name, value)
	}
	return nil
}
