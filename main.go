package main

import (
	"bufio"
	"fmt"
	"net"
)

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
		conn.Write([]byte(scanner.Text() + "\n"))
	}

	return nil
}

func main() {
	const port = ":8080"

	err := StartEchoServer(port)
	if err != nil {
		fmt.Println("server error:", err)
		return
	}
}
