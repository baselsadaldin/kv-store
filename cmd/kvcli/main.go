// Command kvcli is an interactive client for a kvstore server (see
// cmd/kvstore), connecting over TCP and speaking its line protocol.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:6380", "address of the kvstore server")
	flag.Parse()

	conn, err := net.Dial("tcp", *addr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed to connect to %s: %v\n", *addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	server := bufio.NewReader(conn)
	stdin := bufio.NewScanner(os.Stdin)

	fmt.Printf("kvcli> connected to %s - commands: SET key value | GET key | DELETE key | KEYS | COMPACT | EXIT\n", *addr)
	for {
		fmt.Print("> ")
		if !stdin.Scan() {
			return
		}
		line := strings.TrimSpace(stdin.Text())
		if line == "" {
			continue
		}

		cmd := strings.ToUpper(strings.Fields(line)[0])
		if cmd == "EXIT" || cmd == "QUIT" {
			return
		}

		if _, err := fmt.Fprintf(conn, "%s\n", line); err != nil {
			fmt.Fprintf(os.Stderr, "connection error: %v\n", err)
			return
		}

		if cmd == "KEYS" {
			printKeys(server)
			continue
		}
		printResponse(server)
	}
}

// printKeys reads "KEY <k>" lines from the server until the "END" terminator.
func printKeys(server *bufio.Reader) {
	for {
		resp, err := server.ReadString('\n')
		if err != nil {
			fmt.Fprintf(os.Stderr, "connection error: %v\n", err)
			os.Exit(1)
		}
		resp = strings.TrimRight(resp, "\n")
		if resp == "END" {
			return
		}
		fmt.Println(strings.TrimPrefix(resp, "KEY "))
	}
}

// printResponse reads and formats a single response line for any command
// other than KEYS.
func printResponse(server *bufio.Reader) {
	resp, err := server.ReadString('\n')
	if err != nil {
		fmt.Fprintf(os.Stderr, "connection error: %v\n", err)
		os.Exit(1)
	}
	resp = strings.TrimRight(resp, "\n")

	switch {
	case resp == "NIL":
		fmt.Println("(nil)")
	case strings.HasPrefix(resp, "VALUE "):
		fmt.Println(strings.TrimPrefix(resp, "VALUE "))
	case strings.HasPrefix(resp, "ERR "):
		fmt.Fprintln(os.Stderr, resp)
	default:
		fmt.Println(resp)
	}
}
