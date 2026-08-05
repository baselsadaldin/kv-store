// Command kvstore is an interactive REPL for the key-value store.
package main

import (
	"bufio"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/baselsadaldin/kv-store/store"
)

func main() {
	file := flag.String("file", "", "path to a write-ahead log file for persistence (in-memory only if omitted)")
	flag.Parse()

	var s *store.Store
	if *file != "" {
		var err error
		s, err = store.Open(*file)
		if err != nil {
			fmt.Fprintf(os.Stderr, "failed to open %s: %v\n", *file, err)
			os.Exit(1)
		}
		defer s.Close()
	} else {
		s = store.New()
	}

	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("kvstore> commands: SET key value | GET key | DELETE key | KEYS | COMPACT | EXIT")
	for {
		fmt.Print("> ")
		if !scanner.Scan() {
			break
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}

		fields := strings.SplitN(line, " ", 3)
		cmd := strings.ToUpper(fields[0])

		switch cmd {
		case "SET":
			if len(fields) != 3 {
				fmt.Println("usage: SET key value")
				continue
			}
			if err := s.Set(fields[1], fields[2]); err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				continue
			}
			fmt.Println("OK")

		case "GET":
			if len(fields) != 2 {
				fmt.Println("usage: GET key")
				continue
			}
			v, err := s.Get(fields[1])
			if errors.Is(err, store.ErrKeyNotFound) {
				fmt.Println("(nil)")
				continue
			}
			fmt.Println(v)

		case "DELETE":
			if len(fields) != 2 {
				fmt.Println("usage: DELETE key")
				continue
			}
			if err := s.Delete(fields[1]); err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				continue
			}
			fmt.Println("OK")

		case "KEYS":
			for _, k := range s.Keys() {
				fmt.Println(k)
			}

		case "COMPACT":
			if err := s.Compact(); err != nil {
				fmt.Fprintf(os.Stderr, "error: %v\n", err)
				continue
			}
			fmt.Println("OK")

		case "EXIT", "QUIT":
			return

		default:
			fmt.Printf("unknown command: %s\n", fields[0])
		}
	}
}
