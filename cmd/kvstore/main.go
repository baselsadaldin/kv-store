// Command kvstore is an interactive REPL for the in-memory key-value store.
package main

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/baselsadaldin/kv-store/store"
)

func main() {
	s := store.New()
	scanner := bufio.NewScanner(os.Stdin)

	fmt.Println("kvstore> commands: SET key value | GET key | DELETE key | KEYS | EXIT")
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
			s.Set(fields[1], fields[2])
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
			s.Delete(fields[1])
			fmt.Println("OK")

		case "KEYS":
			for _, k := range s.Keys() {
				fmt.Println(k)
			}

		case "EXIT", "QUIT":
			return

		default:
			fmt.Printf("unknown command: %s\n", fields[0])
		}
	}
}
