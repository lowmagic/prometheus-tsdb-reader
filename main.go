// Command prometheus-tsdb-reader: A Go library and CLI for reading and querying Prometheus TSDB dumps offline.
package main

import (
	"fmt"
	"os"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run() error {
	fmt.Println("Hello from prometheus-tsdb-reader!")
	return nil
}
