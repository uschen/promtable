package main

import (
	"fmt"
	"os"

	"github.com/uschen/promtable"
)

func main() {
	cfg := promtable.ParseFlags()
	server, err := promtable.NewServerWithConfig(cfg)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	if err := server.Run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
}
