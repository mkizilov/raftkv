package main

import (
	"fmt"
	"io"
	"os"
)

func run(w io.Writer) {
	fmt.Fprintln(w, "raftkv-server: server not yet implemented")
}

func main() {
	run(os.Stderr)
	os.Exit(1)
}
