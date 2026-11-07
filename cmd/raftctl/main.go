package main

import (
	"fmt"
	"io"
	"os"
)

func run(w io.Writer) {
	fmt.Fprintln(w, "raftctl: client not yet implemented")
}

func main() {
	run(os.Stderr)
	os.Exit(1)
}
