package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/nkootstra/floceed/internal/testfixture"
)

func main() {
	output := flag.String("output", "internal/cli/testdata/inspect", "directory that receives the inspect fixtures")
	flag.Parse()
	if err := testfixture.GenerateInspectFixtures(*output); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
