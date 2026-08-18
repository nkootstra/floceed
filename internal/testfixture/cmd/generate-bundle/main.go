package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/nkootstra/floceed/internal/testfixture"
)

func main() {
	output := flag.String("output", "internal/cli/testdata/inspect", "directory that receives the inspect fixtures")
	representative := flag.Bool("representative", false, "write a small representative replayable bundle instead of inspect comparison fixtures")
	flag.Parse()
	var err error
	if *representative {
		err = testfixture.GenerateRepresentativeBundle(*output)
	} else {
		err = testfixture.GenerateInspectFixtures(*output)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
