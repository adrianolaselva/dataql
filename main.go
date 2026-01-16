package main

import (
	"github.com/adrianolaselva/dataql/cmd"
	"fmt"
	"os"
)

func main() {
	if err := cmd.New().Execute(); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	}
}
