package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"

	"github.com/nkootstra/floceed/internal/app"
	"github.com/nkootstra/floceed/internal/cli"
	"github.com/nkootstra/floceed/internal/tui"
	"github.com/spf13/cobra"
)

var version = "dev"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	application := app.New(version)
	backend := tui.ApplicationBackend{App: application}
	cmd := cli.New(cli.Options{
		Stdin: os.Stdin, Stdout: os.Stdout, Stderr: os.Stderr, Version: version, App: application,
		TUI: func(ctx context.Context, in io.Reader, out io.Writer, noColor bool) error {
			return tui.Run(ctx, in, out, backend, tui.Options{NoColor: noColor})
		},
	})
	os.Exit(execute(ctx, cmd, os.Stderr))
}

func execute(ctx context.Context, cmd *cobra.Command, stderr io.Writer) int {
	err := cmd.ExecuteContext(ctx)
	if err == nil {
		return 0
	}
	jsonRequested, writeErr := cli.WriteInvocationError(cmd, err)
	if jsonRequested {
		if writeErr != nil {
			fmt.Fprintln(stderr, cli.FormatError(writeErr))
		}
		return cli.ExitCode(err)
	}
	fmt.Fprintln(stderr, cli.FormatError(err))
	return cli.ExitCode(err)
}
