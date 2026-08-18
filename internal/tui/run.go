package tui

import (
	"context"
	"io"

	tea "charm.land/bubbletea/v2"
)

func Run(ctx context.Context, in io.Reader, out io.Writer, backend Backend, opts Options) error {
	m := NewModel(backend, opts)
	// NewModel creates a default cancelable context for standalone tests; Run
	// must replace both fields with a pair derived from the caller's context
	// so m.cancel() actually cancels the context the backend commands use.
	ctx, cancel := context.WithCancel(ctx)
	m.ctx, m.cancel = ctx, cancel
	defer cancel()
	_, err := tea.NewProgram(m, tea.WithContext(ctx), tea.WithInput(in), tea.WithOutput(out)).Run()
	return err
}
