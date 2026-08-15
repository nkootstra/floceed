package tui

import (
	"context"
	"io"

	tea "charm.land/bubbletea/v2"
)

func Run(ctx context.Context, in io.Reader, out io.Writer, backend Backend, opts Options) error {
	m := NewModel(backend, opts)
	m.ctx = ctx
	_, err := tea.NewProgram(m, tea.WithContext(ctx), tea.WithInput(in), tea.WithOutput(out)).Run()
	return err
}
