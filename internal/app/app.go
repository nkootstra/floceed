package app

import (
	"time"

	"github.com/nkootstra/floceed/internal/bundle"
)

const DefaultProjectFile = "floceed.yaml"

type Application struct {
	Factory          SourceFactory
	Version          string
	Now              func() time.Time
	ComposeValidator bundle.ComposeValidator
	localRuntime     localRuntime
}

func New(version string) *Application {
	return &Application{
		Factory:      AWSFactory{},
		Version:      version,
		Now:          time.Now,
		localRuntime: newDockerLocalRuntime(),
	}
}
