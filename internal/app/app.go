package app

import (
	"time"

	"github.com/nkootstra/floceed/internal/bundle"
	"github.com/nkootstra/floceed/internal/captureledger"
)

const DefaultProjectFile = "floceed.yaml"

type Application struct {
	Factory          SourceFactory
	Version          string
	Now              func() time.Time
	ComposeValidator bundle.ComposeValidator
	localRuntime     localRuntime
	publishLedger    func(*captureledger.Store, captureledger.Generation, string) error
}

func New(version string) *Application {
	return &Application{
		Factory:      AWSFactory{},
		Version:      version,
		Now:          time.Now,
		localRuntime: newDockerLocalRuntime(),
		publishLedger: func(store *captureledger.Store, generation captureledger.Generation, root string) error {
			return store.Publish(generation, root)
		},
	}
}
