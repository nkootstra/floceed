// Package runtime contains the Python replay program copied into generated bundles.
package runtime

import _ "embed"

// ReplayPython is the self-contained Floci replay runtime.
//
//go:embed replay.py
var ReplayPython []byte
