package model

import "testing"

func FuzzDecodeStructureNeverPanics(f *testing.F) {
	f.Add([]byte(`{"name":"items","billing_mode":"PAY_PER_REQUEST"}`))
	f.Add([]byte{0xff, 0x00, '{', '}'})
	f.Fuzz(func(t *testing.T, payload []byte) {
		snapshot := &Snapshot{Service: "dynamodb", Structure: payload}
		_, _ = DecodeStructure[map[string]any](snapshot)
	})
}
