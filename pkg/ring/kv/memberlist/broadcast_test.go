// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/ring/kv/memberlist/broadcast_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package memberlist

import "testing"

func TestInvalidates(t *testing.T) {
	const key = "ring"

	messages := map[string]ringBroadcast{
		"b1": {key: key, content: []string{"A", "B", "C"}, version: 1},
		"b2": {key: key, content: []string{"A", "B", "C"}, version: 2},
		"b3": {key: key, content: []string{"A"}, version: 3},
		"b4": {key: key, content: []string{"A", "B"}, version: 4},
		"b5": {key: key, content: []string{"A", "B", "D"}, version: 5},
		"b6": {key: key, content: []string{"A", "B", "C", "D"}, version: 6},
	}

	checkInvalidate(t, messages, "b2", "b1", true, false)
	checkInvalidate(t, messages, "b3", "b1", false, false)
	checkInvalidate(t, messages, "b3", "b2", false, false)
	checkInvalidate(t, messages, "b4", "b1", false, false)
	checkInvalidate(t, messages, "b4", "b2", false, false)
	checkInvalidate(t, messages, "b4", "b3", true, false)
	checkInvalidate(t, messages, "b5", "b1", false, false)
	checkInvalidate(t, messages, "b5", "b2", false, false)
	checkInvalidate(t, messages, "b5", "b3", true, false)
	checkInvalidate(t, messages, "b5", "b4", true, false)
	checkInvalidate(t, messages, "b6", "b1", true, false)
	checkInvalidate(t, messages, "b6", "b2", true, false)
	checkInvalidate(t, messages, "b6", "b3", true, false)
	checkInvalidate(t, messages, "b6", "b4", true, false)
	checkInvalidate(t, messages, "b6", "b5", true, false)
}

func checkInvalidate(t *testing.T, messages map[string]ringBroadcast, key1, key2 string, firstInvalidatesSecond, secondInvalidatesFirst bool) {
	b1, ok := messages[key1]
	if !ok {
		t.Fatal("cannot find", key1)
	}

	b2, ok := messages[key2]
	if !ok {
		t.Fatal("cannot find", key2)
	}

	if b1.Invalidates(b2) != firstInvalidatesSecond {
		t.Errorf("%s.Invalidates(%s) returned %t. %s={%v, %d}, %s={%v, %d}", key1, key2, !firstInvalidatesSecond, key1, b1.content, b1.version, key2, b2.content, b2.version)
	}

	if b2.Invalidates(b1) != secondInvalidatesFirst {
		t.Errorf("%s.Invalidates(%s) returned %t. %s={%v, %d}, %s={%v, %d}", key2, key1, !secondInvalidatesFirst, key2, b2.content, b2.version, key1, b1.content, b1.version)
	}
}
