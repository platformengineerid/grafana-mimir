// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/tsdb/memcache_client_config_test.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package tsdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMemcachedIndexCacheConfig_GetAddresses(t *testing.T) {
	tests := map[string]struct {
		cfg      MemcachedClientConfig
		expected []string
	}{
		"no addresses": {
			cfg: MemcachedClientConfig{
				Addresses: "",
			},
			expected: []string{},
		},
		"one address": {
			cfg: MemcachedClientConfig{
				Addresses: "dns+localhost:11211",
			},
			expected: []string{"dns+localhost:11211"},
		},
		"two addresses": {
			cfg: MemcachedClientConfig{
				Addresses: "dns+memcached-1:11211,dns+memcached-2:11211",
			},
			expected: []string{"dns+memcached-1:11211", "dns+memcached-2:11211"},
		},
	}
	for testName, testData := range tests {
		t.Run(testName, func(t *testing.T) {
			assert.Equal(t, testData.expected, testData.cfg.GetAddresses())
		})
	}
}
