// SPDX-License-Identifier: AGPL-3.0-only
// Provenance-includes-location: https://github.com/cortexproject/cortex/blob/master/pkg/storage/bucket/gcs/bucket_client.go
// Provenance-includes-license: Apache-2.0
// Provenance-includes-copyright: The Cortex Authors.

package gcs

import (
	"context"

	"github.com/go-kit/kit/log"
	"github.com/thanos-io/thanos/pkg/objstore"
	"github.com/thanos-io/thanos/pkg/objstore/gcs"
	yaml "gopkg.in/yaml.v2"
)

// NewBucketClient creates a new GCS bucket client
func NewBucketClient(ctx context.Context, cfg Config, name string, logger log.Logger) (objstore.Bucket, error) {
	bucketConfig := gcs.Config{
		Bucket:         cfg.BucketName,
		ServiceAccount: cfg.ServiceAccount.Value,
	}

	// Thanos currently doesn't support passing the config as is, but expects a YAML,
	// so we're going to serialize it.
	serialized, err := yaml.Marshal(bucketConfig)
	if err != nil {
		return nil, err
	}

	return gcs.NewBucket(ctx, logger, serialized, name)
}
