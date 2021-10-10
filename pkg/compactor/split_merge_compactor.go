// SPDX-License-Identifier: AGPL-3.0-only

package compactor

import (
	"context"

	"github.com/go-kit/kit/log"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/tsdb"
	"github.com/thanos-io/thanos/pkg/compact/downsample"
)

func splitAndMergeGrouperFactory(ctx context.Context, cfg Config, cfgProvider ConfigProvider, userID string, ownJob ownJobFunc, logger log.Logger, reg prometheus.Registerer) Grouper {
	return NewSplitAndMergeGrouper(
		userID,
		cfg.BlockRanges.ToMilliseconds(),
		uint32(cfgProvider.CompactorSplitAndMergeShards(userID)),
		ownJob,
		logger)
}

func splitAndMergeCompactorFactory(ctx context.Context, cfg Config, logger log.Logger, reg prometheus.Registerer) (Compactor, Planner, error) {
	// We don't need to customise the TSDB compactor so we're just using the Prometheus one.
	compactor, err := tsdb.NewLeveledCompactor(ctx, reg, logger, cfg.BlockRanges.ToMilliseconds(), downsample.NewPool(), nil)
	if err != nil {
		return nil, nil, err
	}

	planner := NewSplitAndMergePlanner(cfg.BlockRanges.ToMilliseconds())
	return compactor, planner, nil
}

// configureSplitAndMergeCompactor updates the provided configuration injecting the split-and-merge compactor.
func configureSplitAndMergeCompactor(cfg *Config) {
	cfg.BlocksGrouperFactory = splitAndMergeGrouperFactory
	cfg.BlocksCompactorFactory = splitAndMergeCompactorFactory
}
