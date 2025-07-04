// Copyright 2025 Prometheus Team
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cluster

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// BootManager manages the boot timeout for cluster joining.
// It provides a configurable delay before the alertmanager joins the gossip cluster,
// allowing the API server to be ready for alert ingestion while keeping the
// readiness probe in NOT READY state until the boot timeout expires.
type BootManager struct {
	timeout   time.Duration
	startTime time.Time
	readyc    chan struct{}
	logger    *slog.Logger
	once      sync.Once
}

// NewBootManager creates a new boot manager with the specified timeout.
func NewBootManager(timeout time.Duration, logger *slog.Logger) *BootManager {
	return &BootManager{
		timeout: timeout,
		readyc:  make(chan struct{}),
		logger:  logger,
	}
}

// Start begins the boot timeout period. This should be called once during startup.
// It starts a goroutine that will close the ready channel after the timeout expires.
func (bm *BootManager) Start() {
	bm.once.Do(func() {
		bm.startTime = time.Now()
		if bm.timeout <= 0 {
			// Zero or negative timeout means immediate readiness
			bm.logger.Info("Boot timeout disabled, proceeding immediately")
			close(bm.readyc)
			return
		}

		bm.logger.Info("Starting boot timeout", "timeout", bm.timeout)
		go bm.runBootTimeout()
	})
}

// IsReady returns true if the boot timeout has expired.
func (bm *BootManager) IsReady() bool {
	select {
	case <-bm.readyc:
		return true
	default:
		return false
	}
}

// WaitReady blocks until the boot timeout expires or the context is cancelled.
func (bm *BootManager) WaitReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-bm.readyc:
		return nil
	}
}

// runBootTimeout runs the boot timeout timer and logs progress.
func (bm *BootManager) runBootTimeout() {
	ticker := time.NewTicker(30 * time.Second) // Log progress every 30 seconds
	defer ticker.Stop()

	deadline := bm.startTime.Add(bm.timeout)

	for {
		select {
		case <-time.After(time.Until(deadline)):
			// Timeout expired
			elapsed := time.Since(bm.startTime)
			bm.logger.Info("Boot timeout completed, ready to join cluster", "elapsed", elapsed)
			close(bm.readyc)
			return
		case <-ticker.C:
			// Progress update
			elapsed := time.Since(bm.startTime)
			remaining := bm.timeout - elapsed
			if remaining > 0 {
				bm.logger.Info("Boot timeout in progress", "elapsed", elapsed, "remaining", remaining)
			}
		}
	}
}
