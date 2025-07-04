// Copyright 2024 Prometheus Team
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

package nflog

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"sync"
	"time"

	uuid "github.com/gofrs/uuid"
	"github.com/matttproud/golang_protobuf_extensions/pbutil"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/alertmanager/db"
	pb "github.com/prometheus/alertmanager/nflog/nflogpb"
)

// DBLog implements a notification log backed by a database.
type DBLog struct {
	db           db.DB
	nodeID       string
	logger       *slog.Logger
	metrics      *metrics
	mtx          sync.RWMutex
	
	retention    time.Duration
	broadcastFunc func([]byte)
	
	// Sync management
	syncInterval time.Duration
	stopc       chan struct{}
	syncWg      sync.WaitGroup
}

type DBLogOptions struct {
	DB           db.DB
	NodeID       string
	Retention    time.Duration
	Logger       *slog.Logger
	Metrics      prometheus.Registerer
	SyncInterval time.Duration
}

// NewDBLog creates a new database-backed notification log.
func NewDBLog(opts DBLogOptions) (*DBLog, error) {
	if opts.DB == nil {
		return nil, fmt.Errorf("database is required")
	}
	
	if opts.NodeID == "" {
		nodeUUID, err := uuid.NewV4()
		if err != nil {
			return nil, fmt.Errorf("failed to generate node ID: %w", err)
		}
		opts.NodeID = nodeUUID.String()
	}
	
	if opts.SyncInterval == 0 {
		opts.SyncInterval = 30 * time.Second
	}
	
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	
	l := &DBLog{
		db:           opts.DB,
		nodeID:       opts.NodeID,
		logger:       opts.Logger,
		metrics:      newMetrics(opts.Metrics),
		retention:    opts.Retention,
		syncInterval: opts.SyncInterval,
		stopc:        make(chan struct{}),
		broadcastFunc: func([]byte) {}, // No-op by default
	}
	
	// Register this node in the database
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	if err := l.db.RegisterNode(ctx, l.nodeID, ""); err != nil {
		return nil, fmt.Errorf("failed to register node: %w", err)
	}
	
	// Start background sync
	l.startSync()
	
	return l, nil
}

// startSync starts the background synchronization with the database.
func (l *DBLog) startSync() {
	l.syncWg.Add(1)
	
	go func() {
		defer l.syncWg.Done()
		ticker := time.NewTicker(l.syncInterval)
		defer ticker.Stop()
		
		for {
			select {
			case <-l.stopc:
				return
			case <-ticker.C:
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				
				// Update heartbeat
				if err := l.db.UpdateNodeHeartbeat(ctx, l.nodeID); err != nil {
					l.logger.Warn("failed to update node heartbeat", "err", err)
				}
				
				// Clean up expired entries
				cutoff := time.Now().Add(-l.retention)
				if err := l.db.DeleteExpiredNotificationEntries(ctx, cutoff); err != nil {
					l.logger.Warn("failed to clean up expired notification entries", "err", err)
				}
				
				cancel()
			}
		}
	}()
}

// Close stops the database log and cleans up resources.
func (l *DBLog) Close() error {
	close(l.stopc)
	l.syncWg.Wait()
	return nil
}

// Log records a notification entry.
func (l *DBLog) Log(r *pb.Receiver, gkey string, firingAlerts, resolvedAlerts []uint64, expiry time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	now := time.Now().UTC()
	
	expiresAt := now.Add(l.retention)
	if expiry > 0 && l.retention > expiry {
		expiresAt = now.Add(expiry)
	}
	
	entry := &pb.MeshEntry{
		Entry: &pb.Entry{
			Receiver:       r,
			GroupKey:       []byte(gkey),
			Timestamp:      now,
			FiringAlerts:   firingAlerts,
			ResolvedAlerts: resolvedAlerts,
		},
		ExpiresAt: expiresAt,
	}
	
	if err := l.db.SetNotificationEntry(ctx, entry); err != nil {
		return fmt.Errorf("failed to store notification entry: %w", err)
	}
	
	// Broadcast the change (for compatibility with existing code)
	if l.broadcastFunc != nil {
		if b, err := l.marshalMeshEntry(entry); err == nil {
			l.broadcastFunc(b)
		}
	}
	
	l.metrics.propagatedMessagesTotal.Inc()
	l.logger.Debug("logged notification", "receiver", receiverKey(r), "group_key", gkey)
	
	return nil
}

// Query retrieves notification entries using the same interface as the regular Log.
func (l *DBLog) Query(params ...QueryParam) ([]*pb.Entry, error) {
	q := &query{}
	for _, p := range params {
		if err := p(q); err != nil {
			return nil, err
		}
	}
	
	// TODO(fabxc): For now our only query mode is the most recent entry for a
	// receiver/group_key combination, to match the original implementation.
	if q.recv == nil || q.groupKey == "" {
		return nil, fmt.Errorf("no query parameters specified")
	}
	
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	// Get recent entries (last 24 hours should be sufficient for most queries)
	since := time.Now().Add(-24 * time.Hour)
	entries, err := l.db.GetNotificationEntries(ctx, since)
	if err != nil {
		return nil, fmt.Errorf("failed to get notification entries: %w", err)
	}
	
	// Find the most recent entry for this receiver/group_key combination
	var mostRecent *pb.Entry
	for _, meshEntry := range entries {
		entry := meshEntry.Entry
		
		// Filter by receiver and group key
		if receiverKey(entry.Receiver) == receiverKey(q.recv) && string(entry.GroupKey) == q.groupKey {
			if mostRecent == nil || entry.Timestamp.After(mostRecent.Timestamp) {
				mostRecent = entry
			}
		}
	}
	
	if mostRecent == nil {
		return nil, ErrNotFound
	}
	
	return []*pb.Entry{mostRecent}, nil
}

// GC runs garbage collection, removing expired notification entries.
func (l *DBLog) GC() (int, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Calculate the cutoff time for expired entries
	cutoffTime := time.Now().Add(-l.retention)

	// Delete expired notification entries
	if err := l.db.DeleteExpiredNotificationEntries(ctx, cutoffTime); err != nil {
		l.logger.Error("failed to delete expired notification entries", "err", err)
		return 0, err
	}

	l.logger.Debug("notification log garbage collection completed", "cutoff_time", cutoffTime)
	// Return 0 for count as we don't track the number deleted
	return 0, nil
}

// Snapshot writes the current notification log state to a writer.
func (l *DBLog) Snapshot(w io.Writer) (int64, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	// Get all non-expired entries
	since := time.Now().Add(-l.retention)
	entries, err := l.db.GetNotificationEntries(ctx, since)
	if err != nil {
		return 0, fmt.Errorf("failed to get notification entries: %w", err)
	}
	
	var buf bytes.Buffer
	for _, entry := range entries {
		if _, err := pbutil.WriteDelimited(&buf, entry); err != nil {
			return 0, err
		}
	}
	
	n, err := w.Write(buf.Bytes())
	return int64(n), err
}

// MarshalBinary serializes the notification log state.
func (l *DBLog) MarshalBinary() ([]byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	
	// Get all non-expired entries
	since := time.Now().Add(-l.retention)
	entries, err := l.db.GetNotificationEntries(ctx, since)
	if err != nil {
		return nil, fmt.Errorf("failed to get notification entries: %w", err)
	}
	
	var buf bytes.Buffer
	for _, entry := range entries {
		if _, err := pbutil.WriteDelimited(&buf, entry); err != nil {
			return nil, err
		}
	}
	
	return buf.Bytes(), nil
}

// Merge processes incoming notification log state.
func (l *DBLog) Merge(b []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	// Decode the incoming state
	entries, err := l.decodeState(bytes.NewReader(b))
	if err != nil {
		return err
	}
	
	now := time.Now().UTC()
	merged := false
	
	for _, entry := range entries {
		// Check if we should store this entry
		if entry.ExpiresAt.Before(now) {
			continue
		}
		
		// Store in database
		if err := l.db.SetNotificationEntry(ctx, entry); err != nil {
			l.logger.Warn("failed to merge notification entry", "receiver", entry.Entry.Receiver, "err", err)
			continue
		}
		
		merged = true
	}
	
	if merged {
		l.metrics.propagatedMessagesTotal.Inc()
	}
	
	return nil
}

// SetBroadcast sets the broadcast function (for compatibility).
func (l *DBLog) SetBroadcast(f func([]byte)) {
	l.broadcastFunc = f
}

// Maintenance performs periodic maintenance tasks.
func (l *DBLog) Maintenance(interval time.Duration, snapFile string, stopc <-chan struct{}, maintenanceFunc MaintenanceFunc) {
	if interval > 0 {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		
		for {
			select {
			case <-stopc:
				return
			case <-ticker.C:
				// Run garbage collection to remove expired entries
				if _, err := l.GC(); err != nil {
					l.logger.Warn("notification log garbage collection failed", "err", err)
				}
				
				// Take snapshot if requested
				if snapFile != "" {
					if err := l.takeSnapshot(snapFile); err != nil {
						l.logger.Warn("failed to take snapshot", "file", snapFile, "err", err)
					}
				}
				
				// Run custom maintenance function if provided
				if maintenanceFunc != nil {
					if _, err := maintenanceFunc(); err != nil {
						l.logger.Warn("maintenance function failed", "err", err)
					}
				}
			}
		}
	}
}

// Helper methods

func (l *DBLog) marshalMeshEntry(entry *pb.MeshEntry) ([]byte, error) {
	var buf bytes.Buffer
	if _, err := pbutil.WriteDelimited(&buf, entry); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (l *DBLog) decodeState(r io.Reader) ([]*pb.MeshEntry, error) {
	var entries []*pb.MeshEntry
	for {
		var entry pb.MeshEntry
		_, err := pbutil.ReadDelimited(r, &entry)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, &entry)
	}
	return entries, nil
}

func (l *DBLog) takeSnapshot(filename string) error {
	tmpFile := filename + ".tmp"
	
	f, err := os.Create(tmpFile)
	if err != nil {
		return err
	}
	defer f.Close()
	
	if _, err := l.Snapshot(f); err != nil {
		os.Remove(tmpFile)
		return err
	}
	
	if err := f.Sync(); err != nil {
		os.Remove(tmpFile)
		return err
	}
	
	return os.Rename(tmpFile, filename)
}
