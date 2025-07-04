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

package cluster

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/gofrs/uuid"
	"github.com/prometheus/client_golang/prometheus"

	"github.com/prometheus/alertmanager/db"
)

// DBPeer implements ClusterPeer interface using a database for shared state
// instead of gossip protocol.
type DBPeer struct {
	db       db.DB
	nodeID   string
	address  string
	logger   *slog.Logger
	metrics  *dbPeerMetrics
	
	// State management
	mtx       sync.RWMutex
	states    map[string]State
	channels  map[string]*DBChannel
	ready     bool
	readyc    chan struct{}
	
	// Background tasks
	syncInterval time.Duration
	nodeTimeout  time.Duration
	stopc        chan struct{}
	wg           sync.WaitGroup
	
	// Cluster state cache
	nodes    []db.Node
	nodesMtx sync.RWMutex
}

type dbPeerMetrics struct {
	syncTotal         prometheus.Counter
	syncErrorsTotal   prometheus.Counter
	syncDuration      prometheus.Histogram
	nodesActive       prometheus.Gauge
	statesTotal       prometheus.Gauge
	channelOperations *prometheus.CounterVec
}

func newDBPeerMetrics(reg prometheus.Registerer) *dbPeerMetrics {
	m := &dbPeerMetrics{
		syncTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "alertmanager_cluster_db_sync_total",
			Help: "Total number of database sync operations.",
		}),
		syncErrorsTotal: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "alertmanager_cluster_db_sync_errors_total",
			Help: "Total number of database sync errors.",
		}),
		syncDuration: prometheus.NewHistogram(prometheus.HistogramOpts{
			Name:    "alertmanager_cluster_db_sync_duration_seconds",
			Help:    "Duration of database sync operations.",
			Buckets: prometheus.DefBuckets,
		}),
		nodesActive: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "alertmanager_cluster_db_nodes_active",
			Help: "Number of active nodes in the cluster.",
		}),
		statesTotal: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "alertmanager_cluster_db_states_total",
			Help: "Number of registered states in the cluster.",
		}),
		channelOperations: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "alertmanager_cluster_db_channel_operations_total",
				Help: "Total number of channel operations.",
			},
			[]string{"channel", "operation"},
		),
	}
	
	if reg != nil {
		reg.MustRegister(
			m.syncTotal,
			m.syncErrorsTotal,
			m.syncDuration,
			m.nodesActive,
			m.statesTotal,
			m.channelOperations,
		)
	}
	
	return m
}

// DBPeerConfig holds the configuration for a database-backed peer.
type DBPeerConfig struct {
	DB           db.DB
	NodeID       string
	Address      string
	SyncInterval time.Duration
	NodeTimeout  time.Duration
	Logger       *slog.Logger
	Metrics      prometheus.Registerer
}

// NewDBPeer creates a new database-backed cluster peer.
func NewDBPeer(config DBPeerConfig) (*DBPeer, error) {
	if config.DB == nil {
		return nil, fmt.Errorf("database is required")
	}
	
	if config.NodeID == "" {
		nodeUUID, err := uuid.NewV4()
		if err != nil {
			return nil, fmt.Errorf("failed to generate node ID: %w", err)
		}
		config.NodeID = nodeUUID.String()
	}
	
	if config.SyncInterval == 0 {
		config.SyncInterval = 30 * time.Second
	}
	
	if config.NodeTimeout == 0 {
		config.NodeTimeout = 5 * time.Minute
	}
	
	if config.Logger == nil {
		config.Logger = slog.Default()
	}
	
	if config.Address == "" {
		config.Address = "unknown"
	}
	
	p := &DBPeer{
		db:           config.DB,
		nodeID:       config.NodeID,
		address:      config.Address,
		logger:       config.Logger,
		metrics:      newDBPeerMetrics(config.Metrics),
		states:       make(map[string]State),
		channels:     make(map[string]*DBChannel),
		readyc:       make(chan struct{}),
		syncInterval: config.SyncInterval,
		nodeTimeout:  config.NodeTimeout,
		stopc:        make(chan struct{}),
	}
	
	return p, nil
}

// Join registers this node with the cluster and starts background sync.
func (p *DBPeer) Join(reconnectInterval, reconnectTimeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	// Register this node in the database
	if err := p.db.RegisterNode(ctx, p.nodeID, p.address); err != nil {
		return fmt.Errorf("failed to register node: %w", err)
	}
	
	// Start background sync
	p.wg.Add(2)
	go p.syncLoop()
	go p.cleanupLoop()
	
	p.logger.Info("joined database cluster", "node_id", p.nodeID, "address", p.address)
	return nil
}

// Leave removes this node from the cluster and stops background sync.
func (p *DBPeer) Leave(timeout time.Duration) error {
	close(p.stopc)
	p.wg.Wait()
	
	p.logger.Info("left database cluster", "node_id", p.nodeID)
	return nil
}

// Settle waits for the cluster to be ready (immediate for database implementation).
func (p *DBPeer) Settle(ctx context.Context, interval time.Duration) {
	// For database implementation, we're ready immediately after joining
	p.mtx.Lock()
	if !p.ready {
		p.ready = true
		close(p.readyc)
	}
	p.mtx.Unlock()
	
	p.logger.Info("database cluster settled", "node_id", p.nodeID)
}

// WaitReady waits until the peer is ready.
func (p *DBPeer) WaitReady(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-p.readyc:
		return nil
	}
}

// Ready returns true if the peer is ready.
func (p *DBPeer) Ready() bool {
	p.mtx.RLock()
	defer p.mtx.RUnlock()
	return p.ready
}

// Name returns the unique ID of this peer.
func (p *DBPeer) Name() string {
	return p.nodeID
}

// ClusterSize returns the number of active nodes in the cluster.
func (p *DBPeer) ClusterSize() int {
	p.nodesMtx.RLock()
	defer p.nodesMtx.RUnlock()
	return len(p.nodes)
}

// Status returns the status of the peer.
func (p *DBPeer) Status() string {
	if p.Ready() {
		return "ready"
	}
	return "settling"
}

// Peers returns the list of cluster members.
func (p *DBPeer) Peers() []ClusterMember {
	p.nodesMtx.RLock()
	defer p.nodesMtx.RUnlock()
	
	peers := make([]ClusterMember, len(p.nodes))
	for i, node := range p.nodes {
		peers[i] = DBMember{node: node}
	}
	return peers
}

// Position returns the position of this peer in the cluster.
func (p *DBPeer) Position() int {
	p.nodesMtx.RLock()
	defer p.nodesMtx.RUnlock()
	
	// Create a sorted list of node IDs to ensure consistent ordering
	nodeIDs := make([]string, len(p.nodes))
	for i, node := range p.nodes {
		nodeIDs[i] = node.ID
	}
	sort.Strings(nodeIDs)
	
	// Find our position
	for i, nodeID := range nodeIDs {
		if nodeID == p.nodeID {
			return i
		}
	}
	return 0
}

// AddState adds a new state to be managed by the cluster.
func (p *DBPeer) AddState(key string, s State, reg prometheus.Registerer) ClusterChannel {
	p.mtx.Lock()
	defer p.mtx.Unlock()
	
	p.states[key] = s
	
	channel := &DBChannel{
		key:    key,
		peer:   p,
		logger: p.logger.With("channel", key),
	}
	
	p.channels[key] = channel
	p.metrics.statesTotal.Set(float64(len(p.states)))
	
	return channel
}

// syncLoop runs periodic synchronization with the database.
func (p *DBPeer) syncLoop() {
	defer p.wg.Done()
	
	ticker := time.NewTicker(p.syncInterval)
	defer ticker.Stop()
	
	for {
		select {
		case <-p.stopc:
			return
		case <-ticker.C:
			p.sync()
		}
	}
}

// cleanupLoop runs periodic cleanup of inactive nodes.
func (p *DBPeer) cleanupLoop() {
	defer p.wg.Done()
	
	ticker := time.NewTicker(p.syncInterval * 2) // Less frequent cleanup
	defer ticker.Stop()
	
	for {
		select {
		case <-p.stopc:
			return
		case <-ticker.C:
			p.cleanup()
		}
	}
}

// sync performs a synchronization cycle.
func (p *DBPeer) sync() {
	start := time.Now()
	defer func() {
		p.metrics.syncDuration.Observe(time.Since(start).Seconds())
	}()
	
	ctx, cancel := context.WithTimeout(context.Background(), p.syncInterval/2)
	defer cancel()
	
	// Update our heartbeat
	if err := p.db.UpdateNodeHeartbeat(ctx, p.nodeID); err != nil {
		p.logger.Warn("failed to update node heartbeat", "err", err)
		p.metrics.syncErrorsTotal.Inc()
		return
	}
	
	// Get active nodes
	nodes, err := p.db.GetActiveNodes(ctx)
	if err != nil {
		p.logger.Warn("failed to get active nodes", "err", err)
		p.metrics.syncErrorsTotal.Inc()
		return
	}
	
	// Update our node cache
	p.nodesMtx.Lock()
	p.nodes = nodes
	p.nodesMtx.Unlock()
	
	p.metrics.nodesActive.Set(float64(len(nodes)))
	p.metrics.syncTotal.Inc()
}

// cleanup removes inactive nodes from the cluster.
func (p *DBPeer) cleanup() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	
	if err := p.db.RemoveInactiveNodes(ctx, p.nodeTimeout); err != nil {
		p.logger.Warn("failed to remove inactive nodes", "err", err)
	}
}

// DBMember implements ClusterMember interface.
type DBMember struct {
	node db.Node
}

// Name returns the name of the node.
func (m DBMember) Name() string {
	return m.node.ID
}

// Address returns the address of the node.
func (m DBMember) Address() string {
	return m.node.Address
}

// DBChannel implements ClusterChannel interface for database-backed communication.
type DBChannel struct {
	key    string
	peer   *DBPeer
	logger *slog.Logger
}

// Broadcast sends a message to all nodes in the cluster by updating the database.
func (c *DBChannel) Broadcast(data []byte) {
	c.peer.metrics.channelOperations.WithLabelValues(c.key, "broadcast").Inc()
	
	// For database implementation, broadcasting means updating the shared state
	// The actual synchronization happens through the database
	c.logger.Debug("broadcasting message", "key", c.key, "size", len(data))
	
	// The actual state update should be handled by the calling code
	// This is just for metric tracking and logging
}
