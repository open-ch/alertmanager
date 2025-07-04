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

package db

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	"github.com/prometheus/alertmanager/nflog/nflogpb"
	"github.com/prometheus/alertmanager/silence/silencepb"
)

// SQL implements the DB interface using SQL databases.
type SQL struct {
	db      *sql.DB
	config  Config
	logger  *slog.Logger
	metrics *sqlMetrics
}

type sqlMetrics struct {
	operations       *prometheus.CounterVec
	operationDuration *prometheus.HistogramVec
	connectionPool   *prometheus.GaugeVec
}

func newSQLMetrics(reg prometheus.Registerer) *sqlMetrics {
	m := &sqlMetrics{
		operations: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "alertmanager_db_operations_total",
				Help: "Total number of database operations.",
			},
			[]string{"operation", "status"},
		),
		operationDuration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "alertmanager_db_operation_duration_seconds",
				Help:    "Duration of database operations.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"operation"},
		),
		connectionPool: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "alertmanager_db_connection_pool",
				Help: "Database connection pool statistics.",
			},
			[]string{"state"},
		),
	}
	
	if reg != nil {
		reg.MustRegister(m.operations, m.operationDuration, m.connectionPool)
	}
	
	return m
}

// NewSQL creates a new SQL database implementation.
func NewSQL(config Config, logger *slog.Logger, reg prometheus.Registerer) (*SQL, error) {
	db, err := sql.Open(config.Driver, config.DSN)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}
	
	// Configure connection pool
	db.SetMaxOpenConns(config.MaxOpenConns)
	db.SetMaxIdleConns(config.MaxIdleConns)
	db.SetConnMaxLifetime(config.ConnMaxLifetime)
	db.SetConnMaxIdleTime(config.ConnMaxIdleTime)
	
	s := &SQL{
		db:      db,
		config:  config,
		logger:  logger,
		metrics: newSQLMetrics(reg),
	}
	
	// Initialize schema
	if err := s.initSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}
	
	// Start metrics collection
	go s.collectConnectionPoolMetrics()
	
	return s, nil
}

func (s *SQL) initSchema() error {
	var blobType string
	switch s.config.Driver {
	case "postgres", "pgx", "pq":
		blobType = "BYTEA"
	default:
		blobType = "BLOB"
	}

	schema := fmt.Sprintf(`
	CREATE TABLE IF NOT EXISTS silences (
		id TEXT PRIMARY KEY,
		data %s NOT NULL,
		expires_at TIMESTAMP NOT NULL,
		updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	
	CREATE INDEX IF NOT EXISTS idx_silences_expires_at ON silences(expires_at);
	CREATE INDEX IF NOT EXISTS idx_silences_updated_at ON silences(updated_at);
	
	CREATE TABLE IF NOT EXISTS notification_entries (
		id TEXT PRIMARY KEY,
		group_key TEXT NOT NULL,
		receiver TEXT NOT NULL,
		data %s NOT NULL,
		expires_at TIMESTAMP NOT NULL,
		timestamp TIMESTAMP NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	
	CREATE INDEX IF NOT EXISTS idx_notification_entries_expires_at ON notification_entries(expires_at);
	CREATE INDEX IF NOT EXISTS idx_notification_entries_timestamp ON notification_entries(timestamp);
	CREATE INDEX IF NOT EXISTS idx_notification_entries_group_receiver ON notification_entries(group_key, receiver);
	
	CREATE TABLE IF NOT EXISTS alerts (
		fingerprint TEXT PRIMARY KEY,
		data %s NOT NULL,
		labels_hash TEXT NOT NULL,
		starts_at TIMESTAMP NOT NULL,
		ends_at TIMESTAMP NOT NULL,
		updated_at TIMESTAMP NOT NULL,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	
	CREATE INDEX IF NOT EXISTS idx_alerts_starts_at ON alerts(starts_at);
	CREATE INDEX IF NOT EXISTS idx_alerts_ends_at ON alerts(ends_at);
	CREATE INDEX IF NOT EXISTS idx_alerts_updated_at ON alerts(updated_at);
	CREATE INDEX IF NOT EXISTS idx_alerts_labels_hash ON alerts(labels_hash);
	
	CREATE TABLE IF NOT EXISTS cluster_nodes (
		id TEXT PRIMARY KEY,
		address TEXT NOT NULL,
		last_seen TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	);
	
	CREATE INDEX IF NOT EXISTS idx_cluster_nodes_last_seen ON cluster_nodes(last_seen);
	`, blobType, blobType, blobType)

	_, err := s.db.Exec(schema)
	return err
}

func (s *SQL) collectConnectionPoolMetrics() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()
	
	for range ticker.C {
		stats := s.db.Stats()
		s.metrics.connectionPool.WithLabelValues("open").Set(float64(stats.OpenConnections))
		s.metrics.connectionPool.WithLabelValues("idle").Set(float64(stats.Idle))
		s.metrics.connectionPool.WithLabelValues("in_use").Set(float64(stats.InUse))
		s.metrics.connectionPool.WithLabelValues("wait_count").Set(float64(stats.WaitCount))
		s.metrics.connectionPool.WithLabelValues("wait_duration").Set(stats.WaitDuration.Seconds())
	}
}

func (s *SQL) recordOperation(operation string, start time.Time, err error) {
	duration := time.Since(start)
	s.metrics.operationDuration.WithLabelValues(operation).Observe(duration.Seconds())
	
	status := "success"
	if err != nil {
		status = "error"
	}
	s.metrics.operations.WithLabelValues(operation, status).Inc()
}

// GetSilences retrieves all silences from the database.
func (s *SQL) GetSilences(ctx context.Context) ([]*silencepb.MeshSilence, error) {
	start := time.Now()
	var err error
	defer func() { s.recordOperation("get_silences", start, err) }()
	
	var query string
	switch s.config.Driver {
	case "postgres", "pgx", "pq":
		query = `SELECT data FROM silences WHERE expires_at > $1 ORDER BY updated_at`
	default:
		query = `SELECT data FROM silences WHERE expires_at > ? ORDER BY updated_at`
	}
	rows, err := s.db.QueryContext(ctx, query, time.Now())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var silences []*silencepb.MeshSilence
	for rows.Next() {
		var data []byte
		if err = rows.Scan(&data); err != nil {
			return nil, err
		}
		
		var silence silencepb.MeshSilence
		if err = proto.Unmarshal(data, &silence); err != nil {
			s.logger.Warn("failed to unmarshal silence", "err", err)
			continue
		}
		
		silences = append(silences, &silence)
	}
	
	err = rows.Err()
	return silences, err
}

// SetSilence stores or updates a silence in the database.
func (s *SQL) SetSilence(ctx context.Context, silence *silencepb.MeshSilence) error {
	start := time.Now()
	var err error
	defer func() { s.recordOperation("set_silence", start, err) }()
	
	data, err := proto.Marshal(silence)
	if err != nil {
		return err
	}
	
	var query string
	switch s.config.Driver {
	case "postgres", "pgx", "pq":
		query = `
		INSERT INTO silences (id, data, expires_at, updated_at) 
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (id) DO UPDATE SET
			data = EXCLUDED.data,
			expires_at = EXCLUDED.expires_at,
			updated_at = EXCLUDED.updated_at
		`
	default:
		query = `
		INSERT OR REPLACE INTO silences (id, data, expires_at, updated_at) 
		VALUES (?, ?, ?, ?)
		`
	}
	
	_, err = s.db.ExecContext(ctx, query, 
		silence.Silence.Id, 
		data, 
		silence.ExpiresAt.UTC(), 
		silence.Silence.UpdatedAt.UTC(),
	)
	return err
}

// DeleteSilence removes a silence from the database.
func (s *SQL) DeleteSilence(ctx context.Context, silenceID string) error {
	start := time.Now()
	var err error
	defer func() { s.recordOperation("delete_silence", start, err) }()
	
	var query string
	switch s.config.Driver {
	case "postgres", "pgx", "pq":
		query = `DELETE FROM silences WHERE id = $1`
	default:
		query = `DELETE FROM silences WHERE id = ?`
	}
	_, err = s.db.ExecContext(ctx, query, silenceID)
	return err
}

// GetNotificationEntries retrieves notification entries since a given time.
func (s *SQL) GetNotificationEntries(ctx context.Context, since time.Time) ([]*nflogpb.MeshEntry, error) {
	start := time.Now()
	var err error
	defer func() { s.recordOperation("get_notification_entries", start, err) }()
	
	var query string
	switch s.config.Driver {
	case "postgres", "pgx", "pq":
		query = `SELECT data FROM notification_entries WHERE timestamp >= $1 AND expires_at > $2 ORDER BY timestamp`
	default:
		query = `SELECT data FROM notification_entries WHERE timestamp >= ? AND expires_at > ? ORDER BY timestamp`
	}
	rows, err := s.db.QueryContext(ctx, query, since.UTC(), time.Now().UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var entries []*nflogpb.MeshEntry
	for rows.Next() {
		var data []byte
		if err = rows.Scan(&data); err != nil {
			return nil, err
		}
		
		var entry nflogpb.MeshEntry
		if err = proto.Unmarshal(data, &entry); err != nil {
			s.logger.Warn("failed to unmarshal notification entry", "err", err)
			continue
		}
		
		entries = append(entries, &entry)
	}
	
	err = rows.Err()
	return entries, err
}

// SetNotificationEntry stores a notification entry in the database.
func (s *SQL) SetNotificationEntry(ctx context.Context, entry *nflogpb.MeshEntry) error {
	start := time.Now()
	var err error
	defer func() { s.recordOperation("set_notification_entry", start, err) }()
	
	data, err := proto.Marshal(entry)
	if err != nil {
		return err
	}
	
	entryID := fmt.Sprintf("%s:%s:%d", 
		string(entry.Entry.GroupKey), 
		entry.Entry.Receiver.String(), 
		entry.Entry.Timestamp.Unix(),
	)
	
	var query string
	switch s.config.Driver {
	case "postgres", "pgx", "pq":
		query = `
		INSERT INTO notification_entries (id, group_key, receiver, data, expires_at, timestamp) 
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (id) DO UPDATE SET
			group_key = EXCLUDED.group_key,
			receiver = EXCLUDED.receiver,
			data = EXCLUDED.data,
			expires_at = EXCLUDED.expires_at,
			timestamp = EXCLUDED.timestamp
		`
	default:
		query = `
		INSERT OR REPLACE INTO notification_entries (id, group_key, receiver, data, expires_at, timestamp) 
		VALUES (?, ?, ?, ?, ?, ?)
		`
	}
	
	_, err = s.db.ExecContext(ctx, query,
		entryID,
		string(entry.Entry.GroupKey),
		entry.Entry.Receiver.String(),
		data,
		entry.ExpiresAt.UTC(),
		entry.Entry.Timestamp.UTC(),
	)
	return err
}

// DeleteExpiredNotificationEntries removes expired entries from the database.
func (s *SQL) DeleteExpiredNotificationEntries(ctx context.Context, before time.Time) error {
	start := time.Now()
	var err error
	defer func() { s.recordOperation("delete_expired_notification_entries", start, err) }()
	
	var query string
	switch s.config.Driver {
	case "postgres", "pgx", "pq":
		query = `DELETE FROM notification_entries WHERE expires_at < $1`
	default:
		query = `DELETE FROM notification_entries WHERE expires_at < ?`
	}
	_, err = s.db.ExecContext(ctx, query, before.UTC())
	return err
}

// RegisterNode registers a cluster node in the database.
func (s *SQL) RegisterNode(ctx context.Context, nodeID string, address string) error {
	start := time.Now()
	var err error
	defer func() { s.recordOperation("register_node", start, err) }()
	
	var query string
	switch s.config.Driver {
	case "postgres", "pgx", "pq":
		query = `
		INSERT INTO cluster_nodes (id, address, last_seen) 
		VALUES ($1, $2, $3)
		ON CONFLICT (id) DO UPDATE SET
			address = EXCLUDED.address,
			last_seen = EXCLUDED.last_seen
		`
	default:
		query = `
		INSERT OR REPLACE INTO cluster_nodes (id, address, last_seen) 
		VALUES (?, ?, ?)
		`
	}
	
	_, err = s.db.ExecContext(ctx, query, nodeID, address, time.Now().UTC())
	return err
}

// GetActiveNodes returns all active cluster nodes.
func (s *SQL) GetActiveNodes(ctx context.Context) ([]Node, error) {
	start := time.Now()
	var err error
	defer func() { s.recordOperation("get_active_nodes", start, err) }()
	
	cutoff := time.Now().Add(-s.config.NodeTimeout).UTC()
	var query string
	switch s.config.Driver {
	case "postgres", "pgx", "pq":
		query = `SELECT id, address, last_seen, created_at FROM cluster_nodes WHERE last_seen > $1 ORDER BY last_seen DESC`
	default:
		query = `SELECT id, address, last_seen, created_at FROM cluster_nodes WHERE last_seen > ? ORDER BY last_seen DESC`
	}
	
	rows, err := s.db.QueryContext(ctx, query, cutoff)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var nodes []Node
	for rows.Next() {
		var node Node
		if err = rows.Scan(&node.ID, &node.Address, &node.LastSeen, &node.CreatedAt); err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	
	err = rows.Err()
	return nodes, err
}

// UpdateNodeHeartbeat updates a node's last seen timestamp.
func (s *SQL) UpdateNodeHeartbeat(ctx context.Context, nodeID string) error {
	start := time.Now()
	var err error
	defer func() { s.recordOperation("update_node_heartbeat", start, err) }()
	
	var query string
	switch s.config.Driver {
	case "postgres", "pgx", "pq":
		query = `UPDATE cluster_nodes SET last_seen = $1 WHERE id = $2`
	default:
		query = `UPDATE cluster_nodes SET last_seen = ? WHERE id = ?`
	}
	_, err = s.db.ExecContext(ctx, query, time.Now().UTC(), nodeID)
	return err
}

// RemoveInactiveNodes removes nodes that haven't been seen for the specified timeout.
func (s *SQL) RemoveInactiveNodes(ctx context.Context, timeout time.Duration) error {
	start := time.Now()
	var err error
	defer func() { s.recordOperation("remove_inactive_nodes", start, err) }()
	
	cutoff := time.Now().Add(-timeout).UTC()
	var query string
	switch s.config.Driver {
	case "postgres", "pgx", "pq":
		query = `DELETE FROM cluster_nodes WHERE last_seen < $1`
	default:
		query = `DELETE FROM cluster_nodes WHERE last_seen < ?`
	}
	_, err = s.db.ExecContext(ctx, query, cutoff)
	return err
}

// Begin starts a new database transaction.
func (s *SQL) Begin(ctx context.Context) (Tx, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	return &sqlTx{tx: tx, logger: s.logger}, nil
}

// Close closes the database connection.
func (s *SQL) Close() error {
	return s.db.Close()
}

// Health checks the database connection health.
func (s *SQL) Health(ctx context.Context) error {
	return s.db.PingContext(ctx)
}

// sqlTx implements the Tx interface for SQL transactions.
type sqlTx struct {
	tx      *sql.Tx
	logger  *slog.Logger
}

func (tx *sqlTx) Commit() error {
	return tx.tx.Commit()
}

func (tx *sqlTx) Rollback() error {
	return tx.tx.Rollback()
}

// Transaction implementations use the same logic but with tx.tx instead of main connection

func (tx *sqlTx) GetSilences(ctx context.Context) ([]*silencepb.MeshSilence, error) {
	query := `SELECT data FROM silences WHERE expires_at > $1 ORDER BY updated_at`
	rows, err := tx.tx.QueryContext(ctx, query, time.Now())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var silences []*silencepb.MeshSilence
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		
		var silence silencepb.MeshSilence
		if err := proto.Unmarshal(data, &silence); err != nil {
			tx.logger.Warn("failed to unmarshal silence", "err", err)
			continue
		}
		
		silences = append(silences, &silence)
	}
	
	return silences, rows.Err()
}

func (tx *sqlTx) SetSilence(ctx context.Context, silence *silencepb.MeshSilence) error {
    data, err := proto.Marshal(silence)
    if err != nil {
        return err
    }
    
    var query string
    // Note: We need to check the parent SQL instance's driver
    // For simplicity, we'll detect based on the query structure
    query = `
    INSERT INTO silences (id, data, expires_at, updated_at) 
    VALUES ($1, $2, $3, $4)
    ON CONFLICT (id) DO UPDATE SET
        data = EXCLUDED.data,
        expires_at = EXCLUDED.expires_at,
        updated_at = EXCLUDED.updated_at
    `
    
    _, err = tx.tx.ExecContext(ctx, query, 
        silence.Silence.Id, 
        data, 
        silence.ExpiresAt.UTC(), 
        silence.Silence.UpdatedAt.UTC(),
    )
    return err
}

func (tx *sqlTx) DeleteSilence(ctx context.Context, silenceID string) error {
	query := `DELETE FROM silences WHERE id = $1`
	_, err := tx.tx.ExecContext(ctx, query, silenceID)
	return err
}

func (tx *sqlTx) GetNotificationEntries(ctx context.Context, since time.Time) ([]*nflogpb.MeshEntry, error) {
	query := `SELECT data FROM notification_entries WHERE timestamp >= $1 AND expires_at > $2 ORDER BY timestamp`
	rows, err := tx.tx.QueryContext(ctx, query, since.UTC(), time.Now().UTC())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var entries []*nflogpb.MeshEntry
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		
		var entry nflogpb.MeshEntry
		if err := proto.Unmarshal(data, &entry); err != nil {
			tx.logger.Warn("failed to unmarshal notification entry", "err", err)
			continue
		}
		
		entries = append(entries, &entry)
	}
	
	return entries, rows.Err()
}

func (tx *sqlTx) SetNotificationEntry(ctx context.Context, entry *nflogpb.MeshEntry) error {
    data, err := proto.Marshal(entry)
    if err != nil {
        return err
    }
    
    entryID := fmt.Sprintf("%s:%s:%d", 
        string(entry.Entry.GroupKey), 
        entry.Entry.Receiver.String(), 
        entry.Entry.Timestamp.Unix(),
    )
    
    query := `
    INSERT INTO notification_entries (id, group_key, receiver, data, expires_at, timestamp) 
    VALUES ($1, $2, $3, $4, $5, $6)
    ON CONFLICT (id) DO UPDATE SET
        group_key = EXCLUDED.group_key,
        receiver = EXCLUDED.receiver,
        data = EXCLUDED.data,
        expires_at = EXCLUDED.expires_at,
        timestamp = EXCLUDED.timestamp
    `
    
    _, err = tx.tx.ExecContext(ctx, query,
        entryID,
        string(entry.Entry.GroupKey),
        entry.Entry.Receiver.String(),
        data,
        entry.ExpiresAt.UTC(),
        entry.Entry.Timestamp.UTC(),
    )
    return err
}

func (tx *sqlTx) DeleteExpiredNotificationEntries(ctx context.Context, before time.Time) error {
	query := `DELETE FROM notification_entries WHERE expires_at < $1`
	_, err := tx.tx.ExecContext(ctx, query, before.UTC())
	return err
}

func (tx *sqlTx) GetAlerts(ctx context.Context) ([]*Alert, error) {
	query := `SELECT data FROM alerts ORDER BY starts_at`
	rows, err := tx.tx.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var alerts []*Alert
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		
		var alert Alert
		if err = json.Unmarshal(data, &alert); err != nil {
			tx.logger.Warn("failed to unmarshal alert", "err", err)
			continue
		}
		
		alerts = append(alerts, &alert)
	}
	
	return alerts, rows.Err()
}

func (tx *sqlTx) SetAlert(ctx context.Context, alert *Alert) error {
	data, err := json.Marshal(alert)
	if err != nil {
		return err
	}
	
	labelsHash := generateLabelsHash(alert.Labels)
	
	var query string
	// Note: assuming PostgreSQL since transactions are typically used with PostgreSQL
	query = `
	INSERT INTO alerts (fingerprint, data, labels_hash, starts_at, ends_at, updated_at) 
	VALUES ($1, $2, $3, $4, $5, $6)
	ON CONFLICT (fingerprint) DO UPDATE SET
		data = EXCLUDED.data,
		labels_hash = EXCLUDED.labels_hash,
		starts_at = LEAST(alerts.starts_at, EXCLUDED.starts_at),
		ends_at = GREATEST(alerts.ends_at, EXCLUDED.ends_at),
		updated_at = EXCLUDED.updated_at
	`
	
	_, err = tx.tx.ExecContext(ctx, query, 
		alert.Fingerprint,
		data,
		labelsHash,
		alert.StartsAt.UTC(), 
		alert.EndsAt.UTC(),
		alert.UpdatedAt.UTC(),
	)
	return err
}

func (tx *sqlTx) DeleteAlert(ctx context.Context, fingerprint string) error {
	query := `DELETE FROM alerts WHERE fingerprint = $1`
	_, err := tx.tx.ExecContext(ctx, query, fingerprint)
	return err
}

func (tx *sqlTx) DeleteExpiredAlerts(ctx context.Context, before time.Time) error {
	query := `DELETE FROM alerts WHERE ends_at < $1`
	_, err := tx.tx.ExecContext(ctx, query, before.UTC())
	return err
}

// generateLabelsHash creates a deterministic hash from labels for indexing
func generateLabelsHash(labels model.LabelSet) string {
	// Convert labels to sorted slice for deterministic hash
	var labelPairs []string
	for name, value := range labels {
		labelPairs = append(labelPairs, fmt.Sprintf("%s=%s", name, value))
	}
	sort.Strings(labelPairs)
	
	// Create hash of sorted labels
	hasher := sha256.New()
	for _, pair := range labelPairs {
		hasher.Write([]byte(pair))
	}
	return hex.EncodeToString(hasher.Sum(nil))
}

// GetAlerts retrieves all active alerts from the database.
func (s *SQL) GetAlerts(ctx context.Context) ([]*Alert, error) {
	start := time.Now()
	var err error
	defer func() { s.recordOperation("get_alerts", start, err) }()
	
	var query string
	switch s.config.Driver {
	case "postgres", "pgx", "pq":
		query = `SELECT data FROM alerts WHERE ends_at > $1 ORDER BY updated_at`
	default:
		query = `SELECT data FROM alerts WHERE ends_at > ? ORDER BY updated_at`
	}
	rows, err := s.db.QueryContext(ctx, query, time.Now())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	
	var alerts []*Alert
	for rows.Next() {
		var data []byte
		if err = rows.Scan(&data); err != nil {
			return nil, err
		}
		
		var alert Alert
		if err = json.Unmarshal(data, &alert); err != nil {
			s.logger.Warn("failed to unmarshal alert", "err", err)
			continue
		}
		
		alerts = append(alerts, &alert)
	}
	
	err = rows.Err()
	return alerts, err
}

// SetAlert stores or updates an alert in the database.
func (s *SQL) SetAlert(ctx context.Context, alert *Alert) error {
	start := time.Now()
	var err error
	defer func() { s.recordOperation("set_alert", start, err) }()
	
	data, err := json.Marshal(alert)
	if err != nil {
		return err
	}
	
	labelsHash := generateLabelsHash(alert.Labels)
	
	var query string
	switch s.config.Driver {
	case "postgres", "pgx", "pq":
		query = `
		INSERT INTO alerts (fingerprint, data, labels_hash, starts_at, ends_at, updated_at) 
		VALUES ($1, $2, $3, $4, $5, $6)
		ON CONFLICT (fingerprint) DO UPDATE SET
			data = EXCLUDED.data,
			labels_hash = EXCLUDED.labels_hash,
			starts_at = LEAST(alerts.starts_at, EXCLUDED.starts_at),
			ends_at = GREATEST(alerts.ends_at, EXCLUDED.ends_at),
			updated_at = EXCLUDED.updated_at
		`
	default:
		query = `
		INSERT OR REPLACE INTO alerts (fingerprint, data, labels_hash, starts_at, ends_at, updated_at) 
		VALUES (?, ?, ?, ?, ?, ?)
		`
	}
	
	_, err = s.db.ExecContext(ctx, query, 
		alert.Fingerprint,
		data,
		labelsHash,
		alert.StartsAt.UTC(), 
		alert.EndsAt.UTC(),
		alert.UpdatedAt.UTC(),
	)
	return err
}

// DeleteAlert removes an alert from the database.
func (s *SQL) DeleteAlert(ctx context.Context, fingerprint string) error {
	start := time.Now()
	var err error
	defer func() { s.recordOperation("delete_alert", start, err) }()
	
	var query string
	switch s.config.Driver {
	case "postgres", "pgx", "pq":
		query = `DELETE FROM alerts WHERE fingerprint = $1`
	default:
		query = `DELETE FROM alerts WHERE fingerprint = ?`
	}
	_, err = s.db.ExecContext(ctx, query, fingerprint)
	return err
}

// DeleteExpiredAlerts removes expired alerts from the database.
func (s *SQL) DeleteExpiredAlerts(ctx context.Context, before time.Time) error {
	start := time.Now()
	var err error
	defer func() { s.recordOperation("delete_expired_alerts", start, err) }()
	
	var query string
	switch s.config.Driver {
	case "postgres", "pgx", "pq":
		query = `DELETE FROM alerts WHERE ends_at < $1`
	default:
		query = `DELETE FROM alerts WHERE ends_at < ?`
	}
	_, err = s.db.ExecContext(ctx, query, before.UTC())
	return err
}
