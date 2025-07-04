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

// Package db provides database interfaces and implementations for shared state
// storage in Alertmanager clusters.
package db

import (
	"context"
	"time"

	"github.com/prometheus/common/model"
	
	"github.com/prometheus/alertmanager/nflog/nflogpb"
	"github.com/prometheus/alertmanager/silence/silencepb"
)

// Alert represents an alert stored in the database
type Alert struct {
	// Unique fingerprint for the alert (based on labels)
	Fingerprint string `json:"fingerprint"`
	// Labels associated with the alert
	Labels model.LabelSet `json:"labels"`
	// Annotations for the alert
	Annotations model.LabelSet `json:"annotations"`
	// StartsAt is when the alert started firing
	StartsAt time.Time `json:"startsAt"`
	// EndsAt is when the alert stopped firing (or expected to stop)
	EndsAt time.Time `json:"endsAt"`
	// GeneratorURL identifies the source of this alert
	GeneratorURL string `json:"generatorURL"`
	// UpdatedAt is the last time this alert was updated
	UpdatedAt time.Time `json:"updatedAt"`
	// Timeout indicates if this alert has timed out
	Timeout bool `json:"timeout"`
}

// DB defines the interface for database operations supporting shared state storage.
type DB interface {
	// Silences operations
	GetSilences(ctx context.Context) ([]*silencepb.MeshSilence, error)
	SetSilence(ctx context.Context, silence *silencepb.MeshSilence) error
	DeleteSilence(ctx context.Context, silenceID string) error
	
	// Notification log operations
	GetNotificationEntries(ctx context.Context, since time.Time) ([]*nflogpb.MeshEntry, error)
	SetNotificationEntry(ctx context.Context, entry *nflogpb.MeshEntry) error
	DeleteExpiredNotificationEntries(ctx context.Context, before time.Time) error
	
	// Alert operations - for shared alert storage and deduplication
	GetAlerts(ctx context.Context) ([]*Alert, error)
	SetAlert(ctx context.Context, alert *Alert) error
	DeleteAlert(ctx context.Context, fingerprint string) error
	DeleteExpiredAlerts(ctx context.Context, before time.Time) error
	
	// Cluster management
	RegisterNode(ctx context.Context, nodeID string, address string) error
	GetActiveNodes(ctx context.Context) ([]Node, error)
	UpdateNodeHeartbeat(ctx context.Context, nodeID string) error
	RemoveInactiveNodes(ctx context.Context, timeout time.Duration) error
	
	// Transaction support
	Begin(ctx context.Context) (Tx, error)
	
	// Connection management
	Close() error
	Health(ctx context.Context) error
}

// Tx represents a database transaction.
type Tx interface {
	Commit() error
	Rollback() error
	
	// Same operations as DB but within transaction context
	GetSilences(ctx context.Context) ([]*silencepb.MeshSilence, error)
	SetSilence(ctx context.Context, silence *silencepb.MeshSilence) error
	DeleteSilence(ctx context.Context, silenceID string) error
	GetNotificationEntries(ctx context.Context, since time.Time) ([]*nflogpb.MeshEntry, error)
	SetNotificationEntry(ctx context.Context, entry *nflogpb.MeshEntry) error
	DeleteExpiredNotificationEntries(ctx context.Context, before time.Time) error
	GetAlerts(ctx context.Context) ([]*Alert, error)
	SetAlert(ctx context.Context, alert *Alert) error
	DeleteAlert(ctx context.Context, fingerprint string) error
	DeleteExpiredAlerts(ctx context.Context, before time.Time) error
}

// Node represents a cluster node.
type Node struct {
	ID        string
	Address   string
	LastSeen  time.Time
	CreatedAt time.Time
}

// Config holds database configuration.
type Config struct {
	Driver   string            `yaml:"driver"`
	DSN      string            `yaml:"dsn"`
	Options  map[string]string `yaml:"options"`
	
	// Connection pool settings
	MaxOpenConns    int           `yaml:"max_open_conns"`
	MaxIdleConns    int           `yaml:"max_idle_conns"`
	ConnMaxLifetime time.Duration `yaml:"conn_max_lifetime"`
	ConnMaxIdleTime time.Duration `yaml:"conn_max_idle_time"`
	
	// Cluster settings
	NodeTimeout     time.Duration `yaml:"node_timeout"`
	SyncInterval    time.Duration `yaml:"sync_interval"`
}

// DefaultConfig returns a default database configuration.
func DefaultConfig() Config {
	return Config{
		Driver:          "sqlite3",
		DSN:             "file:alertmanager.db?cache=shared&mode=rwc",
		MaxOpenConns:    25,
		MaxIdleConns:    5,
		ConnMaxLifetime: 30 * time.Minute,
		ConnMaxIdleTime: 5 * time.Minute,
		NodeTimeout:     5 * time.Minute,
		SyncInterval:    30 * time.Second,
	}
}
