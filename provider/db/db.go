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
	"log/slog"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/common/model"

	"github.com/prometheus/alertmanager/db"
	"github.com/prometheus/alertmanager/provider"
	"github.com/prometheus/alertmanager/types"
)

// Alerts provides a database-backed alert provider that implements the provider.Alerts interface.
// Unlike the memory-based provider, this stores alerts persistently in a database and
// enables high availability by sharing alert state across multiple Alertmanager instances.
type Alerts struct {
	db                  db.DB
	logger              *slog.Logger
	marker              types.AlertMarker
	cancel              context.CancelFunc
	maintenanceInterval time.Duration
}

// NewAlerts creates a new database-backed alert provider.
func NewAlerts(db db.DB, marker types.AlertMarker, logger *slog.Logger, r prometheus.Registerer) (*Alerts, error) {
	return NewAlertsWithGC(db, marker, logger, r, 15*time.Minute) // Default 15-minute cleanup interval
}

// NewAlertsWithGC creates a new database-backed alert provider with custom maintenance interval.
func NewAlertsWithGC(db db.DB, marker types.AlertMarker, logger *slog.Logger, r prometheus.Registerer, maintenanceInterval time.Duration) (*Alerts, error) {
	ctx, cancel := context.WithCancel(context.Background())
	
	a := &Alerts{
		db:                   db,
		logger:               logger.With("component", "db-provider"),
		marker:               marker,
		cancel:               cancel,
		maintenanceInterval:  maintenanceInterval,
	}

	if r != nil {
		a.registerMetrics(r)
	}

	// Start garbage collection goroutine
	go a.runGC(ctx)

	return a, nil
}

func (a *Alerts) registerMetrics(r prometheus.Registerer) {
	// Register database-specific alert metrics
	newDBAlertByStatus := func(s types.AlertState) prometheus.GaugeFunc {
		return prometheus.NewGaugeFunc(
			prometheus.GaugeOpts{
				Name:        "alertmanager_alerts",
				Help:        "How many alerts by state.",
				ConstLabels: prometheus.Labels{"state": string(s), "storage": "database"},
			},
			func() float64 {
				return float64(a.count(s))
			},
		)
	}

	r.MustRegister(newDBAlertByStatus(types.AlertStateActive))
	r.MustRegister(newDBAlertByStatus(types.AlertStateSuppressed))
	r.MustRegister(newDBAlertByStatus(types.AlertStateUnprocessed))
}

// Subscribe returns an iterator over active alerts that have not been resolved and successfully notified about.
// They are not guaranteed to be in chronological order.
func (a *Alerts) Subscribe() provider.AlertIterator {
	var (
		ch   = make(chan *types.Alert, 1000)
		done = make(chan struct{})
	)

	go func() {
		defer close(ch)
		
		// For database provider, we'll periodically fetch alerts to simulate subscription
		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		
		// Send initial alerts
		a.sendAlertsToChannel(ch, done)
		
		for {
			select {
			case <-done:
				return
			case <-ticker.C:
				a.sendAlertsToChannel(ch, done)
			}
		}
	}()

	return provider.NewAlertIterator(ch, done, nil)
}

func (a *Alerts) sendAlertsToChannel(ch chan<- *types.Alert, done <-chan struct{}) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	alerts, err := a.db.GetAlerts(ctx)
	if err != nil {
		a.logger.Error("failed to get alerts from database", "err", err)
		return
	}

	for _, dbAlert := range alerts {
		if dbAlert == nil {
			continue
		}
		
		// Convert db.Alert to types.Alert
		labels := make(model.LabelSet)
		for k, v := range dbAlert.Labels {
			labels[model.LabelName(k)] = model.LabelValue(v)
		}
		annotations := make(model.LabelSet)
		for k, v := range dbAlert.Annotations {
			annotations[model.LabelName(k)] = model.LabelValue(v)
		}
		
		alert := &types.Alert{
			Alert: model.Alert{
				Labels:       labels,
				Annotations:  annotations,
				StartsAt:     dbAlert.StartsAt,
				EndsAt:       dbAlert.EndsAt,
				GeneratorURL: dbAlert.GeneratorURL,
			},
			UpdatedAt: dbAlert.UpdatedAt,
			Timeout:   false,
		}

		select {
		case ch <- alert:
		case <-done:
			return
		}
	}
}

// GetPending returns an iterator over all the alerts that have pending notifications.
func (a *Alerts) GetPending() provider.AlertIterator {
	var (
		ch   = make(chan *types.Alert, 1000) // Use a larger buffer for database queries
		done = make(chan struct{})
	)

	go func() {
		defer close(ch)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		alerts, err := a.db.GetAlerts(ctx)
		if err != nil {
			a.logger.Error("failed to get alerts from database", "err", err)
			return
		}

		for _, dbAlert := range alerts {
			if dbAlert == nil {
				continue // Skip nil alerts
			}
			
			// Convert db.Alert to types.Alert
			labels := make(model.LabelSet)
			for k, v := range dbAlert.Labels {
				labels[model.LabelName(k)] = model.LabelValue(v)
			}
			annotations := make(model.LabelSet)
			for k, v := range dbAlert.Annotations {
				annotations[model.LabelName(k)] = model.LabelValue(v)
			}
			
			alert := &types.Alert{
				Alert: model.Alert{
					Labels:       labels,
					Annotations:  annotations,
					StartsAt:     dbAlert.StartsAt,
					EndsAt:       dbAlert.EndsAt,
					GeneratorURL: dbAlert.GeneratorURL,
				},
				UpdatedAt: dbAlert.UpdatedAt,
				Timeout:   false, // Database alerts don't timeout the same way
			}

			select {
			case ch <- alert:
			case <-done:
				return
			}
		}
	}()

	return provider.NewAlertIterator(ch, done, nil)
}

// Get returns the alert for a given fingerprint.
func (a *Alerts) Get(fp model.Fingerprint) (*types.Alert, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	alerts, err := a.db.GetAlerts(ctx)
	if err != nil {
		return nil, err
	}

	for _, dbAlert := range alerts {
		if dbAlert.Fingerprint == fp.String() {
			labels := make(model.LabelSet)
			for k, v := range dbAlert.Labels {
				labels[model.LabelName(k)] = model.LabelValue(v)
			}
			annotations := make(model.LabelSet)
			for k, v := range dbAlert.Annotations {
				annotations[model.LabelName(k)] = model.LabelValue(v)
			}
			
			alert := &types.Alert{
				Alert: model.Alert{
					Labels:       labels,
					Annotations:  annotations,
					StartsAt:     dbAlert.StartsAt,
					EndsAt:       dbAlert.EndsAt,
					GeneratorURL: dbAlert.GeneratorURL,
				},
				UpdatedAt: dbAlert.UpdatedAt,
				Timeout:   false,
			}
			return alert, nil
		}
	}

	return nil, provider.ErrNotFound
}

// Put adds the given set of alerts to the database.
// This method provides deduplication by using the fingerprint as the primary key.
func (a *Alerts) Put(alerts ...*types.Alert) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for _, alert := range alerts {
		// Convert types.Alert to db.Alert - convert LabelSet to LabelSet for db.Alert
		dbAlert := &db.Alert{
			Fingerprint:  alert.Fingerprint().String(),
			Labels:       alert.Labels,
			Annotations:  alert.Annotations,
			StartsAt:     alert.StartsAt,
			EndsAt:       alert.EndsAt,
			UpdatedAt:    alert.UpdatedAt,
			GeneratorURL: alert.GeneratorURL,
		}

		// Store in database - SetAlert handles deduplication via upsert
		if err := a.db.SetAlert(ctx, dbAlert); err != nil {
			a.logger.Error("failed to store alert in database", "err", err, "fingerprint", dbAlert.Fingerprint)
			return err
		}

		a.logger.Debug("stored alert in database", "fingerprint", dbAlert.Fingerprint, "labels", dbAlert.Labels)
	}

	return nil
}

// count returns the number of alerts with the given state.
// For database provider, we'll implement a simple count based on current time.
func (a *Alerts) count(state types.AlertState) int {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	alerts, err := a.db.GetAlerts(ctx)
	if err != nil {
		a.logger.Error("failed to get alerts for count", "err", err)
		return 0
	}

	count := 0
	now := time.Now()

	for _, dbAlert := range alerts {
		labels := make(model.LabelSet)
		for k, v := range dbAlert.Labels {
			labels[model.LabelName(k)] = model.LabelValue(v)
		}
		annotations := make(model.LabelSet)
		for k, v := range dbAlert.Annotations {
			annotations[model.LabelName(k)] = model.LabelValue(v)
		}
		
		alert := &types.Alert{
			Alert: model.Alert{
				Labels:      labels,
				Annotations: annotations,
				StartsAt:    dbAlert.StartsAt,
				EndsAt:      dbAlert.EndsAt,
			},
			UpdatedAt: dbAlert.UpdatedAt,
		}

		alertState := types.AlertStateUnprocessed
		if alert.EndsAt.Before(now) && !alert.EndsAt.IsZero() {
			// Alert is resolved
			alertState = types.AlertStateUnprocessed // Resolved alerts are not counted in active metrics
		} else {
			status := a.marker.Status(alert.Fingerprint())
			if status.State == types.AlertStateSuppressed {
				alertState = types.AlertStateSuppressed
			} else {
				alertState = types.AlertStateActive
			}
		}

		if alertState == state {
			count++
		}
	}

	return count
}

// runGC periodically removes expired alerts from the database.
func (a *Alerts) runGC(ctx context.Context) {
	ticker := time.NewTicker(a.maintenanceInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.cleanupExpiredAlerts()
		}
	}
}

func (a *Alerts) cleanupExpiredAlerts() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Delete expired alerts directly using the database method
	cutoffTime := time.Now().Add(-1 * time.Hour) // Keep alerts for 1 hour after they expire
	
	if err := a.db.DeleteExpiredAlerts(ctx, cutoffTime); err != nil {
		a.logger.Error("failed to cleanup expired alerts", "err", err)
	} else {
		a.logger.Debug("cleanup expired alerts completed", "cutoff_time", cutoffTime)
	}
}

// Close cleans up the alert provider resources.
func (a *Alerts) Close() {
	// Stop the garbage collection goroutine
	if a.cancel != nil {
		a.cancel()
	}
	// Database connections are managed by the db.DB instance
	// No additional cleanup needed here
}
