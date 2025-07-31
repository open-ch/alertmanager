// Copyright 2018 Prometheus Team
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

package store

import (
	"compress/gzip"
	"context"
	"os"
	"testing"
	"time"

	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"

	"github.com/prometheus/alertmanager/types"
)

func TestSetGet(t *testing.T) {
	a := NewAlerts()
	alert := &types.Alert{
		UpdatedAt: time.Now(),
	}
	require.NoError(t, a.Set(alert))
	want := alert.Fingerprint()
	got, err := a.Get(want)

	require.NoError(t, err)
	require.Equal(t, want, got.Fingerprint())
}

func TestDeleteIfNotModified(t *testing.T) {
	t.Run("unmodified alert should be deleted", func(t *testing.T) {
		a := NewAlerts()
		a1 := &types.Alert{
			Alert: model.Alert{
				Labels: model.LabelSet{
					"foo": "bar",
				},
			},
			UpdatedAt: time.Now().Add(-time.Second),
		}
		require.NoError(t, a.Set(a1))

		// a1 should be deleted as it has not been modified.
		a.DeleteIfNotModified(types.AlertSlice{a1})
		got, err := a.Get(a1.Fingerprint())
		require.Equal(t, ErrNotFound, err)
		require.Nil(t, got)
	})

	t.Run("modified alert should not be deleted", func(t *testing.T) {
		a := NewAlerts()
		a1 := &types.Alert{
			Alert: model.Alert{
				Labels: model.LabelSet{
					"foo": "bar",
				},
			},
			UpdatedAt: time.Now(),
		}
		require.NoError(t, a.Set(a1))

		// Make a copy of a1 that is older, but do not put it.
		// We want to make sure a1 is not deleted.
		a2 := &types.Alert{
			Alert: model.Alert{
				Labels: model.LabelSet{
					"foo": "bar",
				},
			},
			UpdatedAt: time.Now().Add(-time.Second),
		}
		require.True(t, a2.UpdatedAt.Before(a1.UpdatedAt))
		a.DeleteIfNotModified(types.AlertSlice{a2})
		// a1 should not be deleted.
		got, err := a.Get(a1.Fingerprint())
		require.NoError(t, err)
		require.Equal(t, a1, got)

		// Make another copy of a1 that is older, but do not put it.
		// We want to make sure a2 is not deleted here either.
		a3 := &types.Alert{
			Alert: model.Alert{
				Labels: model.LabelSet{
					"foo": "bar",
				},
			},
			UpdatedAt: time.Now().Add(time.Second),
		}
		require.True(t, a3.UpdatedAt.After(a1.UpdatedAt))
		a.DeleteIfNotModified(types.AlertSlice{a3})
		// a1 should not be deleted.
		got, err = a.Get(a1.Fingerprint())
		require.NoError(t, err)
		require.Equal(t, a1, got)
	})

	t.Run("should not delete other alerts", func(t *testing.T) {
		a := NewAlerts()
		a1 := &types.Alert{
			Alert: model.Alert{
				Labels: model.LabelSet{
					"foo": "bar",
				},
			},
			UpdatedAt: time.Now(),
		}
		a2 := &types.Alert{
			Alert: model.Alert{
				Labels: model.LabelSet{
					"bar": "baz",
				},
			},
			UpdatedAt: time.Now(),
		}
		require.NoError(t, a.Set(a1))
		require.NoError(t, a.Set(a2))

		// Deleting a1 should not delete a2.
		require.NoError(t, a.DeleteIfNotModified(types.AlertSlice{a1}))
		// a1 should be deleted.
		got, err := a.Get(a1.Fingerprint())
		require.Equal(t, ErrNotFound, err)
		require.Nil(t, got)
		// a2 should not be deleted.
		got, err = a.Get(a2.Fingerprint())
		require.NoError(t, err)
		require.Equal(t, a2, got)
	})
}

func TestGC(t *testing.T) {
	now := time.Now()
	newAlert := func(key string, start, end time.Duration) *types.Alert {
		return &types.Alert{
			Alert: model.Alert{
				Labels:   model.LabelSet{model.LabelName(key): "b"},
				StartsAt: now.Add(start * time.Minute),
				EndsAt:   now.Add(end * time.Minute),
			},
		}
	}
	active := []*types.Alert{
		newAlert("b", 10, 20),
		newAlert("c", -10, 10),
	}
	resolved := []*types.Alert{
		newAlert("a", -10, -5),
		newAlert("d", -10, -1),
	}
	s := NewAlerts()
	var (
		n           int
		done        = make(chan struct{})
		ctx, cancel = context.WithCancel(context.Background())
	)
	s.SetGCCallback(func(a []types.Alert) {
		n += len(a)
		if n >= len(resolved) {
			cancel()
		}
	})
	for _, alert := range append(active, resolved...) {
		require.NoError(t, s.Set(alert))
	}
	go func() {
		s.Run(ctx, 10*time.Millisecond)
		close(done)
	}()
	select {
	case <-done:
		break
	case <-time.After(1 * time.Second):
		t.Fatal("garbage collection didn't complete in time")
	}

	for _, alert := range active {
		if _, err := s.Get(alert.Fingerprint()); err != nil {
			t.Errorf("alert %v should not have been gc'd", alert)
		}
	}
	for _, alert := range resolved {
		if _, err := s.Get(alert.Fingerprint()); err == nil {
			t.Errorf("alert %v should have been gc'd", alert)
		}
	}
	require.Len(t, resolved, n)
}

func TestAlerts_PersistAndLoadRoundTrip(t *testing.T) {
	// Create a temporary directory for the test
	tempDir := t.TempDir()
	persistenceFile := tempDir + "/alerts.json.gz"

	// Create test alerts
	now := time.Now()
	alert1 := &types.Alert{
		Alert: model.Alert{
			Labels: model.LabelSet{
				"alertname": "TestAlert1",
				"instance":  "localhost:9090",
				"job":       "prometheus",
				"severity":  "critical",
			},
			Annotations: model.LabelSet{
				"description": "Test alert 1 description",
				"summary":     "Test alert 1 summary",
			},
			StartsAt:     now.Add(-10 * time.Minute),
			EndsAt:       time.Time{}, // Active alert
			GeneratorURL: "http://localhost:9090/graph?g0.expr=up%3D%3D0",
		},
		UpdatedAt: now,
		Timeout:   false,
	}

	alert2 := &types.Alert{
		Alert: model.Alert{
			Labels: model.LabelSet{
				"alertname": "TestAlert2",
				"instance":  "localhost:9091",
				"job":       "node_exporter",
				"severity":  "warning",
			},
			Annotations: model.LabelSet{
				"description": "Test alert 2 description",
				"summary":     "Test alert 2 summary",
				"runbook_url": "https://example.com/runbook",
			},
			StartsAt:     now.Add(-5 * time.Minute),
			EndsAt:       now.Add(-1 * time.Minute), // Resolved alert
			GeneratorURL: "http://localhost:9090/graph?g1.expr=node_load1%3E1",
		},
		UpdatedAt: now.Add(-1 * time.Minute),
		Timeout:   false,
	}

	// Create alerts store and add test alerts
	originalStore := NewAlerts()
	require.NoError(t, originalStore.Set(alert1))
	require.NoError(t, originalStore.Set(alert2))

	// Verify we have 2 alerts in the original store
	originalAlerts := originalStore.List()
	require.Len(t, originalAlerts, 2)

	// Persist alerts to disk
	require.NoError(t, originalStore.PersistAlerts(persistenceFile))

	// Verify the file was created and is not empty
	fileInfo, err := os.Stat(persistenceFile)
	require.NoError(t, err)
	require.Greater(t, fileInfo.Size(), int64(0))

	// Create a new alerts store and load from disk
	newStore := NewAlerts()
	require.True(t, newStore.Empty())

	require.NoError(t, newStore.LoadAlerts(persistenceFile))

	// Verify we have the same number of alerts
	loadedAlerts := newStore.List()
	require.Len(t, loadedAlerts, 2)

	// Verify alert data consistency
	alertMap := make(map[model.Fingerprint]*types.Alert)
	for _, alert := range loadedAlerts {
		alertMap[alert.Fingerprint()] = alert
	}

	// Check alert1
	loadedAlert1, exists := alertMap[alert1.Fingerprint()]
	require.True(t, exists, "Alert1 should exist in loaded alerts")
	require.Equal(t, alert1.Labels, loadedAlert1.Labels)
	require.Equal(t, alert1.Annotations, loadedAlert1.Annotations)
	require.True(t, alert1.StartsAt.Equal(loadedAlert1.StartsAt))
	require.True(t, alert1.EndsAt.Equal(loadedAlert1.EndsAt))
	require.Equal(t, alert1.GeneratorURL, loadedAlert1.GeneratorURL)
	require.True(t, alert1.UpdatedAt.Equal(loadedAlert1.UpdatedAt))
	require.Equal(t, alert1.Timeout, loadedAlert1.Timeout)

	// Check alert2
	loadedAlert2, exists := alertMap[alert2.Fingerprint()]
	require.True(t, exists, "Alert2 should exist in loaded alerts")
	require.Equal(t, alert2.Labels, loadedAlert2.Labels)
	require.Equal(t, alert2.Annotations, loadedAlert2.Annotations)
	require.True(t, alert2.StartsAt.Equal(loadedAlert2.StartsAt))
	require.True(t, alert2.EndsAt.Equal(loadedAlert2.EndsAt))
	require.Equal(t, alert2.GeneratorURL, loadedAlert2.GeneratorURL)
	require.True(t, alert2.UpdatedAt.Equal(loadedAlert2.UpdatedAt))
	require.Equal(t, alert2.Timeout, loadedAlert2.Timeout)

	// Verify individual alert retrieval works
	retrievedAlert1, err := newStore.Get(alert1.Fingerprint())
	require.NoError(t, err)
	require.Equal(t, alert1.Labels, retrievedAlert1.Labels)

	retrievedAlert2, err := newStore.Get(alert2.Fingerprint())
	require.NoError(t, err)
	require.Equal(t, alert2.Labels, retrievedAlert2.Labels)
}

func TestAlerts_PersistAndLoadEmptyStore(t *testing.T) {
	tempDir := t.TempDir()
	persistenceFile := tempDir + "/empty_alerts.json.gz"

	// Create empty alerts store
	originalStore := NewAlerts()
	require.True(t, originalStore.Empty())

	// Persist empty store to disk
	require.NoError(t, originalStore.PersistAlerts(persistenceFile))

	// Verify the file was created
	fileInfo, err := os.Stat(persistenceFile)
	require.NoError(t, err)
	require.Greater(t, fileInfo.Size(), int64(0)) // Should still have some data due to gzip overhead

	// Load into new store
	newStore := NewAlerts()
	require.NoError(t, newStore.LoadAlerts(persistenceFile))

	// Verify the new store is also empty
	require.True(t, newStore.Empty())
	require.Len(t, newStore.List(), 0)
}

func TestAlerts_LoadNonExistentFile(t *testing.T) {
	store := NewAlerts()
	err := store.LoadAlerts("/non/existent/file.json.gz")
	require.Error(t, err)
	require.Contains(t, err.Error(), "error opening alert persistence file")
}

func TestAlerts_PersistToInvalidPath(t *testing.T) {
	store := NewAlerts()

	// Try to persist to a directory that doesn't exist
	err := store.PersistAlerts("/non/existent/directory/alerts.json.gz")
	require.Error(t, err)
	require.Contains(t, err.Error(), "error creating persistence file")
}

func TestAlerts_LoadCorruptedFile(t *testing.T) {
	tempDir := t.TempDir()
	corruptedFile := tempDir + "/corrupted.json.gz"

	// Create a file with invalid gzip content
	require.NoError(t, os.WriteFile(corruptedFile, []byte("this is not gzip data"), 0o644))

	store := NewAlerts()
	err := store.LoadAlerts(corruptedFile)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error creating gzip reader")
}

func TestAlerts_LoadInvalidJSON(t *testing.T) {
	tempDir := t.TempDir()
	invalidJSONFile := tempDir + "/invalid.json.gz"

	// Create a gzipped file with invalid JSON
	file, err := os.Create(invalidJSONFile)
	require.NoError(t, err)
	defer file.Close()

	writer := gzip.NewWriter(file)
	_, err = writer.Write([]byte("{ invalid json content"))
	require.NoError(t, err)
	require.NoError(t, writer.Close())

	store := NewAlerts()
	err = store.LoadAlerts(invalidJSONFile)
	require.Error(t, err)
	require.Contains(t, err.Error(), "error unmarshalling alerts")
}

func TestAlerts_MultipleRoundTrips(t *testing.T) {
	tempDir := t.TempDir()
	persistenceFile := tempDir + "/multi_roundtrip.json.gz"

	// Create initial alert
	now := time.Now()
	alert := &types.Alert{
		Alert: model.Alert{
			Labels: model.LabelSet{
				"alertname": "MultiRoundTripAlert",
				"instance":  "localhost:8080",
			},
			Annotations: model.LabelSet{
				"description": "Multi round trip test",
			},
			StartsAt:     now,
			EndsAt:       time.Time{},
			GeneratorURL: "http://localhost:9090/test",
		},
		UpdatedAt: now,
		Timeout:   false,
	}

	// First round trip
	store1 := NewAlerts()
	require.NoError(t, store1.Set(alert))
	require.NoError(t, store1.PersistAlerts(persistenceFile))

	// Second round trip
	store2 := NewAlerts()
	require.NoError(t, store2.LoadAlerts(persistenceFile))
	require.NoError(t, store2.PersistAlerts(persistenceFile))

	// Third round trip
	store3 := NewAlerts()
	require.NoError(t, store3.LoadAlerts(persistenceFile))

	// Verify data integrity after multiple round trips
	alerts := store3.List()
	require.Len(t, alerts, 1)
	require.Equal(t, alert.Labels, alerts[0].Labels)
	require.Equal(t, alert.Annotations, alerts[0].Annotations)
	require.True(t, alert.StartsAt.Equal(alerts[0].StartsAt))
	require.True(t, alert.UpdatedAt.Equal(alerts[0].UpdatedAt))
}
