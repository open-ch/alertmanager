# Alertmanager Boot Timeout Implementation Plan

## Overview
Implement a configurable boot timeout for Alertmanager in HA mode only. During the timeout period, the API server will be available for alert ingestion, but the readiness probe will return NOT READY until the timeout expires and the cluster settles.

## Requirements
- ✅ **HA mode only**: Feature only applies when clustering is enabled (`--cluster.listen-address != ""`)
- ✅ **Zero impact on single replica**: No changes to single replica startup behavior
- ✅ **API available immediately**: API server accepts alerts during boot timeout
- ✅ **Readiness reflects boot state**: `/-/ready` returns 503 during boot timeout
- ✅ **Configurable timeout**: Default 5 minutes, configurable via flag
- ✅ **Comprehensive logging**: Clear progress indication for users
- ✅ **Minimal code changes**: Touch as little existing code as possible

## Implementation Steps

### Step 1: Add cluster boot timeout flag
**File**: `cmd/alertmanager/main.go`
- Add new flag: `cluster.boot-timeout` with 5m default
- Place with other cluster flags for proper namespacing
- Include clear help text indicating HA-only behavior

### Step 2: Create boot manager
**File**: `cluster/boot.go` (new file)
- Create `BootManager` struct to handle boot timeout logic
- Implement methods:
  - `NewBootManager(timeout, logger)` - constructor
  - `Start()` - begin boot timeout period
  - `IsReady()` - check if boot timeout has expired
  - `WaitReady(ctx)` - block until boot timeout expires
- Include comprehensive logging with progress updates
- Use existing patterns from cluster package

### Step 3: Modify startup sequence
**File**: `cmd/alertmanager/main.go`
- Create boot manager when clustering is enabled
- Start boot timeout before API server startup
- Move cluster join logic after boot timeout expires
- Preserve existing startup order for single replica mode

### Step 4: Update readiness endpoint
**File**: `ui/web.go`
- Modify `/-/ready` endpoint to check boot state when clustering enabled
- Return 503 (Service Unavailable) during boot timeout
- Return current behavior after boot timeout + cluster ready
- No changes for single replica mode

### Step 5: Enhance cluster status reporting
**Files**: `api/v2/api.go`, `api/v2/models/cluster_status.go`
- Add "booting" status to cluster status enum
- Update status reporting logic in API v2
- Maintain backward compatibility with existing "ready", "settling", "disabled"
- Show "booting" during boot timeout period

### Step 6: Integration and testing
- Verify single replica mode unchanged
- Test HA mode boot sequence
- Validate readiness probe behavior
- Check status API responses
- Confirm logging output

## Technical Details

### New Components
```go
// cluster/boot.go
type BootManager struct {
    timeout   time.Duration
    startTime time.Time
    readyc    chan struct{}
    logger    *slog.Logger
}
```

### Modified Startup Sequence (HA mode only)
```
1. Create cluster peer (existing)
2. Create boot manager (NEW)
3. Start boot timeout (NEW)
4. Start API server (existing, now immediate)
5. Wait for boot timeout (NEW)
6. Join cluster (existing, now delayed)
7. Wait for cluster settle (existing)
8. Set ready state (existing)
```

### Flag Addition
```go
clusterBootTimeout = kingpin.Flag("cluster.boot-timeout", 
    "Time to wait before joining the gossip cluster. During this period, "+
    "the API server accepts alerts but readiness probe returns NOT READY. "+
    "Only applies when clustering is enabled.").Default("5m").Duration()
```

### Readiness Logic
```go
// Single replica: always ready (current behavior)
// HA mode: ready only after boot timeout + cluster settled
func readyHandler(bootManager *cluster.BootManager, peer cluster.ClusterPeer) {
    if peer == nil {
        // Single replica - always ready
        return 200
    }
    if !bootManager.IsReady() {
        // Still in boot timeout
        return 503 
    }
    if !peer.Ready() {
        // Cluster not settled
        return 503
    }
    return 200
}
```

## Benefits
- **Controlled startup**: Prevents premature cluster participation
- **Alert availability**: API accepts alerts immediately
- **Clear observability**: Comprehensive logging and status reporting
- **Zero regression**: Single replica mode completely unchanged
- **Flexible configuration**: Adjustable timeout for different environments
- **Backward compatible**: Existing deployments continue working

## Files to Modify
1. `cmd/alertmanager/main.go` - Add flag, modify startup sequence
2. `cluster/boot.go` - New boot manager implementation  
3. `ui/web.go` - Update readiness endpoint
4. `api/v2/api.go` - Add boot status reporting
5. `api/v2/models/cluster_status.go` - Add "booting" status (if needed)

## Testing Scenarios
1. **Single replica**: Verify no behavior changes
2. **HA with default timeout**: Test 5-minute boot delay
3. **HA with custom timeout**: Test different timeout values
4. **HA with zero timeout**: Test immediate cluster join (current behavior)
5. **API availability**: Confirm alerts accepted during boot timeout
6. **Readiness probe**: Verify 503 → 200 transition
7. **Status API**: Check "booting" → "settling" → "ready" progression

## Risk Mitigation
- **Gradual rollout**: Feature disabled by default in single replica
- **Escape hatch**: Zero timeout maintains current behavior
- **Comprehensive logging**: Clear visibility into boot process
- **Minimal changes**: Reduces risk of introducing bugs
- **Backward compatibility**: Existing configurations work unchanged
