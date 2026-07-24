//! Persistent state for Forge environments.
//!
//! State is stored as JSON in `<state_dir>/state.json`.  All writes
//! are atomic: write to a temporary file, fsync, then rename.

pub mod lock;

use std::{
    collections::BTreeMap,
    io::Write as _,
    path::{Path, PathBuf},
    time::{SystemTime, UNIX_EPOCH},
};

use serde::{Deserialize, Serialize};
use sha2::Digest as _;

use crate::{config::ForgeConfig, error::ForgeError};

/// Schema version for state files.
const STATE_API_VERSION: &str = "forge.praxis.dev/state/v1alpha1";

/// State file name within the state directory.
const STATE_FILE: &str = "state.json";

/// Temporary state file name for atomic writes.
const STATE_TMP: &str = "state.json.tmp";

// ---------------------------------------------------------------
// Types
// ---------------------------------------------------------------

/// Root state object persisted to `state.json`.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ForgeState {
    /// Schema version for the state file.
    pub api_version: String,
    /// Managed cluster states.
    #[serde(default)]
    pub clusters: Vec<ClusterState>,
    /// Managed service states.
    #[serde(default)]
    pub services: Vec<ServiceState>,
    /// Managed stack application states.
    #[serde(default)]
    pub stacks: Vec<StackState>,
    /// Managed container network state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub network: Option<NetworkState>,
    /// Detected container runtime name, if known.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub runtime: Option<String>,
    /// SHA-256 digest of the config that produced this state.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub config_digest: Option<String>,
    /// Description of the last mutation operation.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub last_operation: Option<LastOperation>,
    /// Values captured from stack steps, keyed by cluster then key.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub captures: BTreeMap<String, BTreeMap<String, String>>,
}

/// State of one managed KIND cluster.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ClusterState {
    /// Cluster name from the Forge config (not the KIND name).
    pub name: String,
    /// Full KIND cluster name (prefix + "-" + name).
    pub kind_name: String,
    /// kubectl context name ("kind-" + `kind_name`).
    pub context: String,
    /// Current lifecycle phase.
    pub phase: ClusterPhase,
}

/// Lifecycle phases for a managed cluster.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ClusterPhase {
    /// Cluster creation is pending.
    Pending,
    /// Cluster is being created.
    Creating,
    /// Cluster is running.
    Running,
    /// Cluster is being deleted.
    Deleting,
    /// Cluster has been deleted or failed.
    Gone,
}

/// State of the managed container network.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct NetworkState {
    /// Network name (e.g. `"{env_name}-net"`).
    pub name: String,
    /// Current lifecycle phase.
    pub phase: NetworkPhase,
    /// Discovered network CIDR (e.g. `"172.18.0.0/16"`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cidr: Option<String>,
    /// Per-cluster `MetalLB` pool allocations.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub cluster_pools: Vec<ClusterPool>,
}

/// Lifecycle phases for a managed network.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum NetworkPhase {
    /// Network is active and available.
    Active,
    /// Network has been removed.
    Gone,
}

/// A per-cluster `MetalLB` IP address pool allocation.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ClusterPool {
    /// Cluster name this pool belongs to.
    pub cluster: String,
    /// Allocated address range (e.g. `"172.18.255.231-172.18.255.250"`).
    pub range: String,
}

/// State of one managed container service.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ServiceState {
    /// Service name from the Forge config.
    pub name: String,
    /// Deterministic container name.
    pub container_name: String,
    /// Container image reference.
    pub image: String,
    /// Current lifecycle phase.
    pub phase: ServicePhase,
    /// Health observation.
    pub health: ServiceHealth,
    /// Unix epoch seconds when the service was last observed.
    pub last_observed: u64,
}

/// Lifecycle phases for a managed service.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ServicePhase {
    /// Container creation pending.
    Pending,
    /// Container starting.
    Starting,
    /// Container running normally.
    Running,
    /// Health check failed.
    Unhealthy,
    /// Container stopped.
    Stopped,
    /// Container removed.
    Gone,
}

/// Health observation for a managed service.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ServiceHealth {
    /// No health check configured or not yet probed.
    Unknown,
    /// Last health check succeeded.
    Healthy,
    /// Last health check failed.
    Unhealthy,
}

/// State of one stack application to a cluster.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct StackState {
    /// Stack name from the Forge config.
    pub name: String,
    /// Cluster this stack was applied to.
    pub cluster: String,
    /// Current lifecycle phase.
    pub phase: StackPhase,
    /// SHA-256 digest of the stack spec at last apply.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub digest: Option<String>,
    /// Unix epoch seconds of last state change.
    pub timestamp: u64,
    /// Error message if phase is `Failed`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Lifecycle phases for a managed stack application.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum StackPhase {
    /// Stack application is pending.
    Pending,
    /// Stack is being applied.
    Applying,
    /// Stack has been successfully applied.
    Applied,
    /// Stack application failed.
    Failed,
}

/// Record of the last mutation.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct LastOperation {
    /// Operation name (e.g. "cluster.create", "up", "down").
    pub operation: String,
    /// Unix epoch seconds when the operation started.
    pub timestamp: u64,
    /// Whether the operation succeeded.
    pub success: bool,
}

// ---------------------------------------------------------------
// Construction
// ---------------------------------------------------------------

/// Build a default empty state.
pub fn empty() -> ForgeState {
    ForgeState {
        api_version: STATE_API_VERSION.to_owned(),
        clusters: Vec::new(),
        services: Vec::new(),
        stacks: Vec::new(),
        network: None,
        runtime: None,
        config_digest: None,
        last_operation: None,
        captures: BTreeMap::new(),
    }
}

// ---------------------------------------------------------------
// Load / Save
// ---------------------------------------------------------------

/// Load state from the state directory.
///
/// Returns an empty state if the file does not exist.
///
/// # Errors
///
/// Returns [`ForgeError::State`] if the file exists but cannot be
/// read or parsed.
pub fn load(state_dir: &Path) -> Result<ForgeState, ForgeError> {
    let path = state_path(state_dir);
    if !path.exists() {
        return Ok(empty());
    }
    read_state(&path)
}

/// Save state atomically: write temp, fsync, rename.
///
/// # Errors
///
/// Returns [`ForgeError::State`] if any step fails.
pub fn save(state_dir: &Path, state: &ForgeState) -> Result<(), ForgeError> {
    ensure_dir(state_dir)?;
    let tmp = write_temp(state_dir, state)?;
    fsync_file(&tmp)?;
    rename_state(&tmp, &state_path(state_dir))
}

/// Ensure the state directory exists.
///
/// # Errors
///
/// Returns [`ForgeError::State`] if directory creation fails.
pub fn ensure_dir(state_dir: &Path) -> Result<(), ForgeError> {
    std::fs::create_dir_all(state_dir)
        .map_err(|e| ForgeError::State(format!("cannot create state dir {}: {e}", state_dir.display())))
}

// ---------------------------------------------------------------
// Lookups
// ---------------------------------------------------------------

/// Find a cluster in state by config name.
pub fn find_cluster<'a>(state: &'a ForgeState, name: &str) -> Option<&'a ClusterState> {
    state.clusters.iter().find(|c| c.name == name)
}

/// Find a cluster in state by config name (mutable).
pub fn find_cluster_mut<'a>(state: &'a mut ForgeState, name: &str) -> Option<&'a mut ClusterState> {
    state.clusters.iter_mut().find(|c| c.name == name)
}

/// Find a service in state by config name.
pub fn find_service<'a>(state: &'a ForgeState, name: &str) -> Option<&'a ServiceState> {
    state.services.iter().find(|s| s.name == name)
}

/// Find a service in state by config name (mutable).
pub fn find_service_mut<'a>(state: &'a mut ForgeState, name: &str) -> Option<&'a mut ServiceState> {
    state.services.iter_mut().find(|s| s.name == name)
}

/// Find a stack in state by name and cluster.
pub fn find_stack<'a>(state: &'a ForgeState, name: &str, cluster: &str) -> Option<&'a StackState> {
    state.stacks.iter().find(|s| s.name == name && s.cluster == cluster)
}

/// Find a stack in state by name and cluster (mutable).
pub fn find_stack_mut<'a>(state: &'a mut ForgeState, name: &str, cluster: &str) -> Option<&'a mut StackState> {
    state.stacks.iter_mut().find(|s| s.name == name && s.cluster == cluster)
}

/// Find a cluster's `MetalLB` pool allocation in network state.
pub fn find_cluster_pool<'a>(state: &'a ForgeState, cluster: &str) -> Option<&'a str> {
    state
        .network
        .as_ref()
        .and_then(|n| n.cluster_pools.iter().find(|p| p.cluster == cluster))
        .map(|p| p.range.as_str())
}

// ---------------------------------------------------------------
// Config digest
// ---------------------------------------------------------------

/// Compute a SHA-256 hex digest of the config for change detection.
///
/// Serializes the config to canonical JSON, then hashes the bytes.
///
/// # Errors
///
/// Returns [`ForgeError::State`] if serialization fails.
pub fn config_digest(config: &ForgeConfig) -> Result<String, ForgeError> {
    let json = serde_json::to_string(config)
        .map_err(|e| ForgeError::State(format!("cannot serialize config for digest: {e}")))?;
    let hash = sha2::Sha256::digest(json.as_bytes());
    Ok(format!("{hash:x}"))
}

// ---------------------------------------------------------------
// Timestamps
// ---------------------------------------------------------------

/// Return the current Unix epoch seconds.
pub fn now_epoch_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ---------------------------------------------------------------
// Private helpers
// ---------------------------------------------------------------

/// Build the path to the state file.
fn state_path(state_dir: &Path) -> PathBuf {
    state_dir.join(STATE_FILE)
}

/// Read and parse the state file.
fn read_state(path: &Path) -> Result<ForgeState, ForgeError> {
    let content =
        std::fs::read_to_string(path).map_err(|e| ForgeError::State(format!("cannot read {}: {e}", path.display())))?;
    serde_json::from_str(&content).map_err(|e| ForgeError::State(format!("corrupt state file {}: {e}", path.display())))
}

/// Write state to a temporary file in the state directory.
fn write_temp(state_dir: &Path, state: &ForgeState) -> Result<PathBuf, ForgeError> {
    let tmp = state_dir.join(STATE_TMP);
    let json =
        serde_json::to_string_pretty(state).map_err(|e| ForgeError::State(format!("cannot serialize state: {e}")))?;
    let mut file =
        std::fs::File::create(&tmp).map_err(|e| ForgeError::State(format!("cannot create {}: {e}", tmp.display())))?;
    file.write_all(json.as_bytes())
        .map_err(|e| ForgeError::State(format!("cannot write {}: {e}", tmp.display())))?;
    Ok(tmp)
}

/// Fsync a file by path.
fn fsync_file(path: &Path) -> Result<(), ForgeError> {
    let file = std::fs::File::open(path)
        .map_err(|e| ForgeError::State(format!("cannot open for fsync {}: {e}", path.display())))?;
    file.sync_all()
        .map_err(|e| ForgeError::State(format!("fsync failed for {}: {e}", path.display())))
}

/// Atomic rename from temp to final path.
fn rename_state(tmp: &Path, final_path: &Path) -> Result<(), ForgeError> {
    std::fs::rename(tmp, final_path).map_err(|e| {
        ForgeError::State(format!(
            "cannot rename {} to {}: {e}",
            tmp.display(),
            final_path.display()
        ))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_state_has_correct_api_version() {
        let state = empty();
        assert_eq!(state.api_version, STATE_API_VERSION, "api_version mismatch");
    }

    #[test]
    fn empty_state_round_trips_through_json() {
        let state = empty();
        let json = serde_json::to_string(&state).unwrap_or_else(|_| std::process::abort());
        let parsed: ForgeState = serde_json::from_str(&json).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert_eq!(parsed.api_version, STATE_API_VERSION, "round-trip api_version mismatch");
        assert!(parsed.clusters.is_empty(), "should have no clusters");
    }

    #[test]
    fn load_missing_returns_empty() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        let state = load(dir.path()).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert!(state.clusters.is_empty(), "missing file should yield empty state");
    }

    #[test]
    fn save_and_load_round_trip() {
        let dir = tempfile::tempdir().unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        let mut state = empty();
        state.clusters.push(ClusterState {
            name: "hub".to_owned(),
            kind_name: "forge-hub".to_owned(),
            context: "kind-forge-hub".to_owned(),
            phase: ClusterPhase::Running,
        });
        save(dir.path(), &state).unwrap_or_else(|_| std::process::abort());
        let loaded = load(dir.path()).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert_eq!(loaded.clusters.len(), 1, "should have one cluster");
        assert_eq!(
            loaded.clusters.first().map(|c| c.name.as_str()),
            Some("hub"),
            "cluster name mismatch"
        );
    }

    #[test]
    fn config_digest_produces_hex_string() {
        let yaml = crate::config::minimal_yaml();
        let dir = tempfile::tempdir().unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        let path = dir.path().join("forge.yaml");
        std::fs::write(&path, &yaml).unwrap_or_else(|_| std::process::abort());
        let cfg = crate::config::load(&path).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        let digest = config_digest(&cfg).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert_eq!(digest.len(), 64, "SHA-256 hex should be 64 chars, got {}", digest.len());
        assert!(
            digest.chars().all(|c| c.is_ascii_hexdigit()),
            "digest should be hex: {digest}"
        );
    }

    #[test]
    fn find_cluster_returns_match() {
        let mut state = empty();
        state.clusters.push(ClusterState {
            name: "hub".to_owned(),
            kind_name: "forge-hub".to_owned(),
            context: "kind-forge-hub".to_owned(),
            phase: ClusterPhase::Running,
        });
        assert!(find_cluster(&state, "hub").is_some(), "should find hub");
        assert!(find_cluster(&state, "missing").is_none(), "should not find missing");
    }

    #[test]
    fn find_cluster_mut_allows_mutation() {
        let mut state = empty();
        state.clusters.push(ClusterState {
            name: "hub".to_owned(),
            kind_name: "forge-hub".to_owned(),
            context: "kind-forge-hub".to_owned(),
            phase: ClusterPhase::Pending,
        });
        if let Some(c) = find_cluster_mut(&mut state, "hub") {
            c.phase = ClusterPhase::Running;
        }
        assert_eq!(
            find_cluster(&state, "hub").map(|c| &c.phase),
            Some(&ClusterPhase::Running),
            "phase should be updated"
        );
    }

    #[test]
    fn stack_state_round_trips_through_json() {
        let mut state = empty();
        state.stacks.push(StackState {
            name: "base".to_owned(),
            cluster: "hub".to_owned(),
            phase: StackPhase::Applied,
            digest: Some("abc123".to_owned()),
            timestamp: 1_700_000_000,
            error: None,
        });
        let json = serde_json::to_string(&state).unwrap_or_else(|_| std::process::abort());
        let parsed: ForgeState = serde_json::from_str(&json).unwrap_or_else(|_| {
            std::process::abort();
            #[expect(unreachable_code, reason = "abort prevents reaching this")]
            {
                unreachable!()
            }
        });
        assert_eq!(parsed.stacks.len(), 1, "should have one stack");
        assert_eq!(
            parsed.stacks.first().map(|s| s.name.as_str()),
            Some("base"),
            "stack name mismatch"
        );
    }

    #[test]
    fn find_stack_returns_match() {
        let mut state = empty();
        state.stacks.push(make_stack_state("base", "hub"));
        assert!(find_stack(&state, "base", "hub").is_some(), "should find base/hub");
        assert!(find_stack(&state, "base", "missing").is_none(), "wrong cluster");
        assert!(find_stack(&state, "missing", "hub").is_none(), "wrong name");
    }

    #[test]
    fn find_stack_mut_allows_mutation() {
        let mut state = empty();
        state.stacks.push(make_stack_state("base", "hub"));
        if let Some(s) = find_stack_mut(&mut state, "base", "hub") {
            s.phase = StackPhase::Applied;
        }
        assert_eq!(
            find_stack(&state, "base", "hub").map(|s| &s.phase),
            Some(&StackPhase::Applied),
            "phase should be updated"
        );
    }

    #[test]
    fn cluster_pool_roundtrip() {
        let pool = ClusterPool {
            cluster: "hub".to_owned(),
            range: "172.18.255.231-172.18.255.250".to_owned(),
        };
        let json = serde_json::to_string(&pool).unwrap_or_else(|_| std::process::abort());
        let back: ClusterPool = serde_json::from_str(&json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(back.cluster, "hub", "cluster name should survive roundtrip");
        assert_eq!(back.range, pool.range, "range should survive roundtrip");
    }

    #[test]
    fn network_state_with_pools_roundtrip() {
        let ns = NetworkState {
            name: "test-net".to_owned(),
            phase: NetworkPhase::Active,
            cidr: Some("172.18.0.0/16".to_owned()),
            cluster_pools: vec![
                ClusterPool {
                    cluster: "hub".to_owned(),
                    range: "172.18.255.231-172.18.255.250".to_owned(),
                },
                ClusterPool {
                    cluster: "spoke".to_owned(),
                    range: "172.18.255.211-172.18.255.230".to_owned(),
                },
            ],
        };
        let json = serde_json::to_string(&ns).unwrap_or_else(|_| std::process::abort());
        let back: NetworkState = serde_json::from_str(&json).unwrap_or_else(|_| std::process::abort());
        assert_eq!(back.cluster_pools.len(), 2, "both pools should roundtrip");
        assert_eq!(back.cidr.as_deref(), Some("172.18.0.0/16"), "cidr should roundtrip");
    }

    #[test]
    fn find_cluster_pool_returns_range() {
        let mut state = empty();
        state.network = Some(NetworkState {
            name: "test-net".to_owned(),
            phase: NetworkPhase::Active,
            cidr: Some("172.18.0.0/16".to_owned()),
            cluster_pools: vec![ClusterPool {
                cluster: "hub".to_owned(),
                range: "172.18.255.231-172.18.255.250".to_owned(),
            }],
        });
        assert_eq!(
            find_cluster_pool(&state, "hub"),
            Some("172.18.255.231-172.18.255.250"),
            "should find hub pool"
        );
        assert_eq!(
            find_cluster_pool(&state, "spoke"),
            None,
            "should return None for unknown"
        );
    }

    // Test Utilities

    /// Build a minimal [`StackState`] for testing.
    fn make_stack_state(name: &str, cluster: &str) -> StackState {
        StackState {
            name: name.to_owned(),
            cluster: cluster.to_owned(),
            phase: StackPhase::Pending,
            digest: None,
            timestamp: 0,
            error: None,
        }
    }
}
