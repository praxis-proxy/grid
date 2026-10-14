//! Per-request site selection: `on_request` over a snapshot of N measured sites, prefix affinity off.

use std::{
    collections::HashMap,
    pin::pin,
    sync::{Arc, LazyLock},
    task::{Context, Poll, Waker},
    time::Instant,
};

use ai_grid_filters::{
    AdmissionState, CapabilityKind, ClusterHealth, RouteCandidate, RouteSnapshot, load_serving_config,
    register_grid_filters, spawn_grid_routing,
};
use arc_swap::ArcSwap;
use divan::Bencher;
use http::{HeaderName, HeaderValue, Method, Uri};
use praxis_core::{id::IdGenerator, time::SystemTimeSource};
use praxis_filter::{
    AnyFilter, BodyMode, FilterAction, FilterError, FilterRegistry, HttpFilter, HttpFilterContext, Request,
    RequestExtensions, SubRequestResponseMode,
};

/// The request id source every call shares.
static IDS: LazyLock<IdGenerator> = LazyLock::new(|| IdGenerator::with_seed(0));

/// `sites` measured sites serving `llama`, each with room and a distinct rho.
fn snapshot(sites: u32) -> RouteSnapshot {
    let candidates = (1..=sites)
        .map(|index| {
            let site: Arc<str> = Arc::from(format!("site-{index}"));
            RouteCandidate {
                admission_state: AdmissionState::NewAndExisting,
                capacity: Some(8.0),
                rho: Some(f64::from(index) / f64::from(sites.saturating_add(1))),
                backlog: None,
                full: None,
                relieved: None,
                cluster: Arc::from(format!("pool-{index}")),
                credential: None,
                fresh: true,
                kind: CapabilityKind::InferenceModel,
                name: Arc::from("llama"),
                rank: None,
                selection_tier: None,
                site: Arc::clone(&site),
                stable_id: site,
                upstream: None,
            }
        })
        .collect();
    let mut snapshot = RouteSnapshot::from_static(candidates, Arc::from("site-1"));
    snapshot.scores = snapshot
        .candidates
        .iter()
        .map(|candidate| candidate.rho.unwrap_or(0.0))
        .collect();
    snapshot
}

/// The registered `grid_site_route` filter over `snapshot`, affinity off via its filter block.
fn filter(snapshot: RouteSnapshot) -> Result<Box<dyn HttpFilter>, FilterError> {
    let mut config = load_serving_config("testdata/serving-config.json")?;
    config.peers.clear();
    let runtime = spawn_grid_routing(&config, std::collections::BTreeMap::new())?;
    let mut registry = FilterRegistry::with_builtins();
    let snapshot = Arc::new(ArcSwap::from_pointee(snapshot));
    register_grid_filters(
        &mut registry,
        snapshot,
        runtime.affinity(),
        Arc::new(ClusterHealth::default()),
        runtime.tuning(),
    )?;
    let block: serde_yaml::Value = serde_yaml::from_str("prefix_affinity: {enabled: false}")
        .map_err(|error| -> FilterError { error.to_string().into() })?;
    // Building the filter adopts its affinity settings; the runtime's health tick stops on drop.
    let filter = match registry.create("grid_site_route", &block)? {
        AnyFilter::Http(filter) => filter,
        AnyFilter::Tcp(_) => return Err("grid_site_route is not an HTTP filter".into()),
    };
    drop(runtime);
    Ok(filter)
}

/// A context as the pipeline hands it to the first selecting filter: nothing chosen yet.
#[expect(clippy::too_many_lines, reason = "the context literal names every field")]
fn context(request: &Request) -> HttpFilterContext<'_> {
    HttpFilterContext {
        buffered_request_body: None,
        body_done_indices: Vec::new(),
        branch_iterations: HashMap::new(),
        client_addr: None,
        cluster: None,
        current_filter_id: Some(0),
        downstream_tls: false,
        metrics_route: None,
        peer_identity: None,
        extensions: RequestExtensions::default(),
        executed_branch_filters: Vec::new(),
        executed_filter_indices: Vec::new(),
        extra_request_headers: Vec::new(),
        request_headers_to_remove: Vec::new(),
        request_headers_to_set: Vec::new(),
        filter_metadata: HashMap::new(),
        grpc_completion: None,
        prior_pre_read_mutations: Vec::new(),
        pre_read_mutations: Vec::new(),
        structured_metadata: HashMap::new(),
        filter_results: HashMap::new(),
        filter_state: HashMap::new(),
        health_registry: None,
        id_generator: &IDS,
        kv_stores: None,
        session_stores: None,
        subrequest_client: None,
        subrequest_response_mode: SubRequestResponseMode::Buffered,
        request,
        request_body_bytes: 0,
        request_body_mode: BodyMode::Stream,
        request_start: Instant::now(),
        response_body_bytes: 0,
        response_body_mode: BodyMode::Stream,
        response_header: None,
        response_headers_modified: false,
        upstream_reached: false,
        selected_endpoint_index: None,
        attempted_endpoints: Vec::new(),
        retry_policy: None,
        route_retry_policy: None,
        cluster_retry_state: None,
        cluster_retry_state_released: false,
        endpoint_reselector: None,
        pinned_endpoint_address: None,
        time_source: &SystemTimeSource,
        rewritten_path: None,
        upstream: None,
    }
}

/// One `on_request`, polled to completion, and the cluster it selected.
fn route(filter: &dyn HttpFilter, ctx: &mut HttpFilterContext<'_>) -> Option<Arc<str>> {
    // The call has no await point on this path, so one poll completes it.
    let routed = {
        let mut call = pin!(filter.on_request(ctx));
        let polled = call.as_mut().poll(&mut Context::from_waker(Waker::noop()));
        matches!(polled, Poll::Ready(Ok(FilterAction::Continue)))
    };
    routed.then(|| ctx.cluster.take()).flatten()
}

/// One request through the filter over `sites` measured sites.
#[divan::bench(args = [2, 4, 16, 64])]
#[expect(clippy::expect_used, reason = "bench setup")]
fn choose(bencher: Bencher<'_, '_>, sites: u32) {
    let filter = filter(snapshot(sites)).expect("the filter builds");
    let request = Request {
        headers: [(HeaderName::from_static("x-model"), HeaderValue::from_static("llama"))]
            .into_iter()
            .collect(),
        method: Method::POST,
        uri: Uri::from_static("/v1/chat/completions"),
    };
    let mut ctx = context(&request);
    assert!(route(filter.as_ref(), &mut ctx).is_some(), "the first request routes");
    bencher.bench_local(|| route(filter.as_ref(), &mut ctx));
}

fn main() {
    divan::main();
}
