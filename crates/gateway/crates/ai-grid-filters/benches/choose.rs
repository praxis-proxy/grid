//! Per-request selection time and allocations, with prefix affinity off.

use std::{
    collections::HashMap,
    pin::pin,
    sync::{Arc, LazyLock},
    task::{Context, Poll, Waker},
    time::Instant,
};

use ai_grid_filters::{
    AdmissionState, CandidateCredential, CapabilityKind, ClusterHealth, CredentialRef, RouteCandidate, RouteSnapshot,
    load_serving_config, register_grid_filters, spawn_grid_routing,
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

/// Count allocations only inside the benchmark's measured iterations.
#[global_allocator]
static ALLOC: divan::AllocProfiler = divan::AllocProfiler::system();

/// The normal, queued fallback, and unknown-capacity selection paths.
#[derive(Clone, Copy)]
enum Shape {
    /// Every site has measured room.
    Measured,
    /// Every site has a backlog despite low measured saturation.
    Backlogged,
    /// Only the first site has measured capacity and saturation.
    Mixed,
    /// Every site has fresh pressure but no known capacity.
    PressureOnly,
}

/// `sites` candidates serving `llama` with the selected pressure pattern.
#[expect(clippy::too_many_lines, reason = "one candidate literal spelled field by field")]
fn snapshot(sites: u32, shape: Shape) -> RouteSnapshot {
    let candidates = (1..=sites)
        .map(|index| {
            let site: Arc<str> = Arc::from(format!("site-{index}"));
            RouteCandidate {
                admission_state: AdmissionState::NewAndExisting,
                capacity: (!matches!(shape, Shape::PressureOnly) && (!matches!(shape, Shape::Mixed) || index == 1))
                    .then_some(8.0),
                rho: (!matches!(shape, Shape::Mixed) || index == 1)
                    .then_some(f64::from(index) / f64::from(sites.saturating_add(1))),
                backlog: Some(matches!(shape, Shape::Backlogged)),
                full: None,
                relieved: None,
                silent: false,
                cluster: Arc::from(format!("pool-{index}")),
                credential: None,
                fresh: true,
                kind: CapabilityKind::InferenceModel,
                name: Arc::from("llama"),
                publishes_load: true,
                rank: None,
                selection_tier: None,
                site: Arc::clone(&site),
                stable_id: site,
                upstream: None,
            }
        })
        .collect();
    let mut snapshot = RouteSnapshot::from_static(candidates, Arc::from("site-1"));
    snapshot
        .scores
        .fill(if matches!(shape, Shape::Backlogged) { 2.0 } else { 0.0 });
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
        hedge_policy: None,
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

/// One `on_request`, polled to completion, with request-local mutation buffers reset.
fn route(filter: &dyn HttpFilter, ctx: &mut HttpFilterContext<'_>) -> bool {
    // The call has no await point on this path, so one poll completes it.
    let routed = {
        let mut call = pin!(filter.on_request(ctx));
        let polled = call.as_mut().poll(&mut Context::from_waker(Waker::noop()));
        matches!(polled, Poll::Ready(Ok(FilterAction::Continue)))
    };
    let chosen = ctx.cluster.take().is_some();
    ctx.request_headers_to_remove.clear();
    ctx.request_headers_to_set.clear();
    routed && chosen
}

/// One request through the filter over `sites` measured sites.
#[divan::bench(args = [2, 4, 16, 64])]
fn choose(bencher: Bencher<'_, '_>, sites: u32) {
    measure(bencher, sites, Shape::Measured);
}

/// The queue fallback when no site is queue-free.
#[divan::bench(args = [2, 4, 16, 64])]
fn backlogged(bencher: Bencher<'_, '_>, sites: u32) {
    measure(bencher, sites, Shape::Backlogged);
}

/// Unknown-capacity participation beside one measured site.
#[divan::bench(args = [2, 4, 16, 64])]
fn mixed(bencher: Bencher<'_, '_>, sites: u32) {
    measure(bencher, sites, Shape::Mixed);
}

/// Fresh pressure guides the draw before any site has learned capacity.
#[divan::bench(args = [2, 4, 16, 64])]
fn pressure_only(bencher: Bencher<'_, '_>, sites: u32) {
    measure(bencher, sites, Shape::PressureOnly);
}

/// Warm up the filter before measuring requests and allocator calls.
#[expect(clippy::expect_used, reason = "bench setup")]
fn measure(bencher: Bencher<'_, '_>, sites: u32, shape: Shape) {
    let filter = filter(snapshot(sites, shape)).expect("the filter builds");
    let request = Request {
        headers: [(HeaderName::from_static("x-model"), HeaderValue::from_static("llama"))]
            .into_iter()
            .collect(),
        method: Method::POST,
        uri: Uri::from_static("/v1/chat/completions"),
    };
    let mut ctx = context(&request);
    assert!(route(filter.as_ref(), &mut ctx), "the first request routes");
    bencher.bench_local(|| route(filter.as_ref(), &mut ctx));
}

/// A local pick whose candidate carries a credential the router selects for `credential_inject`.
#[divan::bench(args = [4])]
fn local_credentialed(bencher: Bencher<'_, '_>, sites: u32) {
    let mut snapshot = snapshot(sites, Shape::Measured);
    for candidate in &mut snapshot.candidates {
        candidate.site = Arc::clone(&snapshot.local_site);
        candidate.credential = Some(CandidateCredential {
            strategy: "bearer_token".to_owned(),
            secret_ref: CredentialRef {
                key: "token".to_owned(),
                name: "grid-model-client".to_owned(),
                namespace: "models".to_owned(),
            },
        });
    }
    measure_fresh(bencher, snapshot);
}

/// A pick of another site's gateway: hop context, and the caller's credentials removed.
#[divan::bench(args = [4])]
fn provider_hop(bencher: Bencher<'_, '_>, sites: u32) {
    let mut snapshot = snapshot(sites, Shape::Measured);
    snapshot.local_site = Arc::from("local");
    snapshot.provider_hop_clusters = Arc::new(
        snapshot
            .candidates
            .iter()
            .map(|candidate| candidate.cluster.to_string())
            .collect(),
    );
    measure_fresh(bencher, snapshot);
}

/// One request per iteration on a fresh context, so growth in its buffers is counted.
#[expect(clippy::expect_used, reason = "bench setup")]
fn measure_fresh(bencher: Bencher<'_, '_>, snapshot: RouteSnapshot) {
    let filter = filter(snapshot).expect("the filter builds");
    let request = Request {
        headers: [(HeaderName::from_static("x-model"), HeaderValue::from_static("llama"))]
            .into_iter()
            .collect(),
        method: Method::POST,
        uri: Uri::from_static("/v1/chat/completions"),
    };
    assert!(
        route(filter.as_ref(), &mut context(&request)),
        "the first request routes"
    );
    bencher
        .with_inputs(|| context(&request))
        .bench_local_refs(|ctx| route(filter.as_ref(), ctx));
}

fn main() {
    divan::main();
}
