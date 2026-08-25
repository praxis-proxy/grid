#![expect(
    clippy::print_stdout,
    clippy::print_stderr,
    reason = "generate-crds is a CLI tool that prints to the terminal"
)]
//! Generate Grid operator CRD manifests as a JSON `List` for `kubectl apply`.
//!
//! Output is written to stdout.  Pipe directly to `kubectl apply -f -` to
//! install or update the CRDs in a cluster:
//!
//! ```text
//! cargo run -p operator --bin generate-crds | kubectl apply -f -
//! ```
//!
//! The output is a single JSON `v1/List` containing all CRDs required by the
//! Grid operator controllers.  `kubectl apply` processes each item in the list
//! independently.

use kube::CustomResourceExt as _;
use operator::crd::{
    agent_tool_provider::AgentToolProvider, grid_network::GridNetwork, grid_site::GridSite,
    inference_provider::InferenceProvider,
};

fn main() {
    let crds = [
        serde_json::to_value(AgentToolProvider::crd()),
        serde_json::to_value(GridNetwork::crd()),
        serde_json::to_value(GridSite::crd()),
        serde_json::to_value(InferenceProvider::crd()),
    ];
    let items: Vec<serde_json::Value> = crds
        .into_iter()
        .map(|result| {
            result.unwrap_or_else(|err| {
                eprintln!("CRD serialization failed: {err}");
                std::process::exit(1);
            })
        })
        .collect();
    let list = serde_json::json!({ "apiVersion": "v1", "kind": "List", "items": items });
    let out = serde_json::to_string_pretty(&list).unwrap_or_else(|err| {
        eprintln!("JSON serialization failed: {err}");
        std::process::exit(1);
    });
    println!("{out}");
}
