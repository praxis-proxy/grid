//! `grid-gateway`: the grid data-plane operand.
//!
//! A Praxis gateway assembled in the grid repo. It runs the grid-only live
//! signals filter (deliberately not shipped in the upstream `praxis-ai-filters`
//! crate) on top of the generic Praxis routing filters, and is deployed and
//! configured by the grid operator. Operator is the control plane; this binary
//! is the operand it manages.
//!
//! This is a stub: the artifact, its image, and its build wiring are in place,
//! but the Praxis assembly is not yet linked. Wiring it needs three
//! dependencies the grid workspace does not carry today: the `praxis-filter`
//! `FilterRegistry` from praxis-core, the generic routing filters from
//! `praxis-ai-filters`, and the grid live-signals filter registered on top.
//! See `deploy/gateway/Containerfile` and the `gateway-image` make target.

fn main() {
    // TODO(grid-gateway): build a `FilterRegistry::with_builtins()`, register the
    // generic routing filters via `register_ai_filters`, register the grid
    // live-signals filter, then run the Praxis server bound to the
    // operator-generated config. Tracked with the gateway-operand design.
}
