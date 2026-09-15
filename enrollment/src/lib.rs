//! Site-token enrollment for the AI grid.
//!
//! A grid-admin mints a single-use site token that pins a name and hands it to a
//! site out of band. The site presents the token and a certificate signing
//! request over HTTP and receives a signed certificate in the same response. The
//! site needs no credentials on the grid's cluster, and the grid never sees the
//! site's private key.
//!
//! Storage is a backend enum, so a MaaS deployment can point this at the Postgres
//! it already runs while a standalone grid brings its own.

pub mod api;
pub mod auth;
pub mod authz;
pub mod generated;
pub mod store;

pub use api::{AppState, router};
pub use auth::GridAdmins;
pub use store::{Issued, NewSiteToken, Pin, Store, StoreError};
