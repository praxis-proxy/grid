//! Bounds for a request relayed to another site's gateway. The gateway's proxy enforces them,
//! so this states the policy and the operator renders it.

use std::time::Duration;

/// How a hop to another site's gateway waits and retries.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct HopPolicy {
    /// Longest wait for the peer to accept a connection.
    pub connect: Duration,
    /// Longest wait for a connection including the TLS handshake.
    pub total_connect: Duration,
    /// Longest wait for one read. Applied per read, so a stream survives while chunks keep
    /// coming; the first token is the longest gap.
    pub read: Duration,
    /// Longest wait for one write: a prompt body a stalled peer never reads.
    pub write: Duration,
    /// Retries after a connect failure, to the same address: a peer cluster has one endpoint.
    /// Safe for a non-idempotent request because a failed connect sent nothing, which RFC 9110
    /// section 9.2.2 permits; no other failure retries.
    pub connect_retries: u32,
}

impl HopPolicy {
    /// The bounds for every hop to another site's gateway.
    pub const PEER: Self = Self {
        connect: Duration::from_secs(2),
        total_connect: Duration::from_secs(4),
        read: Duration::from_secs(120),
        write: Duration::from_secs(30),
        connect_retries: 1,
    };
}

/// The host of a `host:port` address, brackets stripped from an IPv6 literal.
///
/// ```
/// assert_eq!(grid_client::address_host("[fd00::1]:8443"), "fd00::1");
/// assert_eq!(
///     grid_client::address_host("peer.example.com:8443"),
///     "peer.example.com"
/// );
/// assert_eq!(grid_client::address_host("fd00::1"), "fd00::1");
/// ```
#[must_use]
pub fn address_host(address: &str) -> &str {
    if let Some(rest) = address.strip_prefix('[') {
        return rest.split_once(']').map_or(rest, |(host, _)| host);
    }
    match address.split_once(':') {
        // A second colon is an unbracketed IPv6 literal with no port.
        Some((host, port)) if !port.contains(':') => host,
        _ => address,
    }
}

#[cfg(test)]
mod tests {
    use super::{HopPolicy, address_host};

    #[test]
    fn the_host_is_split_from_its_port_bracket_aware() {
        for (address, host) in [
            ("peer.example.com:8443", "peer.example.com"),
            ("10.0.0.1:80", "10.0.0.1"),
            ("[fd00::1]:8443", "fd00::1"),
            ("[fd00::1]", "fd00::1"),
            ("fd00::1", "fd00::1"),
            ("peer", "peer"),
        ] {
            assert_eq!(address_host(address), host, "{address}");
        }
    }

    #[test]
    fn peer_connect_fits_inside_its_total_and_reads_outlast_connects() {
        let peer = HopPolicy::PEER;
        assert!(
            peer.connect < peer.total_connect,
            "the TLS handshake needs time after the connect"
        );
        assert!(peer.total_connect < peer.write, "a stalled write outlasts a connect");
        assert!(peer.write < peer.read, "a slow first token outlasts a write");
        assert_eq!(
            peer.connect_retries, 1,
            "a partitioned address costs one more connect, not three"
        );
    }
}
