//! Sovereignty zone tokens, normalized the same way by the operator and the gateway.

/// Longest zone accepted, a DNS label.
pub const MAX_ZONE_LEN: usize = 63;

/// `raw` as a zone: trimmed and lowercased, `None` unless a bounded run of ASCII
/// alphanumerics, `-`, `_` or `.`.
#[must_use]
pub fn sovereignty_zone(raw: &str) -> Option<String> {
    let zone = raw.trim();
    let valid = (1..=MAX_ZONE_LEN).contains(&zone.len())
        && zone
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'));
    valid.then(|| zone.to_ascii_lowercase())
}

#[cfg(test)]
mod tests {
    use super::sovereignty_zone;

    #[test]
    fn a_zone_is_a_trimmed_lowercased_bounded_token() {
        assert_eq!(sovereignty_zone(" EU-West.1_a ").as_deref(), Some("eu-west.1_a"));
        assert_eq!(sovereignty_zone(""), None);
        assert_eq!(sovereignty_zone("eu west"), None);
        assert_eq!(sovereignty_zone("eu,us"), None);
        assert_eq!(sovereignty_zone(&"a".repeat(64)), None);
    }
}
