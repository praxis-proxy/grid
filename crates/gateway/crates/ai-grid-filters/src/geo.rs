//! Geo fencing: a request names the sovereignty zones it may be served in, one zone
//! or a comma-separated list, and `grid_site_route` routes only to sites in one.
//!
//! A trusted upstream sets the header and must replace, never append to, a caller's
//! copy. Once configured the fence is mandatory: a request naming no zone is refused.
//! In `audit` mode it refuses nothing, and logs and counts what it would refuse.

use serde::Deserialize;

use crate::{decisions::FenceCause, descriptor::RouteCandidate};

/// `grid_site_route`'s `sovereignty` block.
#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SovereigntyConfig {
    /// Request header naming the zones.
    pub(crate) header: String,
    /// Whether the fence refuses or only logs and counts.
    #[serde(default)]
    pub(crate) mode: FenceMode,
}

/// What the fence does with a request it does not admit.
#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum FenceMode {
    /// Refuse it.
    #[default]
    Enforce,
    /// Route it as if unfenced, and log and count what would be refused.
    Audit,
}

/// A configured fence: the header it reads, and whether it refuses or only logs.
#[derive(Debug)]
pub(crate) struct FenceConfig {
    /// The header the request names its zones in.
    pub(crate) header: http::header::HeaderName,
    /// Whether the fence refuses.
    pub(crate) enforce: bool,
}

impl FenceConfig {
    /// Count, and log when only logging, a request the fence refuses for `cause`; whether it refuses.
    pub(crate) fn refuses(&self, cause: FenceCause, site: Option<&str>) -> bool {
        cause.record(self.enforce);
        if !self.enforce {
            tracing::info!(
                cause = cause.as_str(),
                site,
                "grid_site_route: the sovereignty fence would refuse this request"
            );
        }
        self.enforce
    }
}

/// One request's fence, and whether it refuses.
#[derive(Debug)]
pub(crate) struct Fencing<'cfg> {
    /// The zones the request named.
    fence: Fence,
    /// The fence's settings.
    config: &'cfg FenceConfig,
}

impl<'cfg> Fencing<'cfg> {
    /// `fence` under `config`.
    pub(crate) fn new(fence: Fence, config: &'cfg FenceConfig) -> Self {
        Self { fence, config }
    }

    /// The fence when it refuses, `None` when it only logs.
    pub(crate) fn enforced(&self) -> Option<&Fence> {
        self.config.enforce.then_some(&self.fence)
    }

    /// Whether `candidate` may be chosen: always when the fence only logs.
    pub(crate) fn allows(&self, candidate: &RouteCandidate) -> bool {
        !self.config.enforce || self.fence.admits(candidate)
    }

    /// Whether the request may go to `candidate`, counting and logging a refusal.
    pub(crate) fn passes(&self, candidate: &RouteCandidate) -> bool {
        self.fence.admits(candidate) || !self.config.refuses(FenceCause::OutsideZones, Some(&candidate.site))
    }

    /// The fence's settings.
    pub(crate) fn config(&self) -> &FenceConfig {
        self.config
    }

    /// Count and log a pick outside the zones a fence that only logs let through.
    pub(crate) fn audit(&self, candidate: &RouteCandidate) {
        if !self.config.enforce && !self.fence.admits(candidate) {
            self.config.refuses(FenceCause::OutsideZones, Some(&candidate.site));
        }
    }
}

/// Most zones one request may name.
const MAX_ZONES: usize = 16;

/// What a request's zone header says.
#[derive(Debug)]
pub(crate) enum Zones {
    /// The request sent no zone header.
    Absent,
    /// More than one header line, an invalid zone, or past the cap.
    Invalid,
    /// The zones it may be served in.
    Named(Fence),
}

/// The zones a request may be served in, held as the canonical header value.
#[derive(Debug)]
pub(crate) struct Fence(http::HeaderValue);

impl Fence {
    /// Read `header` from `headers`. More than one line is invalid: a union would widen the fence.
    pub(crate) fn from_headers(headers: &http::HeaderMap, header: &http::header::HeaderName) -> Zones {
        let mut lines = headers.get_all(header).iter();
        let (Some(line), None) = (lines.next(), lines.next()) else {
            return if headers.contains_key(header) {
                Zones::Invalid
            } else {
                Zones::Absent
            };
        };
        line.to_str()
            .ok()
            .and_then(parse_zones)
            .and_then(|zones| http::HeaderValue::try_from(zones.join(",")).ok())
            .map_or(Zones::Invalid, |value| Zones::Named(Self(value)))
    }

    /// The canonical value: lowercased, sorted, deduplicated, comma-separated.
    pub(crate) fn value(&self) -> &http::HeaderValue {
        &self.0
    }

    /// Whether `candidate`'s site lies in an allowed zone. A site with no zone never does.
    pub(crate) fn admits(&self, candidate: &RouteCandidate) -> bool {
        candidate.sovereignty_zone.as_deref().is_some_and(|zone| {
            self.0
                .as_bytes()
                .split(|byte| *byte == b',')
                .any(|allowed| allowed == zone.as_bytes())
        })
    }
}

/// The normalized, sorted, deduplicated zones in `value`; `None` when any is invalid or none is named.
fn parse_zones(value: &str) -> Option<Vec<String>> {
    let mut zones = Vec::new();
    for raw in value.split(',').filter(|raw| !raw.trim().is_empty()) {
        zones.push(grid_signals::zone::sovereignty_zone(raw)?);
        if zones.len() > MAX_ZONES * 4 {
            return None;
        }
    }
    zones.sort_unstable();
    zones.dedup();
    (!zones.is_empty() && zones.len() <= MAX_ZONES).then_some(zones)
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::unwrap_used, clippy::panic, reason = "tests")]
pub(crate) mod tests {
    use super::{Fence, Zones};

    const HEADER: http::header::HeaderName = http::header::HeaderName::from_static("x-zone");

    /// What `lines` of the zone header read as.
    pub(crate) fn zones(lines: &[&str]) -> Zones {
        let mut headers = http::HeaderMap::new();
        for line in lines {
            headers.append(HEADER, http::HeaderValue::from_str(line).unwrap());
        }
        Fence::from_headers(&headers, &HEADER)
    }

    /// The fence `line` names.
    pub(crate) fn fence(line: &str) -> Fence {
        match zones(&[line]) {
            Zones::Named(fence) => fence,
            other @ (Zones::Absent | Zones::Invalid) => panic!("{line:?} names no fence: {other:?}"),
        }
    }

    #[test]
    fn one_zone_or_a_list_is_canonical() {
        assert_eq!(fence("eu").value(), "eu");
        assert_eq!(fence(" CH , eu,EU ").value(), "ch,eu");
        let many: String = std::iter::repeat_n("eu,", 40).collect();
        assert_eq!(fence(&many).value(), "eu", "repeats collapse below the cap");
    }

    #[test]
    fn a_request_naming_no_valid_zone_is_refused() {
        assert!(matches!(zones(&[]), Zones::Absent));
        for line in ["", " , ,", "eu,us east"] {
            assert!(matches!(zones(&[line]), Zones::Invalid), "{line:?}");
        }
        let past: String = (0..=16).map(|i| format!("z{i},")).collect();
        assert!(matches!(zones(&[past.as_str()]), Zones::Invalid), "past the cap");
    }

    #[test]
    fn a_second_header_line_is_refused_rather_than_widening_the_fence() {
        assert!(matches!(zones(&["eu", "us"]), Zones::Invalid));
    }
}
