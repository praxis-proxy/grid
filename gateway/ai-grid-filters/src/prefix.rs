//! Prefix affinity: chained block keys over a request's prompt, and the
//! per-replica index of which candidate each key was sent to.
//!
//! The keys are cache-locality state only. They never read or count load.

mod affinity;
mod canon;
mod index;

use std::{io, sync::Arc};

pub use affinity::AffinitySettings;
pub(crate) use affinity::{Affinity, QueueGate, Site};
use arc_swap::{ArcSwap, ArcSwapOption};
pub(crate) use canon::Responses;
pub(crate) use index::PrefixIndex;
use xxhash_rust::xxh64::{Xxh64, xxh64};

use crate::pin::TagKey;

/// Bytes of canonical prompt per block, the EPP estimate backend's 64 pseudo-tokens.
pub(crate) const BLOCK: usize = 256;

/// Leading blocks that each get a key.
const DENSE_KEYS: usize = 448;

/// Keys past the dense head; their stride doubles every [`SPARSE_RUN`] keys.
const SPARSE_KEYS: usize = 64;

/// Sparse keys taken at one stride before it doubles.
const SPARSE_RUN: usize = 8;

/// Most keys one request yields: about 1.1 MiB of canonical prompt.
pub(crate) const MAX_KEYS: usize = DENSE_KEYS + SPARSE_KEYS;

/// This replica's prefix index and the live affinity settings, shared by the
/// control step, which applies each serving config, and every route filter.
#[derive(Debug)]
pub struct PrefixAffinity {
    /// Which candidate each prefix key went to.
    pub(crate) index: PrefixIndex,
    /// The settings from the current serving config.
    pub(crate) settings: ArcSwap<AffinitySettings>,
    /// The key that authenticates stored-state tags, when the config names one.
    pub(crate) tag_key: ArcSwapOption<TagKey>,
}

impl Default for PrefixAffinity {
    fn default() -> Self {
        Self {
            index: PrefixIndex::default(),
            settings: ArcSwap::from_pointee(AffinitySettings::default()),
            tag_key: ArcSwapOption::empty(),
        }
    }
}

impl PrefixAffinity {
    /// Take a serving config's settings and tag key, and forget clusters it no longer routes to.
    pub(crate) fn apply(&self, settings: AffinitySettings, tag_key: Option<TagKey>, clusters: &[Arc<str>]) {
        self.settings.store(Arc::new(settings));
        self.tag_key.store(tag_key.map(Arc::new));
        self.index.retain(clusters);
    }
}

/// The request body shapes that carry a prompt.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Api {
    /// `OpenAI` Chat Completions.
    ChatCompletions,
    /// `OpenAI` Completions.
    Completions,
    /// Anthropic Messages.
    Messages,
    /// `OpenAI` Responses.
    Responses,
}

impl Api {
    /// The API a request path serves, or `None` for a path with no prompt.
    pub(crate) fn from_path(path: &str) -> Option<Self> {
        match path.trim_end_matches('/') {
            "/v1/chat/completions" => Some(Self::ChatCompletions),
            "/v1/completions" => Some(Self::Completions),
            "/v1/messages" => Some(Self::Messages),
            "/v1/responses" => Some(Self::Responses),
            _ => None,
        }
    }
}

/// A request's prompt as chained block keys, head first.
///
/// Key `i` covers every byte up to its block, so two requests share key `i`
/// only when their prompts agree that far. Never logged or exported.
#[derive(Clone, PartialEq, Eq)]
pub(crate) struct PrefixKeys(Vec<u64>);

/// Keys are prompt content, so only their count prints.
impl std::fmt::Debug for PrefixKeys {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "PrefixKeys({} keys)", self.0.len())
    }
}

impl PrefixKeys {
    /// The keys in `model`'s key space, so two models never share a key.
    ///
    /// The body is hashed before the model header is known, so the model is
    /// folded in after: every key is offset by the same model hash.
    pub(crate) fn for_model(mut self, model: &str) -> Self {
        let offset = xxh64(model.as_bytes(), MODEL);
        self.0.iter_mut().for_each(|key| *key ^= offset);
        self
    }

    /// Keys as given, for tests outside this module.
    #[cfg(test)]
    pub(crate) fn from_keys(keys: Vec<u64>) -> Self {
        Self(keys)
    }

    /// The keys, head first.
    pub(crate) fn as_slice(&self) -> &[u64] {
        &self.0
    }
}

/// A Responses create body, read once for both its pinning and its prompt.
pub(crate) fn parse_responses(body: &[u8]) -> Option<Responses<'_>> {
    serde_json::from_slice(body).ok()
}

/// The prefix keys of a parsed Responses request, as [`prefix_keys`] gives for its body.
pub(crate) fn responses_keys(request: &Responses<'_>) -> Option<PrefixKeys> {
    canon::responses(request, Chain::new)?.finish()
}

/// The prefix keys of `body` for `api`, before [`PrefixKeys::for_model`].
///
/// `None` when the body has no prompt the gateway can read: invalid JSON, token
/// arrays, an unknown shape, or a prompt shorter than one block. Such a request
/// routes on load alone.
pub(crate) fn prefix_keys(api: Api, body: &[u8]) -> Option<PrefixKeys> {
    canon::canonical(api, body, Chain::new)?.finish()
}

/// Hashes a canonical prompt stream into chained block keys without holding the stream.
struct Chain {
    /// The block being filled, hashed as it arrives and seeded by the previous block's hash.
    hasher: Xxh64,
    /// Bytes in the block being filled.
    filled: usize,
    /// Full blocks hashed so far.
    blocks: usize,
    /// The block count that takes the next key.
    next_key: usize,
    /// Keys taken, head first.
    keys: Vec<u64>,
}

impl Chain {
    /// A chain seeded by the request's `cache_salt`, so tenants never share keys.
    fn new(salt: Option<&str>) -> Self {
        let seed = salt.map_or(0, |salt| xxh64(salt.as_bytes(), SALTED));
        Self {
            hasher: Xxh64::new(seed),
            filled: 0,
            blocks: 0,
            next_key: 1,
            keys: Vec::new(),
        }
    }

    /// Whether every key is taken, so further bytes change nothing.
    fn full(&self) -> bool {
        self.keys.len() >= MAX_KEYS
    }

    /// Hash the filled block into the chain and take a key if it falls on the schedule.
    fn close_block(&mut self) {
        let last = self.hasher.digest();
        self.hasher.reset(last);
        self.filled = 0;
        self.blocks = self.blocks.saturating_add(1);
        if self.blocks == self.next_key {
            self.keys.push(last);
            self.next_key = self.blocks.saturating_add(stride(self.keys.len()));
        }
    }

    /// The keys, or `None` when the prompt filled no block. A partial tail block
    /// cannot match the next turn, so it takes no key.
    fn finish(self) -> Option<PrefixKeys> {
        (!self.keys.is_empty()).then_some(PrefixKeys(self.keys))
    }
}

/// Seeds the hash of a `cache_salt`.
const SALTED: u64 = 0x5A17_5A17_5A17_5A17;

/// Seeds the hash of a model name.
const MODEL: u64 = 0x6D6F_6465_6C5F_7631;

/// Blocks between the key after `taken` keys and the one before it.
fn stride(taken: usize) -> usize {
    match taken.checked_sub(DENSE_KEYS) {
        None => 1,
        Some(sparse) => 2_usize << (sparse / SPARSE_RUN).min(16),
    }
}

impl io::Write for Chain {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let mut rest = buf;
        while !rest.is_empty() && !self.full() {
            let room = BLOCK.saturating_sub(self.filled);
            let (now, later) = rest.split_at(room.min(rest.len()));
            self.hasher.update(now);
            self.filled = self.filled.saturating_add(now.len());
            if self.filled == BLOCK {
                self.close_block();
            }
            rest = later;
        }
        // Past the cap the bytes are consumed unhashed, so the walk can stop early.
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::panic,
    clippy::min_ident_chars,
    reason = "tests"
)]
mod tests {
    use std::io::Write as _;

    use super::*;

    fn keys(model: &str, salt: Option<&str>, stream: &[u8]) -> Vec<u64> {
        let mut chain = Chain::new(salt);
        chain.write_all(stream).unwrap();
        chain.finish().map(|keys| keys.for_model(model).0).unwrap_or_default()
    }

    #[test]
    fn streamed_keys_match_a_one_shot_chain_whatever_the_writes() {
        let stream: Vec<u8> = (0..(BLOCK * 5 + 17)).map(|i| u8::try_from(i % 251).unwrap()).collect();
        let mut last = 0;
        let want: Vec<u64> = stream
            .chunks_exact(BLOCK)
            .map(|block| {
                last = xxh64(block, last);
                last
            })
            .collect();
        for write in [1, 7, BLOCK, BLOCK + 3, stream.len()] {
            let mut chain = Chain::new(None);
            for piece in stream.chunks(write) {
                chain.write_all(piece).unwrap();
            }
            assert_eq!(chain.finish().unwrap().as_slice(), want, "writes of {write}");
        }
    }

    #[test]
    fn a_shorter_stream_is_a_key_prefix_of_a_longer_one() {
        let long = vec![b'a'; BLOCK * 10];
        let short = &long[..BLOCK * 4 + 17];
        let (long, short) = (keys("m", None, &long), keys("m", None, short));
        assert_eq!(short.len(), 4, "a partial tail block takes no key");
        assert_eq!(long[..4], short[..], "a shared head shares its keys");
    }

    #[test]
    fn chunking_does_not_change_the_keys() {
        let stream: Vec<u8> = (0..BLOCK * 7).map(|i| u8::try_from(i % 251).unwrap()).collect();
        let mut chain = Chain::new(None);
        for piece in stream.chunks(37) {
            chain.write_all(piece).unwrap();
        }
        assert_eq!(chain.finish().unwrap().for_model("m").0, keys("m", None, &stream));
    }

    #[test]
    fn model_and_salt_give_disjoint_keys() {
        let stream = vec![b'x'; BLOCK * 3];
        let base = keys("m", None, &stream);
        for other in [keys("n", None, &stream), keys("m", Some("tenant"), &stream)] {
            assert!(base.iter().all(|key| !other.contains(key)), "no key is shared");
        }
    }

    #[test]
    fn the_key_count_is_capped_and_reaches_past_the_dense_head() {
        let stream = vec![b'y'; BLOCK * 6_000];
        let mut chain = Chain::new(None);
        chain.write_all(&stream).unwrap();
        let reached = chain.blocks;
        assert_eq!(chain.finish().unwrap().0.len(), MAX_KEYS);
        assert!(reached > DENSE_KEYS * 4, "sparse keys reach {reached} blocks");
    }

    #[test]
    fn a_change_past_the_dense_head_changes_a_sparse_key() {
        let mut stream = vec![b'z'; BLOCK * 3_000];
        let before = keys("m", None, &stream);
        stream[BLOCK * 2_000] = b'!';
        let after = keys("m", None, &stream);
        assert_eq!(before[..DENSE_KEYS], after[..DENSE_KEYS], "the head is unchanged");
        assert_ne!(before, after, "the tail edit is seen beyond the dense head");
    }

    #[test]
    fn paths_map_to_apis() {
        assert_eq!(Api::from_path("/v1/chat/completions"), Some(Api::ChatCompletions));
        assert_eq!(Api::from_path("/v1/messages/"), Some(Api::Messages));
        assert_eq!(Api::from_path("/v1/responses"), Some(Api::Responses));
        assert_eq!(Api::from_path("/v1/embeddings"), None);
    }
}
