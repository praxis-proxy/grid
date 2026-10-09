//! Stored-state pinning: a stored response or conversation lives on the site that made it.
//!
//! Ids leave the gateway tagged with that site and its cluster,
//! `resp_<site>.<cluster>.<mac>.<id>`, and come back stripped. A request naming
//! one goes only there. With a key, the mac authenticates the site and cluster,
//! so a client cannot aim a request at a site it was never given.

use bytes::{Bytes, BytesMut};
use serde::Deserialize;
use serde_json::value::RawValue;
use xxhash_rust::xxh64::xxh64;
use zeroize::Zeroizing;

/// The id prefixes of stored state.
const PREFIXES: [&str; 2] = ["resp_", "conv_"];

/// The JSON keys whose values are stored-state ids.
const ID_KEYS: [&[u8]; 5] = [
    b"id",
    b"previous_response_id",
    b"response_id",
    b"conversation",
    b"conversation_id",
];

/// The shortest tag key accepted: 256 bits.
pub(crate) const MIN_KEY_BYTES: usize = 32;

/// Hex digits of the cluster mark.
const MARK_LEN: usize = 8;

/// Hex digits of the mac: 64 bits.
const MAC_LEN: usize = 16;

/// Seeds the cluster mark.
const MARK: u64 = 0x636C_7573_7465_7231;

/// The key that authenticates the site and cluster in a tag. Never printed.
pub(crate) struct TagKey(Zeroizing<Vec<u8>>);

impl std::fmt::Debug for TagKey {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TagKey(..)")
    }
}

impl TagKey {
    /// A key from `bytes`, refused when shorter than [`MIN_KEY_BYTES`].
    ///
    /// # Errors
    ///
    /// Returns a message naming the length when the key is too short.
    pub(crate) fn new(bytes: Zeroizing<Vec<u8>>) -> Result<Self, String> {
        if bytes.len() < MIN_KEY_BYTES {
            return Err(format!(
                "the stored-state tag key must be at least {MIN_KEY_BYTES} bytes, got {}",
                bytes.len()
            ));
        }
        Ok(Self(bytes))
    }

    /// The mac over `site` and `mark`, as hex.
    fn mac(&self, site: &str, mark: &str) -> String {
        let mut message = Vec::new();
        message.extend_from_slice(site.as_bytes());
        message.push(0);
        message.extend_from_slice(mark.as_bytes());
        hex(certs::hmac_sha256(&self.0, &message)
            .get(..MAC_LEN / 2)
            .unwrap_or_default())
    }
}

/// `bytes` as lowercase hex.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The short mark naming `cluster` in a tag, since a cluster name may hold any character.
pub(crate) fn cluster_mark(cluster: &str) -> String {
    hex(xxh64(cluster.as_bytes(), MARK)
        .to_be_bytes()
        .get(..MARK_LEN / 2)
        .unwrap_or_default())
}

/// The tag after an id's prefix, `<site>.<mark>.<mac>.`, the mac empty without a key.
pub(crate) fn tag(site: &str, cluster: &str, key: Option<&TagKey>) -> String {
    let mark = cluster_mark(cluster);
    let mac = key.map(|key| key.mac(site, &mark)).unwrap_or_default();
    format!("{site}.{mark}.{mac}.")
}

/// A tagged id, split into where it was stored and the id the site knows.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Pinned<'id> {
    /// The site that stored the state.
    pub(crate) site: &'id str,
    /// The [`cluster_mark`] of the cluster that stored it.
    pub(crate) mark: &'id str,
    /// The id without the tag.
    pub(crate) upstream: String,
}

/// Split a tagged id. `None` for an id the gateway did not tag, or, with `key`,
/// one whose mac does not match.
pub(crate) fn untag<'id>(id: &'id str, key: Option<&TagKey>) -> Option<Pinned<'id>> {
    PREFIXES.iter().find_map(|prefix| {
        // Site names are DNS labels and marks and macs are hex, so the first three periods split it.
        let (site, rest) = id.strip_prefix(prefix)?.split_once('.')?;
        let (mark, rest) = rest.split_once('.')?;
        let (mac, rest) = rest.split_once('.')?;
        let hex_of = |value: &str, len: usize| value.len() == len && value.bytes().all(|byte| byte.is_ascii_hexdigit());
        let shaped = !site.is_empty() && !rest.is_empty() && hex_of(mark, MARK_LEN);
        let authentic = match key {
            Some(key) => constant_time_eq(mac.as_bytes(), key.mac(site, mark).as_bytes()),
            None => mac.is_empty() || hex_of(mac, MAC_LEN),
        };
        (shaped && authentic).then(|| Pinned {
            site,
            mark,
            upstream: format!("{prefix}{rest}"),
        })
    })
}

/// Whether `left` and `right` match, in time that does not depend on where they differ.
fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .fold(0_u8, |differ, (left, right)| differ | (left ^ right))
            == 0
}

/// Which stored-state API a request path names.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Collection {
    /// `/v1/responses`.
    Responses,
    /// `/v1/conversations`.
    Conversations,
}

impl Collection {
    /// The collection's path.
    fn path(self) -> &'static str {
        match self {
            Self::Responses => "/v1/responses",
            Self::Conversations => "/v1/conversations",
        }
    }
}

/// What a stored-state request path names.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum StatePath<'path> {
    /// The collection itself: create a response or a conversation.
    Create(Collection),
    /// One stored item or its sub-resources, by id.
    Stored {
        /// Its collection.
        collection: Collection,
        /// The id as the client sent it.
        id: &'path str,
        /// What follows the id, such as `/cancel`, or empty.
        rest: &'path str,
    },
}

/// The stored-state resource a path names, or `None` for another API.
pub(crate) fn state_path(path: &str) -> Option<StatePath<'_>> {
    [Collection::Responses, Collection::Conversations]
        .into_iter()
        .find_map(|collection| {
            let tail = path.strip_prefix(collection.path())?;
            if tail.is_empty() || tail == "/" {
                return Some(StatePath::Create(collection));
            }
            let tail = tail.strip_prefix('/')?;
            let (id, rest) = tail.find('/').map_or((tail, ""), |at| tail.split_at(at));
            Some(StatePath::Stored { collection, id, rest })
        })
}

/// The path with the stored id replaced by `upstream`.
pub(crate) fn upstream_path(collection: Collection, upstream: &str, rest: &str) -> String {
    format!("{}/{upstream}{rest}", collection.path())
}

#[cfg(test)]
/// The fields of a Responses create body that name stored state.
#[derive(Deserialize)]
struct Continues<'body> {
    /// A stored response this one continues.
    #[serde(borrow, default)]
    previous_response_id: Option<&'body RawValue>,
    /// A stored conversation: its id, or an object holding it.
    #[serde(borrow, default)]
    conversation: Option<&'body RawValue>,
}

/// A conversation named by object.
#[derive(Deserialize)]
struct ConversationRef<'body> {
    /// The conversation id.
    #[serde(borrow)]
    id: &'body RawValue,
}

/// The site a Responses create body is pinned to, and the body with every tag stripped.
///
/// Each id field is decoded as JSON, so escapes match, and only that field's
/// token is replaced. `None` when the body continues nothing tagged.
#[cfg(test)]
pub(crate) fn pinned_body(body: &[u8], key: Option<&TagKey>) -> Option<(BodyPin, Vec<u8>)> {
    let fields: Continues<'_> = serde_json::from_slice(body).ok()?;
    pinned_fields(body, fields.previous_response_id, fields.conversation, key)
}

/// [`pinned_body`] over the `previous_response_id` and `conversation` already read
/// from `body`, so a body parsed for its prompt is not parsed again.
pub(crate) fn pinned_fields<'body>(
    body: &'body [u8],
    previous_response_id: Option<&'body RawValue>,
    conversation: Option<&'body RawValue>,
    key: Option<&TagKey>,
) -> Option<(BodyPin, Vec<u8>)> {
    let conversation = conversation.and_then(|value| {
        serde_json::from_str::<ConversationRef<'_>>(value.get())
            .map(|reference| reference.id)
            .ok()
            .or(Some(value))
    });
    let mut site = None;
    let mut edits = Vec::new();
    for token in [previous_response_id, conversation].into_iter().flatten() {
        let Ok(id) = serde_json::from_str::<std::borrow::Cow<'_, str>>(token.get()) else {
            continue;
        };
        let Some(pinned) = untag(&id, key) else {
            continue;
        };
        site.get_or_insert_with(|| (std::sync::Arc::from(pinned.site), pinned.mark.to_owned()));
        edits.push((span(body, token)?, serde_json::to_string(&pinned.upstream).ok()?));
    }
    let site = site?;
    edits.sort_by_key(|(at, _)| at.start);
    let mut out = Vec::with_capacity(body.len());
    let mut from = 0;
    for (at, replacement) in edits {
        out.extend_from_slice(body.get(from..at.start)?);
        out.extend_from_slice(replacement.as_bytes());
        from = at.end;
    }
    out.extend_from_slice(body.get(from..)?);
    Some((site, out))
}

/// The site and cluster mark a create body is pinned to.
pub(crate) type BodyPin = (std::sync::Arc<str>, String);

/// Where `token`, borrowed from `body`, sits in it.
fn span(body: &[u8], token: &RawValue) -> Option<std::ops::Range<usize>> {
    let start = token.get().as_ptr().addr().checked_sub(body.as_ptr().addr())?;
    let end = start.checked_add(token.get().len())?;
    (end <= body.len()).then_some(start..end)
}

/// Tags the stored-state ids of a JSON or SSE response body, chunk by chunk.
///
/// A small JSON scanner follows strings, keys and nesting across chunks. It tags
/// a value only when its key is one of [`ID_KEYS`], it starts with a stored-state
/// prefix, and it sits in the response's own object: the top level, a `response`
/// envelope in it, or a `conversation` object in either. Ids in metadata, tools,
/// input or output, and text a model generates, are left alone.
#[derive(Debug)]
pub(crate) struct Tagger {
    /// `<site>.<mark>.<mac>.`, inserted after the prefix.
    tag: Vec<u8>,
    /// Each of [`PREFIXES`] followed by `tag`, to tell an id already tagged.
    tagged: [Vec<u8>; 2],
    /// The last byte outside a string that was not whitespace.
    last: u8,
    /// Whether the scanner is inside a string.
    in_string: bool,
    /// Whether the previous byte in a string was an unescaped backslash.
    escaped: bool,
    /// The string being read, kept only while it could still be an id key.
    current: Short,
    /// The last string closed, when short enough to be an id key.
    key: Option<Short>,
    /// Bytes held back from the previous chunk.
    held: Vec<u8>,
    /// Open objects and arrays.
    depth: usize,
    /// The open containers that are the response's own, outermost first, as a count.
    own_depth: usize,
}

/// Keys whose object value is still the response's own.
const OWN_KEYS: [&[u8]; 2] = [b"response", b"conversation"];

/// The longest key worth remembering, the longest of [`ID_KEYS`].
const KEY_LIMIT: usize = 20;

/// A string of at most [`KEY_LIMIT`] bytes, kept without allocating.
#[derive(Clone, Copy, Debug, Default)]
struct Short {
    /// The bytes.
    bytes: [u8; KEY_LIMIT],
    /// Bytes seen, past [`KEY_LIMIT`] when the string is too long to be a key.
    len: usize,
}

impl Short {
    /// Add `byte`, counting it once the string is too long to keep.
    fn push(&mut self, byte: u8) {
        if let Some(slot) = self.bytes.get_mut(self.len) {
            *slot = byte;
        }
        self.len = self.len.saturating_add(1);
    }

    /// The string, `None` when it was too long to keep.
    fn get(&self) -> Option<&[u8]> {
        self.bytes.get(..self.len)
    }
}

/// What tagging did to a chunk.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Pushed {
    /// Nothing to tag and nothing held: send the chunk as it is.
    Unchanged,
    /// Send these bytes instead, empty when all of it is held back.
    Replaced(Bytes),
}

/// Where a chunk takes a tag, and where the bytes held back for the next chunk begin.
#[derive(Default)]
struct Plan {
    /// Offsets the tag goes in at.
    inserts: Vec<usize>,
    /// The offset from which the chunk is held back.
    hold: Option<usize>,
}

impl Tagger {
    /// A tagger inserting `tag`, from [`tag`].
    pub(crate) fn new(tag: String) -> Self {
        let tag = tag.into_bytes();
        let tagged = PREFIXES.map(|prefix| [prefix.as_bytes(), &tag].concat());
        Self {
            tag,
            tagged,
            last: 0,
            in_string: false,
            escaped: false,
            current: Short::default(),
            key: None,
            held: Vec::new(),
            depth: 0,
            own_depth: 0,
        }
    }

    /// Tag the ids in `chunk`, holding back a possible partial match unless `end`.
    /// A chunk with nothing to tag and nothing held before it is left as it is.
    pub(crate) fn push(&mut self, chunk: &[u8], end: bool) -> Pushed {
        if self.held.is_empty() {
            let plan = self.scan(chunk, end);
            if plan.inserts.is_empty() && plan.hold.is_none() {
                return Pushed::Unchanged;
            }
            return Pushed::Replaced(self.emit(chunk, &plan));
        }
        let mut input = std::mem::take(&mut self.held);
        input.extend_from_slice(chunk);
        let plan = self.scan(&input, end);
        Pushed::Replaced(self.emit(&input, &plan))
    }

    /// Follow `input`, noting where tags go in and where it must be held back.
    fn scan(&mut self, input: &[u8], end: bool) -> Plan {
        let mut plan = Plan::default();
        let mut at = 0;
        while let Some(&byte) = input.get(at) {
            if !self.in_string && byte == b'"' {
                let after = input.get(at.saturating_add(1)..).unwrap_or_default();
                match self.open(after, end) {
                    // The quote is read again with the next chunk.
                    Open::Wait => {
                        plan.hold = Some(at);
                        return plan;
                    },
                    Open::Tag(prefix) => {
                        at = at.saturating_add(1).saturating_add(prefix.len());
                        plan.inserts.push(at);
                    },
                    Open::Plain => at = at.saturating_add(1),
                }
                self.in_string = true;
                self.current = Short::default();
                continue;
            }
            if self.in_string {
                self.string_byte(byte);
            } else if !byte.is_ascii_whitespace() {
                self.nest(byte);
                self.last = byte;
            }
            at = at.saturating_add(1);
        }
        plan
    }

    /// `input` with the tag at each insert, up to where it is held back, which is kept.
    fn emit(&mut self, input: &[u8], plan: &Plan) -> Bytes {
        let end = plan.hold.unwrap_or(input.len());
        let mut out = BytesMut::with_capacity(end.saturating_add(plan.inserts.len().saturating_mul(self.tag.len())));
        let mut from = 0;
        for &at in &plan.inserts {
            out.extend_from_slice(input.get(from..at).unwrap_or_default());
            out.extend_from_slice(&self.tag);
            from = at;
        }
        out.extend_from_slice(input.get(from..end).unwrap_or_default());
        if let Some(hold) = plan.hold {
            self.held = input.get(hold..).unwrap_or_default().to_vec();
        }
        out.freeze()
    }

    /// Follow an object or array opening or closing outside a string.
    fn nest(&mut self, byte: u8) {
        match byte {
            b'{' | b'[' => {
                let named = self.last == b':'
                    && self
                        .key
                        .as_ref()
                        .and_then(Short::get)
                        .is_some_and(|key| OWN_KEYS.contains(&key));
                let own = self.depth == 0 || (self.own_depth == self.depth && named);
                self.depth = self.depth.saturating_add(1);
                if own {
                    self.own_depth = self.depth;
                }
            },
            b'}' | b']' => {
                if self.own_depth == self.depth {
                    self.own_depth = self.own_depth.saturating_sub(1);
                }
                self.depth = self.depth.saturating_sub(1);
            },
            _ => {},
        }
    }

    /// Follow one byte inside a string.
    fn string_byte(&mut self, byte: u8) {
        if self.escaped {
            self.escaped = false;
        } else if byte == b'\\' {
            self.escaped = true;
        } else if byte == b'"' {
            self.in_string = false;
            self.last = b'"';
            self.key = Some(self.current);
            return;
        }
        self.current.push(byte);
    }

    /// What to do with a string that opens before `after`.
    fn open(&self, after: &[u8], end: bool) -> Open {
        let own = self.depth > 0 && self.own_depth == self.depth;
        let is_id_value = own
            && self.last == b':'
            && self
                .key
                .as_ref()
                .and_then(Short::get)
                .is_some_and(|key| ID_KEYS.contains(&key));
        if !is_id_value {
            return Open::Plain;
        }
        for (prefix, tagged) in PREFIXES.iter().zip(&self.tagged) {
            // Wait until the bytes can tell an untagged id from one already tagged.
            if !end && after.len() < tagged.len() && tagged.starts_with(after) {
                return Open::Wait;
            }
            if after.starts_with(prefix.as_bytes()) && !after.starts_with(tagged) {
                return Open::Tag(prefix);
            }
        }
        Open::Plain
    }
}

/// How a string opening is handled.
enum Open {
    /// Too few bytes yet to decide.
    Wait,
    /// An untagged id value with this prefix: tag it.
    Tag(&'static str),
    /// Any other string.
    Plain,
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
    use super::*;

    /// `text` with each `resp_<site>.` and `conv_<site>.` for east and west tagged as a
    /// gateway with no key tags a response from cluster `pool`.
    fn t(text: &str) -> String {
        ["east", "west"].iter().fold(text.to_owned(), |text, site| {
            let tagged = tag(site, "pool", None);
            text.replace(&format!("resp_{site}."), &format!("resp_{tagged}"))
                .replace(&format!("conv_{site}."), &format!("conv_{tagged}"))
        })
    }

    fn key(byte: u8) -> TagKey {
        TagKey::new(Zeroizing::new(vec![byte; MIN_KEY_BYTES])).unwrap()
    }

    #[test]
    fn a_tagged_id_splits_into_site_cluster_and_upstream_id() {
        let mark = cluster_mark("pool");
        assert_eq!(
            untag(&t("resp_east.abc123"), None),
            Some(Pinned {
                site: "east",
                mark: &mark,
                upstream: "resp_abc123".to_owned()
            })
        );
        assert_eq!(
            untag(&t("conv_west.c1"), None).map(|pinned| pinned.upstream),
            Some("conv_c1".to_owned())
        );
        // The first three periods split the tag, so the id keeps the rest.
        assert_eq!(
            untag(&t("resp_east.a.b.c"), None).map(|pinned| (pinned.site, pinned.upstream)),
            Some(("east", "resp_a.b.c".to_owned()))
        );
        let no_mark = format!("resp_east.zz{}..abc", mark.chars().skip(2).collect::<String>());
        for untagged in [
            "resp_abc123",
            "msg_east.abc",
            "resp_east.abc",
            &no_mark,
            &t("resp_.abc"),
            "abc",
        ] {
            assert_eq!(untag(untagged, None), None, "{untagged}");
        }
    }

    #[test]
    fn with_a_key_only_a_tag_it_made_is_accepted() {
        let (ours, theirs) = (key(1), key(2));
        let id = format!("resp_{}a1", tag("east", "pool", Some(&ours)));
        assert_eq!(untag(&id, Some(&ours)).map(|pinned| pinned.site), Some("east"));
        assert_eq!(untag(&id, Some(&theirs)), None, "another key's mac");
        assert_eq!(untag(&t("resp_east.a1"), Some(&ours)), None, "no mac");
        let steered = id.replacen("resp_east.", "resp_west.", 1);
        assert_eq!(untag(&steered, Some(&ours)), None, "a mac for another site");
        let parts: Vec<&str> = id.split('.').collect();
        let swapped = format!("{}.{}.{}.{}", parts[0], cluster_mark("other"), parts[2], parts[3]);
        assert_eq!(untag(&swapped, Some(&ours)), None, "a mark from another cluster");
        assert_eq!(
            TagKey::new(Zeroizing::new(vec![0; MIN_KEY_BYTES - 1])).map(drop).ok(),
            None
        );
        assert_eq!(format!("{ours:?}"), "TagKey(..)");
    }

    #[test]
    fn paths_name_a_collection_or_a_stored_item() {
        assert_eq!(
            state_path("/v1/responses"),
            Some(StatePath::Create(Collection::Responses))
        );
        assert_eq!(
            state_path("/v1/conversations/"),
            Some(StatePath::Create(Collection::Conversations))
        );
        assert_eq!(
            state_path("/v1/responses/resp_east.a1/cancel"),
            Some(StatePath::Stored {
                collection: Collection::Responses,
                id: "resp_east.a1",
                rest: "/cancel"
            })
        );
        assert_eq!(
            state_path("/v1/conversations/conv_east.c1/items"),
            Some(StatePath::Stored {
                collection: Collection::Conversations,
                id: "conv_east.c1",
                rest: "/items"
            })
        );
        assert_eq!(state_path("/v1/responsesx"), None);
        assert_eq!(state_path("/v1/chat/completions"), None);
        assert_eq!(
            upstream_path(Collection::Responses, "resp_a1", "/input_items"),
            "/v1/responses/resp_a1/input_items"
        );
    }

    #[test]
    fn the_body_loses_its_tags_but_user_text_keeps_them() {
        let body = t(r#"{"input":"I saw \"resp_east.a1\" earlier","previous_response_id":"resp_east.a1"}"#);
        let ((site, _), stripped) = pinned_body(body.as_bytes(), None).unwrap();
        assert_eq!(&*site, "east");
        assert_eq!(
            String::from_utf8(stripped).unwrap(),
            t(r#"{"input":"I saw \"resp_east.a1\" earlier","previous_response_id":"resp_a1"}"#)
        );
    }

    #[test]
    fn an_escaped_id_is_decoded_and_a_conversation_object_stripped() {
        let body = t(r#"{"previous_response_id":"resp_east.a1","conversation":{"id":"conv_east.c1"}}"#);
        let ((site, _), stripped) = pinned_body(body.as_bytes(), None).unwrap();
        assert_eq!(&*site, "east");
        assert_eq!(
            String::from_utf8(stripped).unwrap(),
            r#"{"previous_response_id":"resp_a1","conversation":{"id":"conv_c1"}}"#
        );
        let by_string = t(r#"{"input":"x","conversation":"conv_west.c9"}"#);
        assert_eq!(&*pinned_body(by_string.as_bytes(), None).unwrap().0.0, "west");
        assert!(pinned_body(br#"{"input":"x","previous_response_id":"resp_a1"}"#, None).is_none());
    }

    fn tag_all(site: &str, body: &[u8], split: usize) -> String {
        let mut tagger = Tagger::new(tag(site, "pool", None));
        let mut out = Vec::new();
        let mut take = |chunk: &[u8], pushed: Pushed| match pushed {
            Pushed::Unchanged => out.extend_from_slice(chunk),
            Pushed::Replaced(bytes) => out.extend_from_slice(&bytes),
        };
        for chunk in body.chunks(split.max(1)) {
            take(chunk, tagger.push(chunk, false));
        }
        take(&[], tagger.push(&[], true));
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn ids_are_tagged_wherever_a_chunk_splits() {
        let body = br#"{"id": "resp_a1","object":"response","previous_response_id":"resp_z9","conversation":{"id":"conv_c1"},"output":[{"id":"msg_1","content":[{"text":"resp_x"}]}]}"#;
        let want = t(
            r#"{"id": "resp_east.a1","object":"response","previous_response_id":"resp_east.z9","conversation":{"id":"conv_east.c1"},"output":[{"id":"msg_1","content":[{"text":"resp_x"}]}]}"#,
        );
        for split in 1..body.len() {
            assert_eq!(tag_all("east", body, split), want, "split every {split} bytes");
        }
    }

    #[test]
    fn generated_text_starting_with_an_id_prefix_is_left_alone() {
        let body = b"event: response.output_text.delta\ndata: {\"type\":\"response.output_text.delta\",\"delta\":\"resp_q\",\"text\":\"conv_r\"}\n\nevent: response.created\ndata: {\"type\":\"response.created\",\"response\":{\"id\":\"resp_a1\"}}\n\n";
        let tagged = tag_all("west", body, 5);
        assert!(tagged.contains(r#""delta":"resp_q""#), "{tagged}");
        assert!(tagged.contains(r#""text":"conv_r""#), "{tagged}");
        assert!(tagged.contains(&t(r#""response":{"id":"resp_west.a1"}"#)), "{tagged}");
        assert_eq!(tagged.matches("\n\n").count(), 2);
    }

    #[test]
    fn only_the_responses_own_ids_are_tagged() {
        let body = br#"{"id":"resp_a1","metadata":{"id":"resp_m","conversation":"conv_m"},"tools":[{"id":"resp_t"}],"input":[{"previous_response_id":"resp_i"}],"conversation":{"id":"conv_c1","metadata":{"id":"conv_n"}}}"#;
        let want = t(
            r#"{"id":"resp_east.a1","metadata":{"id":"resp_m","conversation":"conv_m"},"tools":[{"id":"resp_t"}],"input":[{"previous_response_id":"resp_i"}],"conversation":{"id":"conv_east.c1","metadata":{"id":"conv_n"}}}"#,
        );
        for split in 1..body.len() {
            assert_eq!(tag_all("east", body, split), want, "split every {split} bytes");
        }
        let event = b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_a1\",\"metadata\":{\"id\":\"resp_m\"}}}\n\ndata: {\"id\":\"resp_b2\"}\n\n";
        let tagged = tag_all("east", event, 4);
        assert!(
            tagged.contains(&t(r#""response":{"id":"resp_east.a1","metadata":{"id":"resp_m"}}"#)),
            "{tagged}"
        );
        assert!(
            tagged.contains(&t(r#"{"id":"resp_east.b2"}"#)),
            "each event starts at the top: {tagged}"
        );
    }

    #[test]
    fn a_key_inside_a_string_does_not_count() {
        let body = br#"{"note":"id: see","x":"resp_1","text":"\"id\":\"resp_2\""}"#;
        assert_eq!(tag_all("east", body, 3), String::from_utf8_lossy(body));
    }

    #[test]
    fn an_already_tagged_id_is_not_tagged_twice() {
        assert_eq!(
            tag_all("east", t(r#"{"id":"resp_east.a1"}"#).as_bytes(), 3),
            t(r#"{"id":"resp_east.a1"}"#)
        );
    }

    #[test]
    fn a_chunk_with_nothing_to_tag_is_sent_as_it_is() {
        let mut tagger = Tagger::new(tag("east", "pool", None));
        assert_eq!(tagger.push(br#"data: {"delta":"resp_q"}"#, false), Pushed::Unchanged);
        assert_eq!(
            tagger.push(br#"{"id":"re"#, false),
            Pushed::Replaced(Bytes::from_static(br#"{"id":"#)),
            "a possible prefix is held back"
        );
        assert_eq!(
            tagger.push(br#"sp_a1"}"#, true),
            Pushed::Replaced(Bytes::from(t(r#""resp_east.a1"}"#)))
        );
    }

    #[test]
    fn a_body_ending_mid_prefix_is_flushed() {
        assert_eq!(tag_all("east", br#"{"id":"res"#, 2), r#"{"id":"res"#);
    }
}
