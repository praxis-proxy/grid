//! Stored-state pinning: a stored response or conversation lives on the site that made it.
//!
//! Ids leave the gateway tagged with that site, `resp_<site>.<id>` or
//! `conv_<site>.<id>`, and come back stripped. A request naming one goes only there.

use serde::Deserialize;
use serde_json::value::RawValue;

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

/// A tagged id, split into the site and the id the site knows.
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Pinned<'id> {
    /// The site that stored the state.
    pub(crate) site: &'id str,
    /// The id without the site tag.
    pub(crate) upstream: String,
}

/// Split a tagged id. `None` for an id the gateway did not tag.
pub(crate) fn untag(id: &str) -> Option<Pinned<'_>> {
    PREFIXES.iter().find_map(|prefix| {
        let (site, rest) = id.strip_prefix(prefix)?.split_once('.')?;
        (!site.is_empty() && !rest.is_empty()).then(|| Pinned {
            site,
            upstream: format!("{prefix}{rest}"),
        })
    })
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
pub(crate) fn pinned_body(body: &[u8]) -> Option<(String, Vec<u8>)> {
    let fields: Continues<'_> = serde_json::from_slice(body).ok()?;
    let conversation = fields.conversation.and_then(|value| {
        serde_json::from_str::<ConversationRef<'_>>(value.get())
            .map(|reference| reference.id)
            .ok()
            .or(Some(value))
    });
    let mut site = None;
    let mut edits = Vec::new();
    for token in [fields.previous_response_id, conversation].into_iter().flatten() {
        let Ok(id) = serde_json::from_str::<String>(token.get()) else {
            continue;
        };
        let Some(pinned) = untag(&id) else {
            continue;
        };
        site.get_or_insert_with(|| pinned.site.to_owned());
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
    /// `<site>.`, inserted after the prefix.
    tag: Vec<u8>,
    /// The last byte outside a string that was not whitespace.
    last: u8,
    /// Whether the scanner is inside a string.
    in_string: bool,
    /// Whether the previous byte in a string was an unescaped backslash.
    escaped: bool,
    /// The string being read, kept only while it could still be an id key.
    current: Vec<u8>,
    /// The last string closed, when short enough to be an id key.
    key: Option<Vec<u8>>,
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

impl Tagger {
    /// A tagger for state stored at `site`.
    pub(crate) fn new(site: &str) -> Self {
        let mut tag = site.as_bytes().to_vec();
        tag.push(b'.');
        Self {
            tag,
            last: 0,
            in_string: false,
            escaped: false,
            current: Vec::new(),
            key: None,
            held: Vec::new(),
            depth: 0,
            own_depth: 0,
        }
    }

    /// Follow an object or array opening or closing outside a string.
    fn nest(&mut self, byte: u8) {
        match byte {
            b'{' | b'[' => {
                let named = self.last == b':' && self.key.as_deref().is_some_and(|key| OWN_KEYS.contains(&key));
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

    /// Tag the ids in `chunk`, holding back a possible partial match unless `end`.
    pub(crate) fn push(&mut self, chunk: &[u8], end: bool) -> Vec<u8> {
        let mut input = std::mem::take(&mut self.held);
        input.extend_from_slice(chunk);
        let mut out = Vec::with_capacity(input.len().saturating_add(self.tag.len()));
        let mut at = 0;
        while let Some(&byte) = input.get(at) {
            if !self.in_string && byte == b'"' {
                let after = input.get(at.saturating_add(1)..).unwrap_or_default();
                match self.open(after, end) {
                    Open::Wait => {
                        self.held = input.get(at..).unwrap_or_default().to_vec();
                        return out;
                    },
                    open @ (Open::Tag(_) | Open::Plain) => {
                        at = at.saturating_add(self.quote(&open, &mut out));
                        continue;
                    },
                }
            }
            if self.in_string {
                self.string_byte(byte);
            } else if !byte.is_ascii_whitespace() {
                self.nest(byte);
                self.last = byte;
            }
            out.push(byte);
            at = at.saturating_add(1);
        }
        out
    }

    /// Open a string, tagging it when `open` says so. Returns the input bytes consumed.
    fn quote(&mut self, open: &Open, out: &mut Vec<u8>) -> usize {
        self.in_string = true;
        self.current.clear();
        out.push(b'"');
        match open {
            &Open::Tag(prefix) => {
                out.extend_from_slice(prefix);
                out.extend_from_slice(&self.tag);
                prefix.len().saturating_add(1)
            },
            Open::Wait | Open::Plain => 1,
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
            self.key = (self.current.len() <= KEY_LIMIT).then(|| std::mem::take(&mut self.current));
            return;
        }
        if self.current.len() <= KEY_LIMIT {
            self.current.push(byte);
        }
    }

    /// What to do with a string that opens before `after`.
    fn open(&self, after: &[u8], end: bool) -> Open {
        let own = self.depth > 0 && self.own_depth == self.depth;
        let is_id_value = own && self.last == b':' && self.key.as_deref().is_some_and(|key| ID_KEYS.contains(&key));
        if !is_id_value {
            return Open::Plain;
        }
        for prefix in PREFIXES.map(str::as_bytes) {
            let tagged = [prefix, &self.tag].concat();
            // Wait until the bytes can tell an untagged id from one already tagged.
            if !end && after.len() < tagged.len() && tagged.starts_with(after) {
                return Open::Wait;
            }
            if after.starts_with(prefix) && !after.starts_with(&tagged) {
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
    Tag(&'static [u8]),
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

    #[test]
    fn a_tagged_id_splits_into_site_and_upstream_id() {
        assert_eq!(
            untag("resp_east.abc123"),
            Some(Pinned {
                site: "east",
                upstream: "resp_abc123".to_owned()
            })
        );
        assert_eq!(
            untag("conv_west.c1").map(|pinned| pinned.upstream),
            Some("conv_c1".to_owned())
        );
        // Site names are DNS labels, so the first period ends the site and the id keeps the rest.
        assert_eq!(
            untag("resp_east.a.b.c").map(|pinned| (pinned.site, pinned.upstream)),
            Some(("east", "resp_a.b.c".to_owned()))
        );
        for untagged in ["resp_abc123", "msg_east.abc", "resp_.abc", "resp_east.", "abc"] {
            assert_eq!(untag(untagged), None, "{untagged}");
        }
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
        let body = br#"{"input":"I saw \"resp_east.a1\" earlier","previous_response_id":"resp_east.a1"}"#;
        let (site, stripped) = pinned_body(body).unwrap();
        assert_eq!(site, "east");
        assert_eq!(
            String::from_utf8(stripped).unwrap(),
            r#"{"input":"I saw \"resp_east.a1\" earlier","previous_response_id":"resp_a1"}"#
        );
    }

    #[test]
    fn an_escaped_id_is_decoded_and_a_conversation_object_stripped() {
        let body = br#"{"previous_response_id":"resp_east.a1","conversation":{"id":"conv_east.c1"}}"#;
        let (site, stripped) = pinned_body(body).unwrap();
        assert_eq!(site, "east");
        assert_eq!(
            String::from_utf8(stripped).unwrap(),
            r#"{"previous_response_id":"resp_a1","conversation":{"id":"conv_c1"}}"#
        );
        let by_string = br#"{"input":"x","conversation":"conv_west.c9"}"#;
        assert_eq!(pinned_body(by_string).unwrap().0, "west");
        assert!(pinned_body(br#"{"input":"x","previous_response_id":"resp_a1"}"#).is_none());
    }

    fn tag_all(site: &str, body: &[u8], split: usize) -> String {
        let mut tagger = Tagger::new(site);
        let mut out = Vec::new();
        for chunk in body.chunks(split.max(1)) {
            out.extend(tagger.push(chunk, false));
        }
        out.extend(tagger.push(&[], true));
        String::from_utf8(out).unwrap()
    }

    #[test]
    fn ids_are_tagged_wherever_a_chunk_splits() {
        let body = br#"{"id": "resp_a1","object":"response","previous_response_id":"resp_z9","conversation":{"id":"conv_c1"},"output":[{"id":"msg_1","content":[{"text":"resp_x"}]}]}"#;
        let want = r#"{"id": "resp_east.a1","object":"response","previous_response_id":"resp_east.z9","conversation":{"id":"conv_east.c1"},"output":[{"id":"msg_1","content":[{"text":"resp_x"}]}]}"#;
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
        assert!(tagged.contains(r#""response":{"id":"resp_west.a1"}"#), "{tagged}");
        assert_eq!(tagged.matches("\n\n").count(), 2);
    }

    #[test]
    fn only_the_responses_own_ids_are_tagged() {
        let body = br#"{"id":"resp_a1","metadata":{"id":"resp_m","conversation":"conv_m"},"tools":[{"id":"resp_t"}],"input":[{"previous_response_id":"resp_i"}],"conversation":{"id":"conv_c1","metadata":{"id":"conv_n"}}}"#;
        let want = r#"{"id":"resp_east.a1","metadata":{"id":"resp_m","conversation":"conv_m"},"tools":[{"id":"resp_t"}],"input":[{"previous_response_id":"resp_i"}],"conversation":{"id":"conv_east.c1","metadata":{"id":"conv_n"}}}"#;
        for split in 1..body.len() {
            assert_eq!(tag_all("east", body, split), want, "split every {split} bytes");
        }
        let event = b"data: {\"type\":\"response.completed\",\"response\":{\"id\":\"resp_a1\",\"metadata\":{\"id\":\"resp_m\"}}}\n\ndata: {\"id\":\"resp_b2\"}\n\n";
        let tagged = tag_all("east", event, 4);
        assert!(
            tagged.contains(r#""response":{"id":"resp_east.a1","metadata":{"id":"resp_m"}}"#),
            "{tagged}"
        );
        assert!(
            tagged.contains(r#"{"id":"resp_east.b2"}"#),
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
            tag_all("east", br#"{"id":"resp_east.a1"}"#, 3),
            r#"{"id":"resp_east.a1"}"#
        );
    }

    #[test]
    fn a_body_ending_mid_prefix_is_flushed() {
        assert_eq!(tag_all("east", br#"{"id":"res"#, 2), r#"{"id":"res"#);
    }
}
