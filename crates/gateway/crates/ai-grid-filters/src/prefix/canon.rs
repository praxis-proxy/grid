//! The canonical prompt stream of each API, written straight into a [`Chain`].
//!
//! Only what the engine caches as a prefix goes in: templates, tools, roles,
//! text, tool calls, and a fixed digest per media part. Reasoning, generation
//! parameters and field order do not, so turns of one conversation share a head.
//! A part, block or item of a type not listed here gives the request no key.

use std::{borrow::Cow, io::Write as _, marker::PhantomData};

use serde::{
    Deserialize, Deserializer as _,
    de::{IgnoredAny, SeqAccess, Visitor},
};
use serde_json::value::RawValue;
use xxhash_rust::xxh64::xxh64;

use super::{Api, Chain};

/// Ends a field in the stream.
const FIELD: u8 = 0x1F;

/// Seeds the digest of a media part, apart from the chain's seeds.
const MEDIA: u64 = 0x6D65_6469_615F_7631;

/// Seeds the digest of a JSON value too large for canonical form.
const LARGE: u64 = 0x6C61_7267_655F_7631;

/// Largest JSON value put in canonical form. A larger one is keyed by a digest of
/// its bytes as sent, since a parsed tree costs many times its size in memory.
const CANONICAL_LIMIT: usize = 16 << 10;

/// Prefix of an Anthropic system block that changes on every request.
const BILLING_HEADER: &str = "x-anthropic-billing-header";

/// Parse `body` as `api`, seed a chain from its `cache_salt`, and write its prompt.
pub(super) fn canonical(api: Api, body: &[u8], chain: impl FnOnce(Option<&str>) -> Chain) -> Option<Chain> {
    match api {
        Api::ChatCompletions => chat(&serde_json::from_slice(body).ok()?, chain),
        Api::Completions => completion(&serde_json::from_slice(body).ok()?, chain),
        Api::Messages => messages(&serde_json::from_slice(body).ok()?, chain),
        Api::Responses => responses(&serde_json::from_slice(body).ok()?, chain),
    }
}

/// Template, tools, then each message.
fn chat(request: &ChatCompletion<'_>, chain: impl FnOnce(Option<&str>) -> Chain) -> Option<Chain> {
    let mut out = Out(chain(request.cache_salt.as_deref()));
    if let Some(template) = request.chat_template {
        out.text(b"chat_template", template);
    }
    out.json(b"chat_template_kwargs", request.chat_template_kwargs)?;
    out.json(b"documents", request.documents)?;
    out.json(b"tools", request.tools)?;
    out.each(request.messages, |out, message: ChatMessage<'_>| {
        out.chat_message(&message)
    })?;
    Some(out.0)
}

/// The one prompt string.
fn completion(request: &Completion<'_>, chain: impl FnOnce(Option<&str>) -> Chain) -> Option<Chain> {
    let prompt = single_string(request.prompt)?;
    let mut out = Out(chain(request.cache_salt.as_deref()));
    out.text(b"text", prompt);
    Some(out.0)
}

/// Tools, the system prompt, then each message.
fn messages(request: &Anthropic<'_>, chain: impl FnOnce(Option<&str>) -> Chain) -> Option<Chain> {
    let mut out = Out(chain(None));
    out.json(b"tools", request.tools)?;
    if let Some(system) = request.system {
        out.anthropic_content(b"system", system, true)?;
    }
    out.each(request.messages, |out, message: AnthropicMessage<'_>| {
        out.anthropic_content(message.role.as_bytes(), message.content, false)
    })?;
    Some(out.0)
}

/// Tools, instructions, then the input. A request that continues a stored
/// response or conversation is pinned to its site, so it gets no key.
pub(super) fn responses(request: &Responses<'_>, chain: impl FnOnce(Option<&str>) -> Chain) -> Option<Chain> {
    if request.previous_response_id.is_some() || request.conversation.is_some() {
        return None;
    }
    let mut out = Out(chain(request.cache_salt.as_deref()));
    out.json(b"tools", request.tools)?;
    if let Some(instructions) = request.instructions {
        out.text(b"instructions", instructions);
    }
    out.responses_input(request.input?)?;
    Some(out.0)
}

/// The stream writer over a chain.
struct Out(Chain);

/// Writes each element of a JSON array as it is read, so the array is never held whole.
struct Each<'out, T, F> {
    /// The stream.
    out: &'out mut Out,
    /// Writes one element; `None` when the element gives the request no key.
    write: F,
    /// The element type.
    item: PhantomData<T>,
}

impl<'de, T: Deserialize<'de>, F: FnMut(&mut Out, T) -> Option<()>> Visitor<'de> for Each<'_, T, F> {
    type Value = Option<()>;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("an array")
    }

    fn visit_seq<A: SeqAccess<'de>>(mut self, mut seq: A) -> Result<Self::Value, A::Error> {
        let mut written = Some(());
        while !self.out.full() {
            let Some(item) = seq.next_element::<T>()? else {
                return Ok(written);
            };
            written = (self.write)(self.out, item);
            if written.is_none() {
                break;
            }
        }
        // The rest is read past without being kept.
        while seq.next_element::<IgnoredAny>()?.is_some() {}
        Ok(written)
    }
}

impl Out {
    /// A tagged JSON string as sent, escapes included, so nothing is decoded or copied.
    fn text(&mut self, tag: &[u8], text: &RawValue) {
        self.field(tag, text.get().as_bytes());
    }

    /// A tagged fixed-size digest of a media part, so one image cannot fill the cap.
    fn media(&mut self, tag: &[u8], content: &RawValue) {
        self.field(tag, &xxh64(content.get().as_bytes(), MEDIA).to_be_bytes());
    }

    /// Whether every key is taken, so the rest of the prompt changes nothing.
    fn full(&self) -> bool {
        self.0.full()
    }

    /// Write each element of the array `raw` with `write`, one at a time, until
    /// the chain is full. `None` when it is not an array of `T` or `write` says so.
    fn each<'body, T: Deserialize<'body>>(
        &mut self,
        raw: &'body RawValue,
        write: impl FnMut(&mut Self, T) -> Option<()>,
    ) -> Option<()> {
        let mut reader = serde_json::Deserializer::from_str(raw.get());
        reader
            .deserialize_seq(Each {
                out: self,
                write,
                item: PhantomData,
            })
            .ok()?
    }

    /// A tagged JSON value with sorted keys and no whitespace, or a digest of its
    /// bytes past [`CANONICAL_LIMIT`]. `None` when it does not parse.
    fn json(&mut self, tag: &[u8], value: Option<&RawValue>) -> Option<()> {
        let Some(value) = value.filter(|_| !self.full()) else {
            return Some(());
        };
        if value.get().len() > CANONICAL_LIMIT {
            self.field(tag, &xxh64(value.get().as_bytes(), LARGE).to_be_bytes());
            return Some(());
        }
        let value: serde_json::Value = serde_json::from_str(value.get()).ok()?;
        self.raw(tag);
        self.raw(&[FIELD]);
        self.sorted(value);
        self.raw(&[FIELD]);
        Some(())
    }

    /// Tool call arguments: canonical when they are JSON, else the string as sent.
    fn arguments(&mut self, arguments: Option<&RawValue>) {
        let Some(arguments) = arguments.filter(|_| !self.full()) else {
            return;
        };
        if arguments.get().len() > CANONICAL_LIMIT {
            return self.text(b"arguments", arguments);
        }
        let parsed = serde_json::from_str::<Cow<'_, str>>(arguments.get())
            .ok()
            .and_then(|text| serde_json::from_str::<serde_json::Value>(&text).ok());
        match parsed {
            Some(value) => {
                self.raw(b"arguments");
                self.raw(&[FIELD]);
                self.sorted(value);
                self.raw(&[FIELD]);
            },
            None => self.text(b"arguments", arguments),
        }
    }

    /// A tag, its content, and the field ends.
    fn field(&mut self, tag: &[u8], content: &[u8]) {
        self.raw(tag);
        self.raw(&[FIELD]);
        self.raw(content);
        self.raw(&[FIELD]);
    }

    /// Write `value` with object keys sorted, whatever order the map keeps.
    fn sorted(&mut self, mut value: serde_json::Value) {
        value.sort_all_objects();
        self.serialize(&value);
    }

    /// A value in compact JSON form.
    fn serialize<T: serde::Serialize + ?Sized>(&mut self, value: &T) {
        // Writing into the chain never fails.
        let _written = serde_json::to_writer(&mut self.0, value);
    }

    /// Bytes into the chain.
    fn raw(&mut self, bytes: &[u8]) {
        // Writing into the chain never fails.
        let _written = self.0.write_all(bytes);
    }

    /// One Chat Completions message: role, content, then tool call names and arguments.
    fn chat_message(&mut self, message: &ChatMessage<'_>) -> Option<()> {
        self.field(b"role", message.role.as_bytes());
        if let Some(content) = message.content {
            match kind(content) {
                Kind::String => self.text(b"text", content),
                Kind::Array => {
                    self.each(content, |out, part: ChatPart<'_>| out.chat_part(&part))?;
                },
                Kind::Null => {},
                Kind::Other => return None,
            }
        }
        if let Some(calls) = message.tool_calls {
            self.each(calls, |out, call: ToolCall<'_>| {
                if let Some(function) = &call.function {
                    out.field(b"tool_call", function.name.as_bytes());
                    out.arguments(function.arguments);
                }
                Some(())
            })?;
        }
        Some(())
    }

    /// One Chat Completions content part. `None` for a part type it does not know.
    fn chat_part(&mut self, part: &ChatPart<'_>) -> Option<()> {
        match &*part.kind {
            "text" => self.text(b"text", part.text?),
            "refusal" => self.text(b"text", part.refusal?),
            "image_url" | "video_url" | "audio_url" => {
                let url = match &*part.kind {
                    "image_url" => part.image_url.as_ref(),
                    "video_url" => part.video_url.as_ref(),
                    _ => part.audio_url.as_ref(),
                };
                // vLLM's per-item uuid names the media when the URL may change or expire.
                self.media(part.kind.as_bytes(), part.uuid.or_else(|| url.map(|url| url.url))?);
            },
            "input_audio" => self.media(b"input_audio", part.input_audio.as_ref()?.data),
            "file" => {
                let file = part.file.as_ref()?;
                self.media(b"file", file.file_data.or(file.file_id)?);
            },
            "image_embeds" => self.media(b"image_embeds", part.image_embeds?),
            _ => return None,
        }
        Some(())
    }

    /// Anthropic content: a string, or blocks with thinking skipped.
    fn anthropic_content(&mut self, role: &[u8], content: &RawValue, system: bool) -> Option<()> {
        self.field(b"role", role);
        match kind(content) {
            Kind::String => self.text(b"text", content),
            Kind::Array => {
                self.each(content, |out, block: Block<'_>| out.anthropic_block(&block, system))?;
            },
            Kind::Null => {},
            Kind::Other => return None,
        }
        Some(())
    }

    /// One Anthropic content block. `None` for a block type it does not know.
    fn anthropic_block(&mut self, block: &Block<'_>, system: bool) -> Option<()> {
        match &*block.kind {
            "text" => {
                let text = block.text?;
                let billing = system
                    && serde_json::from_str::<Cow<'_, str>>(text.get())
                        .is_ok_and(|text| text.starts_with(BILLING_HEADER));
                if !billing {
                    self.text(b"text", text);
                }
            },
            "image" | "document" => {
                let source = block.source.as_ref()?;
                self.media(block.kind.as_bytes(), source.data.or(source.url).or(source.file_id)?);
            },
            "tool_use" => {
                self.field(b"tool_call", block.name.as_deref()?.as_bytes());
                self.json(b"arguments", block.input)?;
            },
            "tool_result" => {
                if let Some(content) = block.content {
                    self.anthropic_content(b"tool_result", content, false)?;
                }
            },
            // Thinking is not part of the cached prompt.
            "thinking" | "redacted_thinking" => {},
            _ => return None,
        }
        Some(())
    }

    /// Responses input: a string, or items with reasoning skipped.
    fn responses_input(&mut self, input: &RawValue) -> Option<()> {
        match kind(input) {
            Kind::String => {
                self.field(b"role", b"user");
                self.text(b"text", input);
            },
            Kind::Array => {
                self.each(input, |out, item: Item<'_>| out.responses_item(&item))?;
            },
            Kind::Null | Kind::Other => return None,
        }
        Some(())
    }

    /// One Responses input item. `None` for an item type it does not know.
    fn responses_item(&mut self, item: &Item<'_>) -> Option<()> {
        match item.kind.as_deref().unwrap_or("message") {
            "message" => {
                self.field(b"role", item.role.as_deref()?.as_bytes());
                if let Some(content) = item.content {
                    self.responses_content(content)?;
                }
            },
            "function_call" => {
                self.field(b"tool_call", item.name.as_deref()?.as_bytes());
                self.arguments(item.arguments);
            },
            "function_call_output" => self.tool_output(item.output?)?,
            // Reasoning is not part of the cached prompt.
            "reasoning" => {},
            _ => return None,
        }
        Some(())
    }

    /// A Responses message's content: a string or parts.
    fn responses_content(&mut self, content: &RawValue) -> Option<()> {
        match kind(content) {
            Kind::String => self.text(b"text", content),
            Kind::Array => {
                self.each(content, |out, part: ResponsesPart<'_>| out.responses_part(&part))?;
            },
            Kind::Null => {},
            Kind::Other => return None,
        }
        Some(())
    }

    /// One Responses content part. `None` for a part type it does not know.
    fn responses_part(&mut self, part: &ResponsesPart<'_>) -> Option<()> {
        match &*part.kind {
            "input_text" | "output_text" => self.text(b"text", part.text?),
            "refusal" => self.text(b"text", part.refusal?),
            "input_image" => self.media(b"image_url", part.image_url.or(part.file_id)?),
            "input_file" => self.media(b"file", part.file_data.or(part.file_id).or(part.file_url)?),
            _ => return None,
        }
        Some(())
    }

    /// A tool's output: a string as sent, anything else as canonical JSON.
    fn tool_output(&mut self, output: &RawValue) -> Option<()> {
        match kind(output) {
            Kind::String => self.text(b"tool_result", output),
            Kind::Array | Kind::Null | Kind::Other => self.json(b"tool_result", Some(output))?,
        }
        Some(())
    }
}

/// The JSON type of a raw value, from its first byte.
enum Kind {
    /// A JSON string.
    String,
    /// A JSON array.
    Array,
    /// JSON null.
    Null,
    /// Anything else.
    Other,
}

/// The [`Kind`] of `value`.
fn kind(value: &RawValue) -> Kind {
    match value.get().trim_start().as_bytes().first() {
        Some(b'"') => Kind::String,
        Some(b'[') => Kind::Array,
        Some(b'n') => Kind::Null,
        _ => Kind::Other,
    }
}

/// A Completions prompt that is one string, alone or in a one-element array. Token arrays give none.
fn single_string(prompt: Option<&RawValue>) -> Option<&RawValue> {
    let prompt = prompt?;
    match kind(prompt) {
        Kind::String => Some(prompt),
        // A one-tuple refuses a longer array at its second element, so a token array is never held.
        Kind::Array => {
            let (one,) = serde_json::from_str::<(&RawValue,)>(prompt.get()).ok()?;
            matches!(kind(one), Kind::String).then_some(one)
        },
        Kind::Null | Kind::Other => None,
    }
}

/// The Chat Completions fields that make up the prompt.
#[derive(Deserialize)]
struct ChatCompletion<'body> {
    /// The conversation.
    #[serde(borrow)]
    messages: &'body RawValue,
    /// Tool definitions, keyed in canonical form.
    #[serde(borrow, default)]
    tools: Option<&'body RawValue>,
    /// A template that replaces the model's own.
    #[serde(borrow, default)]
    chat_template: Option<&'body RawValue>,
    /// Template arguments, such as a thinking switch.
    #[serde(borrow, default)]
    chat_template_kwargs: Option<&'body RawValue>,
    /// Retrieved documents the template renders.
    #[serde(borrow, default)]
    documents: Option<&'body RawValue>,
    /// vLLM's per-tenant cache isolation, part of the seed.
    #[serde(borrow, default)]
    cache_salt: Option<Cow<'body, str>>,
}

/// One Chat Completions message. Reasoning fields are not read.
#[derive(Deserialize)]
struct ChatMessage<'body> {
    /// The speaker.
    #[serde(borrow)]
    role: Cow<'body, str>,
    /// A string or content parts.
    #[serde(borrow, default)]
    content: Option<&'body RawValue>,
    /// The assistant's tool calls.
    #[serde(borrow, default)]
    tool_calls: Option<&'body RawValue>,
}

/// One assistant tool call.
#[derive(Deserialize)]
struct ToolCall<'body> {
    /// The called function.
    #[serde(borrow, default)]
    function: Option<Function<'body>>,
}

/// A called function.
#[derive(Deserialize)]
struct Function<'body> {
    /// The function name.
    #[serde(borrow)]
    name: Cow<'body, str>,
    /// Its arguments, a JSON string.
    #[serde(borrow, default)]
    arguments: Option<&'body RawValue>,
}

/// One Chat Completions content part.
#[derive(Deserialize)]
struct ChatPart<'body> {
    /// The part type.
    #[serde(borrow, rename = "type")]
    kind: Cow<'body, str>,
    /// A text part.
    #[serde(borrow, default)]
    text: Option<&'body RawValue>,
    /// A refusal part.
    #[serde(borrow, default)]
    refusal: Option<&'body RawValue>,
    /// An image part.
    #[serde(borrow, default)]
    image_url: Option<Url<'body>>,
    /// A video part.
    #[serde(borrow, default)]
    video_url: Option<Url<'body>>,
    /// An audio part by URL.
    #[serde(borrow, default)]
    audio_url: Option<Url<'body>>,
    /// Inline audio.
    #[serde(borrow, default)]
    input_audio: Option<Audio<'body>>,
    /// A file part.
    #[serde(borrow, default)]
    file: Option<File<'body>>,
    /// Precomputed image embeddings.
    #[serde(borrow, default)]
    image_embeds: Option<&'body RawValue>,
    /// vLLM's stable id for the media item.
    #[serde(borrow, default)]
    uuid: Option<&'body RawValue>,
}

/// A media reference.
#[derive(Deserialize)]
struct Url<'body> {
    /// A URL or data URI.
    #[serde(borrow)]
    url: &'body RawValue,
}

/// Inline audio.
#[derive(Deserialize)]
struct Audio<'body> {
    /// The encoded audio.
    #[serde(borrow)]
    data: &'body RawValue,
}

/// An inline or uploaded file.
#[derive(Deserialize)]
struct File<'body> {
    /// The encoded file.
    #[serde(borrow, default)]
    file_data: Option<&'body RawValue>,
    /// An uploaded file's id.
    #[serde(borrow, default)]
    file_id: Option<&'body RawValue>,
}

/// The Completions fields that make up the prompt.
#[derive(Deserialize)]
struct Completion<'body> {
    /// A string, strings, or token arrays.
    #[serde(borrow, default)]
    prompt: Option<&'body RawValue>,
    /// vLLM's per-tenant cache isolation, part of the seed.
    #[serde(borrow, default)]
    cache_salt: Option<Cow<'body, str>>,
}

/// The Anthropic Messages fields that make up the prompt.
#[derive(Deserialize)]
struct Anthropic<'body> {
    /// The conversation.
    #[serde(borrow)]
    messages: &'body RawValue,
    /// A string or text blocks.
    #[serde(borrow, default)]
    system: Option<&'body RawValue>,
    /// Tool definitions, keyed in canonical form.
    #[serde(borrow, default)]
    tools: Option<&'body RawValue>,
}

/// One Anthropic message.
#[derive(Deserialize)]
struct AnthropicMessage<'body> {
    /// The speaker.
    #[serde(borrow)]
    role: Cow<'body, str>,
    /// A string or content blocks.
    #[serde(borrow)]
    content: &'body RawValue,
}

/// One Anthropic content block.
#[derive(Deserialize)]
struct Block<'body> {
    /// The block type.
    #[serde(borrow, rename = "type")]
    kind: Cow<'body, str>,
    /// A text block's text.
    #[serde(borrow, default)]
    text: Option<&'body RawValue>,
    /// An image or document block's source.
    #[serde(borrow, default)]
    source: Option<Source<'body>>,
    /// A tool use block's tool name.
    #[serde(borrow, default)]
    name: Option<Cow<'body, str>>,
    /// A tool use block's input.
    #[serde(borrow, default)]
    input: Option<&'body RawValue>,
    /// A tool result block's content.
    #[serde(borrow, default)]
    content: Option<&'body RawValue>,
}

/// An Anthropic media source.
#[derive(Deserialize)]
struct Source<'body> {
    /// Inline encoded media.
    #[serde(borrow, default)]
    data: Option<&'body RawValue>,
    /// A media URL.
    #[serde(borrow, default)]
    url: Option<&'body RawValue>,
    /// An uploaded file's id.
    #[serde(borrow, default)]
    file_id: Option<&'body RawValue>,
}

/// The Responses fields that make up the prompt, and the stored state it names.
#[derive(Deserialize)]
pub(crate) struct Responses<'body> {
    /// A string or input items.
    #[serde(borrow, default)]
    input: Option<&'body RawValue>,
    /// The system prompt.
    #[serde(borrow, default)]
    instructions: Option<&'body RawValue>,
    /// Tool definitions, keyed in canonical form.
    #[serde(borrow, default)]
    tools: Option<&'body RawValue>,
    /// A stored response this one continues.
    #[serde(borrow, default)]
    pub(crate) previous_response_id: Option<&'body RawValue>,
    /// A stored conversation this one continues.
    #[serde(borrow, default)]
    pub(crate) conversation: Option<&'body RawValue>,
    /// vLLM's per-tenant cache isolation, part of the seed.
    #[serde(borrow, default)]
    cache_salt: Option<Cow<'body, str>>,
}

/// One Responses input item.
#[derive(Deserialize)]
struct Item<'body> {
    /// The item type; a message when absent.
    #[serde(borrow, default, rename = "type")]
    kind: Option<Cow<'body, str>>,
    /// A message's speaker.
    #[serde(borrow, default)]
    role: Option<Cow<'body, str>>,
    /// A message's content.
    #[serde(borrow, default)]
    content: Option<&'body RawValue>,
    /// A function call's name.
    #[serde(borrow, default)]
    name: Option<Cow<'body, str>>,
    /// A function call's arguments.
    #[serde(borrow, default)]
    arguments: Option<&'body RawValue>,
    /// A function call output.
    #[serde(borrow, default)]
    output: Option<&'body RawValue>,
}

/// One Responses content part.
#[derive(Deserialize)]
struct ResponsesPart<'body> {
    /// The part type.
    #[serde(borrow, rename = "type")]
    kind: Cow<'body, str>,
    /// A text part.
    #[serde(borrow, default)]
    text: Option<&'body RawValue>,
    /// A refusal part.
    #[serde(borrow, default)]
    refusal: Option<&'body RawValue>,
    /// An image part's URL or data URI.
    #[serde(borrow, default)]
    image_url: Option<&'body RawValue>,
    /// An inline file.
    #[serde(borrow, default)]
    file_data: Option<&'body RawValue>,
    /// An uploaded file's id.
    #[serde(borrow, default)]
    file_id: Option<&'body RawValue>,
    /// A file by URL.
    #[serde(borrow, default)]
    file_url: Option<&'body RawValue>,
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
    use serde_json::json;

    use super::super::{Api, prefix_keys};

    /// A system prompt long enough to fill several blocks.
    fn system() -> String {
        "You are a careful assistant. ".repeat(40)
    }

    fn keys(api: Api, body: &serde_json::Value) -> Vec<u64> {
        prefix_keys(api, &serde_json::to_vec(body).unwrap())
            .map(|keys| keys.for_model("llama"))
            .map(|keys| keys.as_slice().to_vec())
            .unwrap_or_default()
    }

    fn shared(left: &[u64], right: &[u64]) -> usize {
        left.iter().zip(right).take_while(|(l, r)| l == r).count()
    }

    fn chat(turns: usize) -> serde_json::Value {
        let mut messages = vec![json!({"role": "system", "content": system()})];
        for turn in 0..turns {
            messages.push(json!({"role": "user", "content": format!("question {turn} ").repeat(30)}));
            messages.push(json!({"role": "assistant", "content": format!("answer {turn} ").repeat(30)}));
        }
        json!({"model": "llama", "messages": messages})
    }

    #[test]
    fn a_value_past_the_canonical_limit_keys_by_its_bytes() {
        let tools =
            |name: &str| json!([{"type": "function", "function": {"name": name, "description": "x".repeat(20_000)}}]);
        let body = |name: &str| json!({"tools": tools(name), "messages": [{"role": "system", "content": system()}]});
        let first = keys(Api::ChatCompletions, &body("a"));
        assert!(!first.is_empty());
        assert_eq!(
            first,
            keys(Api::ChatCompletions, &body("a")),
            "the same bytes, the same keys"
        );
        assert_ne!(first, keys(Api::ChatCompletions, &body("b")), "other bytes, other keys");
    }

    #[test]
    fn turns_of_a_conversation_share_a_growing_head() {
        let turns: Vec<_> = (1..=5).map(|turn| keys(Api::ChatCompletions, &chat(turn))).collect();
        for pair in turns.windows(2) {
            let common = shared(&pair[0], &pair[1]);
            assert!(
                common >= pair[0].len().saturating_sub(1),
                "turn k shares its head with turn k+1"
            );
            assert!(pair[1].len() > pair[0].len(), "the shared run grows by turn");
        }
    }

    #[test]
    fn field_order_and_generation_parameters_do_not_change_the_keys() {
        let plain = chat(2);
        let mut reordered = serde_json::Map::new();
        reordered.insert("temperature".into(), json!(0.7));
        reordered.insert("stream".into(), json!(true));
        reordered.insert("messages".into(), plain["messages"].clone());
        reordered.insert("model".into(), json!("llama"));
        assert_eq!(
            keys(Api::ChatCompletions, &plain),
            keys(Api::ChatCompletions, &reordered.into())
        );
    }

    #[test]
    fn tool_key_order_does_not_change_the_keys() {
        let tool = |a: serde_json::Value| json!({"messages": [{"role": "system", "content": system()}], "tools": [a]});
        let one = tool(json!({"type": "function", "function": {"name": "f", "parameters": {"a": 1, "b": 2}}}));
        let two = tool(json!({"function": {"parameters": {"b": 2, "a": 1}, "name": "f"}, "type": "function"}));
        assert_eq!(keys(Api::ChatCompletions, &one), keys(Api::ChatCompletions, &two));
    }

    #[test]
    fn tool_calls_count_and_reasoning_does_not() {
        let call = |arguments: &str, reasoning: &str| {
            json!({"messages": [
                {"role": "system", "content": system()},
                {"role": "assistant", "content": null, "reasoning_content": reasoning,
                 "tool_calls": [{"id": "c1", "type": "function", "function": {"name": "f", "arguments": arguments}}]},
                {"role": "tool", "tool_call_id": "c1", "content": "result ".repeat(60)}
            ]})
        };
        let base = keys(Api::ChatCompletions, &call(r#"{"a":1,"b":2}"#, "think"));
        assert_eq!(
            base,
            keys(Api::ChatCompletions, &call(r#"{ "b": 2, "a": 1 }"#, "other thoughts")),
            "argument formatting and reasoning are not keyed"
        );
        assert_ne!(
            base,
            keys(Api::ChatCompletions, &call(r#"{"a":2}"#, "think")),
            "arguments are keyed"
        );
    }

    #[test]
    fn an_image_is_a_fixed_digest_not_its_bytes() {
        let image = |data: &str| {
            json!({"messages": [{"role": "system", "content": system()},
                {"role": "user", "content": [{"type": "image_url", "image_url": {"url": data}},
                                             {"type": "text", "text": "what is this ".repeat(30)}]}]})
        };
        let big = format!("data:image/png;base64,{}", "A".repeat(4 << 20));
        let small = keys(Api::ChatCompletions, &image("data:image/png;base64,AAAA"));
        let large = keys(Api::ChatCompletions, &image(&big));
        assert_eq!(
            large.len(),
            small.len(),
            "a 4 MiB image costs the same blocks as a tiny one"
        );
        assert_ne!(large, small, "different images differ");
    }

    #[test]
    fn cache_salt_separates_tenants() {
        let mut salted = chat(1);
        salted["cache_salt"] = json!("tenant-a");
        let base = keys(Api::ChatCompletions, &chat(1));
        assert!(
            keys(Api::ChatCompletions, &salted)
                .iter()
                .all(|key| !base.contains(key))
        );
    }

    #[test]
    fn anthropic_skips_thinking_and_the_billing_header() {
        let body = |billing: &str, thinking: &str| {
            json!({"system": [{"type": "text", "text": billing}, {"type": "text", "text": system()}],
                   "messages": [{"role": "user", "content": "hi ".repeat(100)},
                                {"role": "assistant", "content": [{"type": "thinking", "thinking": thinking},
                                                                  {"type": "text", "text": "ok"}]}]})
        };
        let one = keys(Api::Messages, &body("x-anthropic-billing-header: a=1", "x"));
        assert!(!one.is_empty());
        assert_eq!(one, keys(Api::Messages, &body("x-anthropic-billing-header: a=2", "y")));
    }

    #[test]
    fn completions_take_one_string_and_refuse_token_arrays() {
        let text = "Once upon a time ".repeat(40);
        assert!(!keys(Api::Completions, &json!({"prompt": text})).is_empty());
        assert_eq!(
            keys(Api::Completions, &json!({"prompt": [text]})),
            keys(Api::Completions, &json!({"prompt": text}))
        );
        assert!(keys(Api::Completions, &json!({"prompt": [1, 2, 3]})).is_empty());
    }

    #[test]
    fn responses_key_instructions_and_items_and_skip_reasoning() {
        let body = |reasoning: &str| {
            json!({"instructions": system(), "input": [
                {"role": "user", "content": "hello ".repeat(50)},
                {"type": "reasoning", "summary": [{"type": "summary_text", "text": reasoning}]},
                {"type": "function_call", "name": "f", "arguments": "{\"a\":1}", "call_id": "x"},
                {"type": "function_call_output", "call_id": "x", "output": "done"}
            ]})
        };
        assert!(!keys(Api::Responses, &body("a")).is_empty());
        assert_eq!(keys(Api::Responses, &body("a")), keys(Api::Responses, &body("b")));
    }

    fn with_part(part: &serde_json::Value) -> serde_json::Value {
        json!({"messages": [{"role": "system", "content": system()},
            {"role": "user", "content": [part, {"type": "text", "text": "describe it ".repeat(30)}]}]})
    }

    #[test]
    fn video_and_audio_parts_are_keyed() {
        let video = |url: &str| with_part(&json!({"type": "video_url", "video_url": {"url": url}}));
        assert_ne!(
            keys(Api::ChatCompletions, &video("https://a/1.mp4")),
            keys(Api::ChatCompletions, &video("https://a/2.mp4"))
        );
        let audio = |url: &str| with_part(&json!({"type": "audio_url", "audio_url": {"url": url}}));
        assert_ne!(
            keys(Api::ChatCompletions, &audio("https://a/1.wav")),
            keys(Api::ChatCompletions, &audio("https://a/2.wav"))
        );
    }

    #[test]
    fn a_media_uuid_names_the_item_whatever_its_url() {
        let image = |url: &str| with_part(&json!({"type": "image_url", "image_url": {"url": url}, "uuid": "img-1"}));
        assert_eq!(
            keys(Api::ChatCompletions, &image("https://bucket/x?sig=1")),
            keys(Api::ChatCompletions, &image("https://bucket/x?sig=2")),
            "a presigned URL that changes keeps its uuid"
        );
    }

    #[test]
    fn an_unknown_part_type_gives_no_key() {
        assert!(
            keys(
                Api::ChatCompletions,
                &with_part(&json!({"type": "hologram", "data": "x"}))
            )
            .is_empty()
        );
    }

    #[test]
    fn string_content_and_one_text_part_hash_alike() {
        let question = "what next ".repeat(40);
        let string = json!({"messages": [{"role": "user", "content": question}]});
        let parts = json!({"messages": [{"role": "user", "content": [{"type": "text", "text": question}]}]});
        assert_eq!(keys(Api::ChatCompletions, &string), keys(Api::ChatCompletions, &parts));
    }

    #[test]
    fn template_arguments_change_the_keys() {
        let mut thinking = chat(1);
        thinking["chat_template_kwargs"] = json!({"enable_thinking": true});
        assert_ne!(
            keys(Api::ChatCompletions, &thinking),
            keys(Api::ChatCompletions, &chat(1))
        );
    }

    #[test]
    fn a_continued_response_gets_no_key() {
        let body =
            json!({"instructions": system(), "input": "go on ".repeat(60), "previous_response_id": "resp_east.a1"});
        assert!(keys(Api::Responses, &body).is_empty());
    }

    /// Hashing cost per body size; run with `--ignored --nocapture` in release.
    #[test]
    #[ignore = "timing, not a check"]
    #[expect(clippy::print_stdout, reason = "reports the timing")]
    fn hashing_cost_by_body_size() {
        for kib in [2_usize, 32, 1024] {
            let turns = (kib * 1024 / 600).max(1);
            let body = serde_json::to_vec(&chat(turns)).unwrap();
            let mut samples: Vec<_> = std::iter::repeat_with(|| {
                let started = std::time::Instant::now();
                std::hint::black_box(prefix_keys(Api::ChatCompletions, &body));
                started.elapsed()
            })
            .take(200)
            .collect();
            samples.sort_unstable();
            println!(
                "body {} KiB: p50 {:?} p99 {:?}",
                body.len() / 1024,
                samples[samples.len() / 2],
                samples[samples.len() * 99 / 100]
            );
        }
    }

    #[test]
    fn unreadable_bodies_give_no_keys() {
        for body in [
            &b"not json"[..],
            b"{}",
            br#"{"messages": 7}"#,
            br#"{"messages": [{"role": "user", "content": 5}]}"#,
        ] {
            assert!(
                prefix_keys(Api::ChatCompletions, body).is_none(),
                "{:?}",
                String::from_utf8_lossy(body)
            );
        }
        assert!(
            keys(
                Api::ChatCompletions,
                &json!({"messages": [{"role": "user", "content": "short"}]})
            )
            .is_empty()
        );
    }
}
