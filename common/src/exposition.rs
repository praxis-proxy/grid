//! Prometheus text exposition tokenizer, shared by producer and consumer.
//!
//! The operator produces exposition when it serves signals; the gateway filter
//! consumes it. Both need the same quote- and escape-aware label scanning, so it
//! lives in this plane-neutral crate that either plane may depend on. The gateway
//! filter consumes it here; the operator producer keeps its own copy for now. This
//! module tokenizes a line into a metric name, a lazily iterated label set, a
//! finite value, and an optional timestamp. It does not interpret the grid labels;
//! that is the caller's job.

use std::borrow::Cow;

/// Label name the operator stamps with the owning site.
pub const SITE_LABEL: &str = "grid_site";

/// Label name the operator stamps with the owning provider (cluster).
pub const PROVIDER_LABEL: &str = "grid_provider";

/// A parsed exposition line: a metric name, a validated label section, a finite
/// value, and an optional millisecond timestamp.
///
/// The label section is validated at parse time but iterated lazily, so a caller
/// that wants only a couple of labels never allocates a map.
#[derive(Debug)]
pub struct Metric<'text> {
    /// Metric name (the token before the opening brace).
    name: &'text str,
    /// Reported value; always finite.
    value: f64,
    /// Millisecond timestamp, when the line carried one.
    timestamp_ms: Option<i64>,
    /// Validated label section between the braces, for lazy iteration.
    labels: &'text str,
}

impl<'text> Metric<'text> {
    /// The metric name.
    #[must_use]
    pub fn name(&self) -> &'text str {
        self.name
    }

    /// The reported value, always finite.
    #[must_use]
    pub fn value(&self) -> f64 {
        self.value
    }

    /// The millisecond timestamp, when the line carried one.
    #[must_use]
    pub fn timestamp_ms(&self) -> Option<i64> {
        self.timestamp_ms
    }

    /// Iterate the label pairs left to right, quote-aware. Values borrow the
    /// input unless they carried an escape.
    #[must_use]
    pub fn labels(&self) -> Labels<'text> {
        Labels { rest: self.labels }
    }
}

/// Parse one `name{labels} value [timestamp]` line.
///
/// Returns `None` for a blank line, a comment, a name without a brace, a
/// malformed label section, a non-finite value, or trailing tokens after the
/// timestamp. The label section is validated quote- and escape-aware, so a
/// quoted comma or an escaped quote inside a value can never be read as a label
/// separator.
#[must_use]
pub fn parse(line: &str) -> Option<Metric<'_>> {
    let line = line.trim();
    if line.is_empty() || line.starts_with('#') {
        return None;
    }
    let (name, rest) = line.split_once('{')?;
    let name = name.trim_end();
    if name.is_empty() {
        return None;
    }
    let (labels, tail) = scan_labels(rest)?;
    let mut fields = tail.split_whitespace();
    let value = fields.next()?.parse::<f64>().ok()?;
    // A non-finite value would poison any comparison downstream, so reject it at
    // the boundary.
    if !value.is_finite() {
        return None;
    }
    let timestamp_ms = match fields.next() {
        Some(token) => Some(token.parse::<i64>().ok()?),
        None => None,
    };
    if fields.next().is_some() {
        return None;
    }
    Some(Metric {
        name,
        value,
        timestamp_ms,
        labels,
    })
}

/// Validate the label section beginning just after the opening brace and return
/// it together with the tail after the closing brace. Every value is consumed
/// quote-aware, so the closing brace is found even when a value holds a brace or
/// a quoted comma. A malformed section yields `None`.
fn scan_labels(after_brace: &str) -> Option<(&str, &str)> {
    let mut cursor = after_brace;
    loop {
        cursor = cursor.trim_start();
        if cursor.starts_with('}') {
            // `cursor` is always a suffix of `after_brace`, so this is the index
            // of the closing brace; the section is everything before it.
            let end = after_brace.len().checked_sub(cursor.len())?;
            let section = after_brace.get(..end)?;
            let tail = cursor.get(1..)?;
            return Some((section, tail));
        }
        let (_name, tail) = split_label_name(cursor)?;
        let tail = tail.trim_start().strip_prefix('=')?.trim_start();
        let (_value, tail) = parse_label_value(tail)?;
        cursor = tail.trim_start();
        cursor = cursor.strip_prefix(',').unwrap_or(cursor);
    }
}

/// Iterator over the label pairs of a validated label section.
///
/// The section was validated by [`parse`], so iteration does not surface errors:
/// it yields each `(name, value)` in order and ends at the section's end.
#[derive(Debug)]
pub struct Labels<'text> {
    /// Remaining unparsed label section.
    rest: &'text str,
}

impl<'text> Iterator for Labels<'text> {
    type Item = (&'text str, Cow<'text, str>);

    fn next(&mut self) -> Option<Self::Item> {
        let cursor = self.rest.trim_start();
        if cursor.is_empty() {
            self.rest = "";
            return None;
        }
        let (name, tail) = split_label_name(cursor)?;
        let tail = tail.trim_start().strip_prefix('=')?.trim_start();
        let (value, tail) = parse_label_value(tail)?;
        let tail = tail.trim_start();
        self.rest = tail.strip_prefix(',').unwrap_or(tail);
        Some((name, value))
    }
}

/// Split a leading label name (a run of ASCII alphanumerics or underscores) from
/// the rest. Yields `None` when no name is present.
fn split_label_name(text: &str) -> Option<(&str, &str)> {
    let end = text
        .find(|ch: char| !(ch.is_ascii_alphanumeric() || ch == '_'))
        .unwrap_or(text.len());
    (end > 0).then(|| text.split_at(end))
}

/// Parse one quoted label value, returning it and the tail after the closing
/// quote. The value borrows the input when it holds no escape, else it is
/// unescaped into an owned string. An unterminated quote yields `None`.
fn parse_label_value(text: &str) -> Option<(Cow<'_, str>, &str)> {
    let rest = text.strip_prefix('"')?;
    let mut chars = rest.char_indices();
    let mut escaped = false;
    let close = loop {
        let (idx, ch) = chars.next()?;
        match ch {
            '"' => break idx,
            // Consume the escaped char so an escaped quote does not end the value.
            '\\' => {
                escaped = true;
                chars.next();
            },
            _ => {},
        }
    };
    let (content, after) = rest.split_at(close);
    let tail = after.get(1..)?;
    let value = if escaped {
        Cow::Owned(unescape(content))
    } else {
        Cow::Borrowed(content)
    };
    Some((value, tail))
}

/// Apply the label-value escapes: `\n` is a newline, any other escaped char is
/// itself.
fn unescape(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(ch) = chars.next() {
        if ch == '\\' {
            match chars.next() {
                Some('n') => out.push('\n'),
                Some(other) => out.push(other),
                None => {},
            }
        } else {
            out.push(ch);
        }
    }
    out
}

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(clippy::expect_used, clippy::float_cmp, clippy::indexing_slicing, reason = "tests")]
mod tests {
    use super::*;

    #[test]
    fn parses_name_value_and_timestamp() {
        let metric = parse(r#"queue{grid_site="east"} 3.5 1000"#).expect("parses");
        assert_eq!(metric.name(), "queue", "name is the token before the brace");
        assert_eq!(metric.value(), 3.5, "value as reported");
        assert_eq!(metric.timestamp_ms(), Some(1000), "timestamp as reported");
    }

    #[test]
    fn a_missing_timestamp_is_none_not_an_error() {
        let metric = parse(r#"queue{grid_site="east"} 3.5"#).expect("parses without a timestamp");
        assert_eq!(metric.timestamp_ms(), None, "no timestamp present");
    }

    #[test]
    fn blank_and_comment_lines_do_not_parse() {
        assert!(parse("").is_none(), "blank line");
        assert!(parse("   ").is_none(), "whitespace line");
        assert!(parse("# HELP queue a help string").is_none(), "comment line");
    }

    #[test]
    fn a_line_without_a_brace_does_not_parse() {
        assert!(parse("queue 3 1000").is_none(), "no label section, no target labels");
    }

    #[test]
    fn a_non_finite_value_does_not_parse() {
        assert!(parse(r#"queue{a="b"} NaN 1000"#).is_none(), "NaN rejected");
        assert!(parse(r#"queue{a="b"} +Inf 1000"#).is_none(), "Inf rejected");
    }

    #[test]
    fn trailing_tokens_after_the_timestamp_do_not_parse() {
        assert!(
            parse(r#"queue{a="b"} 3 1000 extra"#).is_none(),
            "trailing token rejected"
        );
    }

    #[test]
    fn an_unterminated_quote_does_not_parse() {
        assert!(parse(r#"queue{a="b} 3 1000"#).is_none(), "unterminated value rejected");
    }

    #[test]
    fn labels_iterate_in_order_and_borrow() {
        let metric = parse(r#"queue{grid_site="east",grid_provider="pool-a"} 3 1000"#).expect("parses");
        let labels: Vec<(&str, Cow<'_, str>)> = metric.labels().collect();
        assert_eq!(labels.len(), 2, "two labels");
        assert_eq!(labels[0], ("grid_site", Cow::Borrowed("east")), "first label, borrowed");
        assert_eq!(
            labels[1],
            ("grid_provider", Cow::Borrowed("pool-a")),
            "second label, borrowed"
        );
    }

    #[test]
    fn a_quoted_comma_stays_inside_one_value() {
        let metric = parse(r#"queue{note="x,grid_provider=evil",grid_site="east"} 3 1000"#).expect("parses");
        let labels: Vec<(&str, Cow<'_, str>)> = metric.labels().collect();
        assert_eq!(
            labels.len(),
            2,
            "the quoted comma does not split the value into a forged label"
        );
        assert_eq!(
            labels[0].1.as_ref(),
            "x,grid_provider=evil",
            "the whole quoted value is one label"
        );
    }

    #[test]
    fn an_escaped_quote_stays_inside_one_value() {
        let metric = parse(r#"queue{note="a\",grid_site=evil",grid_provider="pool-a"} 3 1000"#).expect("parses");
        let labels: Vec<(&str, Cow<'_, str>)> = metric.labels().collect();
        assert_eq!(labels.len(), 2, "the escaped quote does not end the value early");
        assert_eq!(labels[0].0, "note", "the first label name is intact");
    }

    #[test]
    fn an_escaped_value_is_unescaped_and_owned() {
        let metric = parse(r#"queue{note="line\none"} 3 1000"#).expect("parses");
        let labels: Vec<(&str, Cow<'_, str>)> = metric.labels().collect();
        assert_eq!(labels[0].1.as_ref(), "line\none", "the escape becomes a newline");
        assert!(
            matches!(labels[0].1, Cow::Owned(_)),
            "an escaped value owns its unescaped string"
        );
    }
}
