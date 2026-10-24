//! Expected status against observed status, rendered as a diff a failing test prints.

use serde_json::Value;

use crate::{Failure, Observed};

/// One line of expected status: a condition type, or `phase` for the phase triple, with
/// its status and reason. For `phase`, the status is `status.phase` and the reason is
/// `status.reason`.
pub type Want<'want> = (&'want str, &'want str, &'want str);

/// A row of the diff: the type, what was expected, what was observed.
type Row = (String, Option<(String, String)>, Option<(String, String, String)>);

/// The status and reason of `type_` in `observed`, with its message.
fn held(observed: &Observed, type_: &str) -> Option<(String, String, String)> {
    let text = |value: &Value, key: &str| value.get(key).and_then(Value::as_str).unwrap_or_default().to_owned();
    if type_ == "phase" {
        let phase = observed.field("phase");
        return (!phase.is_empty()).then(|| {
            (
                phase.to_owned(),
                observed.field("reason").to_owned(),
                observed.field("message").to_owned(),
            )
        });
    }
    observed.condition(type_).map(|condition| {
        (
            text(condition, "status"),
            text(condition, "reason"),
            text(condition, "message"),
        )
    })
}

/// The condition types `observed` holds, in the order stored.
fn held_types(observed: &Observed) -> Vec<String> {
    observed
        .conditions
        .iter()
        .filter_map(|condition| condition.get("type").and_then(Value::as_str))
        .map(str::to_owned)
        .collect()
}

/// Whether a row's observed side matches its expected side.
fn row_matches(row: &Row) -> bool {
    match (&row.1, &row.2) {
        (Some((status, reason)), Some((got_status, got_reason, _))) => status == got_status && reason == got_reason,
        (None, None) => true,
        (Some(_), None) | (None, Some(_)) => false,
    }
}

/// `rows` as a table: `!` marks a row that differs, and the observed message follows.
fn render(title: &str, rows: &[Row]) -> String {
    let lines: Vec<String> = rows
        .iter()
        .map(|row| {
            let want = row.1.as_ref().map_or_else(
                || "(absent)".to_owned(),
                |(status, reason)| format!("{status} {reason}"),
            );
            let got = row.2.as_ref().map_or_else(
                || "(absent)".to_owned(),
                |(status, reason, message)| format!("{status} {reason}  {message:?}"),
            );
            let mark = if row_matches(row) { ' ' } else { '!' };
            format!("  {mark} {:<22} {want:<34} {got}", row.0)
        })
        .collect();
    format!(
        "{title}\n    {:<22} {:<34} observed\n{}\n",
        "type",
        "expected",
        lines.join("\n")
    )
}

/// Every condition `observed` holds, and the phase triple, as a table.
#[must_use]
pub fn table(observed: &Observed) -> String {
    let lines: Vec<String> = held_types(observed)
        .into_iter()
        .chain(std::iter::once("phase".to_owned()))
        .filter_map(|type_| {
            let (status, reason, message) = held(observed, &type_)?;
            Some(format!(
                "    {type_:<22} {:<34} {message:?}",
                format!("{status} {reason}")
            ))
        })
        .collect();
    format!(
        "status held:\n    {:<22} {:<34} message\n{}\n",
        "type",
        "status",
        lines.join("\n")
    )
}

/// The status `observed` should hold. Every type in `expected` must read its status and
/// reason, and any condition type `observed` holds beyond `expected` is a difference too:
/// the expectation is the whole status.
///
/// # Errors
///
/// Returns the diff, every type on a line, when anything differs.
pub fn status_matches(observed: &Observed, expected: &[Want<'_>]) -> Result<(), Failure> {
    let mut rows: Vec<Row> = expected
        .iter()
        .map(|(type_, status, reason)| {
            (
                (*type_).to_owned(),
                Some(((*status).to_owned(), (*reason).to_owned())),
                held(observed, type_),
            )
        })
        .collect();
    for type_ in held_types(observed) {
        if !expected.iter().any(|(want, ..)| *want == type_) {
            let got = held(observed, &type_);
            rows.push((type_, None, got));
        }
    }
    if rows.iter().all(row_matches) {
        return Ok(());
    }
    Err(Failure::from(render("status differs from expected:", &rows)))
}

/// What changed between two rounds, as a diff of every type either holds.
#[must_use]
pub fn between(earlier: &Observed, later: &Observed) -> String {
    let mut types = held_types(earlier);
    for type_ in held_types(later).into_iter().chain(std::iter::once("phase".to_owned())) {
        if !types.contains(&type_) {
            types.push(type_);
        }
    }
    let rows: Vec<Row> = types
        .into_iter()
        .filter_map(|type_| {
            let before = held(earlier, &type_);
            let after = held(later, &type_);
            if before.is_none() && after.is_none() {
                return None;
            }
            Some((type_, before.map(|(status, reason, _)| (status, reason)), after))
        })
        .collect();
    render("the round before against this round:", &rows)
}
