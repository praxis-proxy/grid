//! Rewrites a query so one query set works against a central store that tags
//! every series with the cluster it came from.

use promql_parser::{
    label::{MatchOp, Matcher},
    parser::{Expr, ParenExpr, SubqueryExpr, UnaryExpr, VectorSelector, parse},
};

use super::MetricsError;

/// Adds `label="value"` to every vector selector in `promql`.
///
/// # Errors
///
/// [`MetricsError::InvalidPromql`] when `promql` does not parse.
pub fn inject_matcher(promql: &str, label: &str, value: &str) -> Result<String, MetricsError> {
    let mut expr = parse(promql).map_err(|reason| MetricsError::InvalidPromql {
        promql: promql.to_owned(),
        reason,
    })?;
    scope(&mut expr, &Matcher::new(MatchOp::Equal, label, value));
    Ok(expr.to_string())
}

/// Visits every sub-expression, prepending `matcher` to each selector so it
/// takes precedence over any matcher the query already carries on that label.
fn scope(expr: &mut Expr, matcher: &Matcher) {
    match expr {
        Expr::VectorSelector(selector) => prepend(selector, matcher),
        Expr::MatrixSelector(selector) => prepend(&mut selector.vs, matcher),
        Expr::Aggregate(aggregate) => {
            scope(&mut aggregate.expr, matcher);
            if let Some(param) = &mut aggregate.param {
                scope(param, matcher);
            }
        },
        Expr::Binary(binary) => {
            scope(&mut binary.lhs, matcher);
            scope(&mut binary.rhs, matcher);
        },
        Expr::Unary(UnaryExpr { expr })
        | Expr::Paren(ParenExpr { expr })
        | Expr::Subquery(SubqueryExpr { expr, .. }) => scope(expr, matcher),
        Expr::Call(call) => {
            for arg in &mut call.args.args {
                scope(arg, matcher);
            }
        },
        Expr::NumberLiteral(_) | Expr::StringLiteral(_) | Expr::Extension(_) => {},
    }
}

/// Puts `matcher` first in `selector`'s matcher list, and in every `or`
/// alternative, since a selector with alternatives is printed from those
/// alone and the plain list would be dropped.
fn prepend(selector: &mut VectorSelector, matcher: &Matcher) {
    selector.matchers.matchers.insert(0, matcher.clone());
    for group in &mut selector.matchers.or_matchers {
        group.insert(0, matcher.clone());
    }
}

#[cfg(test)]
#[expect(clippy::unwrap_used, reason = "tests")]
mod tests {
    use super::inject_matcher;

    #[test]
    fn a_bare_selector_gains_the_matcher() {
        assert_eq!(
            inject_matcher("up", "cluster", "spoke1").unwrap(),
            r#"up{cluster="spoke1"}"#
        );
    }

    #[test]
    fn the_matcher_is_prepended_to_existing_matchers() {
        let got = inject_matcher(
            r#"sum(rate(vllm:request_success_total{job="vllm"}[1m]))"#,
            "cluster",
            "spoke1",
        )
        .unwrap();
        assert_eq!(
            got,
            r#"sum(rate(vllm:request_success_total{cluster="spoke1",job="vllm"}[1m]))"#
        );
    }

    #[test]
    fn both_sides_of_a_binary_expression_are_scoped() {
        let got = inject_matcher("a - b", "cluster", "spoke1").unwrap();
        assert_eq!(got, r#"a{cluster="spoke1"} - b{cluster="spoke1"}"#);
    }

    #[test]
    fn selectors_nested_in_calls_and_aggregations_are_scoped() {
        let got = inject_matcher(
            "histogram_quantile(0.5, sum by (le) (rate(x_bucket[5m]))) * 1000",
            "cluster",
            "spoke1",
        )
        .unwrap();
        assert_eq!(
            got,
            r#"histogram_quantile(0.5, sum by (le) (rate(x_bucket{cluster="spoke1"}[5m]))) * 1000"#
        );
    }

    #[test]
    fn every_or_matcher_group_is_scoped() {
        let got = inject_matcher(r#"up{a="x" or b="y"}"#, "cluster", "spoke1").unwrap();
        assert_eq!(
            got.matches(r#"cluster="spoke1""#).count(),
            2,
            "each alternative must carry the site scope: {got}"
        );
    }

    #[test]
    fn invalid_promql_is_an_error() {
        assert!(
            inject_matcher("sum(", "cluster", "spoke1").is_err(),
            "unbalanced parenthesis must not parse"
        );
    }
}
