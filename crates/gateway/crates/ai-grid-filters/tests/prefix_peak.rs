//! A request body never expands in memory much past its own size while the gateway keys it.

#[cfg(test)]
#[expect(clippy::allow_attributes, reason = "blanket test suppressions")]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::arithmetic_side_effects,
    clippy::too_many_lines,
    reason = "tests"
)]
mod tests {
    #[global_allocator]
    static ALLOC: dhat::Alloc = dhat::Alloc;

    /// Bodies near the 10 MiB the filter buffers.
    const BODY: usize = 10 << 20;

    /// Peak heap bytes while keying `body`, the body itself excluded.
    fn peak(path: &str, body: &[u8]) -> usize {
        let _profiler = dhat::Profiler::builder().testing().build();
        let keys = ai_grid_filters::prefix_key_count(path, body);
        // A token array has no prompt text, so it keys nothing; everything else keys.
        assert_eq!(keys > 0, path != "/v1/completions", "{path}: {keys} keys");
        dhat::HeapStats::get().max_bytes
    }

    /// `count` copies of `item` joined into a JSON array.
    fn array(item: &str, count: usize) -> String {
        format!("[{}]", vec![item; count].join(","))
    }

    #[test]
    fn keying_a_large_body_stays_under_four_mib() {
        let system = format!(
            r#"{{"role":"system","content":"{}"}}"#,
            "Answer in short paragraphs about routing, caching and load. ".repeat(40)
        );
        let tiny = r#"{"role":"user","content":"a"}"#;
        let tool = r#"{"type":"function","function":{"name":"f","parameters":{"type":"object","properties":{"a":{"type":"string"}}}}}"#;
        let bodies = [
            (
                "tools",
                "/v1/chat/completions",
                format!(
                    r#"{{"tools":{},"messages":[{system}]}}"#,
                    array(tool, BODY / tool.len())
                ),
            ),
            (
                "messages",
                "/v1/chat/completions",
                format!(r#"{{"messages":{}}}"#, array(tiny, BODY / tiny.len())),
            ),
            (
                "arguments",
                "/v1/chat/completions",
                format!(
                    r#"{{"messages":[{system},{{"role":"assistant","tool_calls":[{{"function":{{"name":"f","arguments":{}}}}}]}}]}}"#,
                    serde_json::to_string(&array(r#"{"k":[1,2,3]}"#, BODY / 14)).unwrap()
                ),
            ),
            (
                "documents",
                "/v1/chat/completions",
                format!(
                    r#"{{"documents":{},"messages":[{system}]}}"#,
                    array(r#"{"t":"x"}"#, BODY / 10)
                ),
            ),
            (
                "parts",
                "/v1/chat/completions",
                format!(
                    r#"{{"messages":[{system},{{"role":"user","content":{}}}]}}"#,
                    array(r#"{"type":"text","text":"a"}"#, BODY / 26)
                ),
            ),
            (
                "tokens",
                "/v1/completions",
                format!(r#"{{"prompt":{}}}"#, array("7", BODY / 2)),
            ),
            (
                "blocks",
                "/v1/messages",
                format!(
                    r#"{{"system":"{}","messages":[{{"role":"user","content":{}}}]}}"#,
                    "Answer briefly. ".repeat(100),
                    array(r#"{"type":"text","text":"a"}"#, BODY / 26)
                ),
            ),
            (
                "items",
                "/v1/responses",
                format!(
                    r#"{{"instructions":"{}","input":{}}}"#,
                    "Answer briefly. ".repeat(100),
                    array(r#"{"role":"user","content":"a"}"#, BODY / 29)
                ),
            ),
            (
                "nested_arguments",
                "/v1/chat/completions",
                format!(
                    r#"{{"messages":[{system},{}]}}"#,
                    vec![
                        format!(
                            r#"{{"role":"assistant","tool_calls":[{{"function":{{"name":"f","arguments":{}}}}}]}}"#,
                            serde_json::to_string(&array("[[[[1]]]]", 6_000)).unwrap()
                        );
                        BODY / 66_000
                    ]
                    .join(",")
                ),
            ),
            (
                "nested_under_limit",
                "/v1/chat/completions",
                format!(
                    r#"{{"messages":[{system},{}]}}"#,
                    vec![
                        format!(
                            r#"{{"role":"assistant","tool_calls":[{{"function":{{"name":"f","arguments":{}}}}}]}}"#,
                            serde_json::to_string(&array("[[[[1]]]]", 1_600)).unwrap()
                        );
                        BODY / 16_500
                    ]
                    .join(",")
                ),
            ),
        ];
        for (name, path, body) in bodies {
            let used = peak(path, body.as_bytes());
            assert!(
                used < 4 << 20,
                "{name}: {used} bytes at peak for a {} byte body",
                body.len()
            );
        }
    }
}
