//! grok-proxy-cc — Anthropic Messages API proxy for Grok (xAI) models.
//!
//! Single-file, hyper-direct, tokio current_thread. Born at maximum
//! optimization: no axum, no tracing, no uuid, no chrono, no anyhow.
//!
//! Auth: reuses the Grok CLI session at ~/.grok/auth.json (OIDC JWT).
//! Upstream: https://cli-chat-proxy.grok.com/v1/chat/completions
//! Loopback-only. Fail-closed everywhere.

use std::sync::Arc;

use http_body_util::combinators::UnsyncBoxBody;
use http_body_util::{BodyExt, Full, StreamBody};
use hyper::body::Bytes;
use hyper::body::Frame;
use hyper::body::Incoming;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use serde_json::{json, Value};
use tokio::sync::mpsc;

/// The body type for all responses.
type BoxBody = UnsyncBoxBody<Bytes, std::io::Error>;

/// Helper: Full body with error type coerced to io::Error (Full is Infallible).
fn full_body(data: impl Into<Bytes>) -> BoxBody {
    use http_body_util::BodyExt as _;
    UnsyncBoxBody::new(Full::new(data.into()).map_err(|e| match e {}))
}

// ─── Constants ──────────────────────────────────────────────────────────

const UPSTREAM_BASE: &str = "https://cli-chat-proxy.grok.com/v1";
const CLI_VERSION: &str = "1.0.52";

/// Static model table: (public id suffix, display) — verified against upstream /v1/models 2026-10-16.
const MODELS: &[(&str, &str)] = &[
    ("grok-4.7", "Grok 4.7"),
    ("grok-4.7-build-fast", "Grok 4.7 Build Fast"),
    ("grok-4.6", "Grok 4.6"),
    ("grok-4.5", "Grok 4.5"),
];

/// Same-provider fallback chain.
const CHAIN_DEFAULT: &[&str] = &["grok-4.7", "grok-4.7-build-fast", "grok-4.6", "grok-4.5"];

const BODY_CAP: usize = 32 * 1024 * 1024; // 32MB
const PING_INTERVAL: std::time::Duration = std::time::Duration::from_secs(5);
const MAX_STREAM_IDLE: std::time::Duration = std::time::Duration::from_secs(300);

// ─── Auth: read ~/.grok/auth.json ────────────────────────────────────────

fn grok_home() -> std::path::PathBuf {
    std::env::var_os("GROK_HOME")
        .map(std::path::PathBuf::from)
        .unwrap_or_else(|| {
            std::env::var_os("HOME")
                .map(std::path::PathBuf::from)
                .unwrap_or_default()
                .join(".grok")
        })
}

/// Extract the OIDC access token (field `key`) from ~/.grok/auth.json.
/// The file is a MAP keyed by "<issuer>::<client_id>" — take the first entry.
fn load_grok_token() -> Result<(String, String), String> {
    let path = grok_home().join("auth.json");
    let raw = std::fs::read_to_string(&path).map_err(|e| {
        format!(
            "{} not readable: {e} (run `grok login` first)",
            path.display()
        )
    })?;
    let v: Value =
        serde_json::from_str(&raw).map_err(|e| format!("parse {}: {e}", path.display()))?;
    let Some(map) = v.as_object() else {
        return Err(format!("{}: expected a JSON object", path.display()));
    };
    for (_k, entry) in map {
        let key = entry
            .get("key")
            .and_then(Value::as_str)
            .unwrap_or_default()
            .to_string();
        if key.is_empty() {
            continue;
        }
        let expires = entry
            .get("expires_at")
            .and_then(Value::as_str)
            .unwrap_or("")
            .to_string();
        return Ok((key, expires));
    }
    Err(format!(
        "{}: no entry with a non-empty `key`",
        path.display()
    ))
}

// ─── Config ────────────────────────────────────────────────────────────────

struct Cfg {
    client: reqwest::Client,
    /// OIDC token, re-readable from auth.json on 401 (RwLock: reads on the
    /// hot path, one write per refresh — the grok CLI refreshes the file
    /// in place; the proxy picks the new token up without a restart).
    token: std::sync::RwLock<String>,
    /// Path to auth.json (re-read on upstream 401).
    auth_path: std::path::PathBuf,
    master_key: String,
    addr: std::net::SocketAddr,
}

impl Cfg {
    fn token_now(&self) -> String {
        self.token.read().unwrap().clone()
    }

    /// Re-read ~/.grok/auth.json and swap the token. Returns true if the
    /// file yielded a different, non-empty token.
    fn reload_token(&self) -> bool {
        match std::fs::read_to_string(&self.auth_path) {
            Ok(raw) => {
                if let Ok(v) = serde_json::from_str::<Value>(&raw) {
                    if let Some(map) = v.as_object() {
                        for (_k, entry) in map {
                            let key = entry.get("key").and_then(Value::as_str).unwrap_or_default();
                            if !key.is_empty() {
                                let mut lock = self.token.write().unwrap();
                                if *lock != key {
                                    eprintln!("[grok-proxy] token reloaded from auth.json");
                                    *lock = key.to_string();
                                    return true;
                                }
                                return false;
                            }
                        }
                    }
                }
                false
            }
            Err(_) => false,
        }
    }

    /// Identity headers the cli-chat-proxy gates on (confirmed by live gate test).
    fn upstream_headers(&self) -> reqwest::header::HeaderMap {
        let mut h = reqwest::header::HeaderMap::new();
        let bearer = format!("Bearer {}", self.token_now());
        if let Ok(v) = reqwest::header::HeaderValue::from_str(&bearer) {
            h.insert(reqwest::header::AUTHORIZATION, v);
        }
        let static_headers = [
            ("x-xai-token-auth", "xai-grok-cli"),
            ("x-grok-client-identifier", "grok-shell"),
            ("x-grok-client-version", CLI_VERSION),
        ];
        for (name, val) in static_headers {
            if let (Ok(n), Ok(v)) = (
                reqwest::header::HeaderName::try_from(name),
                reqwest::header::HeaderValue::from_str(val),
            ) {
                h.insert(n, v);
            }
        }
        h
    }
}

// ─── Model resolution ──────────────────────────────────────────────────────

/// `claude-grok-4.7` → `grok-4.7`. Also accepts bare `grok-4.7`.
fn resolve_slug(model_id: &str) -> String {
    model_id
        .strip_prefix("claude-")
        .unwrap_or(model_id)
        .to_string()
}

/// `grok-4.7:high` → (`grok-4.7`, Some("high")). The suffix only splits when
/// it is a valid effort (low|medium|high); otherwise it stays part of the
/// slug (and fails model validation, as it should).
fn split_effort(slug: &str) -> (&str, Option<&str>) {
    match slug.rsplit_once(':') {
        Some((base, effort)) if matches!(effort, "low" | "medium" | "high") => (base, Some(effort)),
        _ => (slug, None),
    }
}

fn is_valid_slug(slug: &str) -> bool {
    MODELS.iter().any(|(s, _)| *s == slug)
}

// ─── Quota detection ───────────────────────────────────────────────────────

fn is_quota_exhaustion(body: &str) -> bool {
    body.contains("usage limit") || body.contains("spending limit") || body.contains("quota")
}

// ─── Conversion: Anthropic Messages → OpenAI Chat Completions ─────────────

/// Anthropic tool_result content → string (string content, or the text
/// blocks inside array content, joined).
fn tool_result_text(block: &Value) -> String {
    match block.get("content") {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(arr)) => arr
            .iter()
            .filter_map(|b| b.get("text").and_then(Value::as_str))
            .collect::<Vec<_>>()
            .join(""),
        _ => String::new(),
    }
}

fn convert_to_chat(req: &Value) -> Value {
    let mut out = json!({"stream": true});
    let obj = out.as_object_mut().unwrap();

    // Model (`claude-grok-4.7:high` → slug `grok-4.7` + effort `high`)
    let raw_slug = resolve_slug(
        req.get("model")
            .and_then(Value::as_str)
            .unwrap_or("grok-4.7"),
    );
    let (slug, suffix_effort) = split_effort(&raw_slug);
    obj.insert("model".into(), json!(slug));

    // System → first message (string, or array of text blocks —
    // modern Claude Code sends the array form with cache_control)
    if let Some(sys) = req.get("system") {
        let sys_text: Option<String> = match sys {
            Value::String(s) => Some(s.clone()),
            Value::Array(arr) => {
                let parts: Vec<&str> = arr
                    .iter()
                    .filter_map(|b| b.get("text").and_then(Value::as_str))
                    .collect();
                (!parts.is_empty()).then(|| parts.join("\n"))
            }
            _ => None,
        };
        if let Some(s) = sys_text {
            obj.insert("messages".into(), json!([{"role":"system","content":s}]));
        }
    }

    // Messages
    let mut msgs: Vec<Value> = Vec::new();
    if let Some(arr) = req.get("messages").and_then(Value::as_array) {
        for m in arr {
            let role = m.get("role").and_then(Value::as_str).unwrap_or("user");
            match role {
                "user" => {
                    // Claude Code sends tool_result blocks inside role:"user".
                    // Each becomes role:tool; sibling text blocks must NOT be
                    // dropped — they become a trailing user message.
                    let content = m.get("content");
                    let has_tool_results = content.and_then(Value::as_array).is_some_and(|arr| {
                        arr.iter()
                            .any(|b| b.get("type").and_then(Value::as_str) == Some("tool_result"))
                    });
                    if has_tool_results {
                        if let Some(arr) = content.and_then(Value::as_array) {
                            let mut text_parts: Vec<String> = Vec::new();
                            for b in arr {
                                match b.get("type").and_then(Value::as_str) {
                                    Some("tool_result") => {
                                        let tool_call_id = b
                                            .get("tool_use_id")
                                            .and_then(Value::as_str)
                                            .unwrap_or("");
                                        msgs.push(json!({
                                            "role":"tool",
                                            "tool_call_id":tool_call_id,
                                            "content":tool_result_text(b)
                                        }));
                                    }
                                    Some("text") => {
                                        text_parts.push(
                                            b.get("text")
                                                .and_then(Value::as_str)
                                                .unwrap_or("")
                                                .to_string(),
                                        );
                                    }
                                    Some("tool_reference") => {
                                        // Claude Code 2.1.300+ sends tool_reference
                                        // blocks; convert to text so the model
                                        // sees the context (grok API has no
                                        // tool reference concept).
                                        let name =
                                            b.get("name").and_then(Value::as_str).unwrap_or("");
                                        let rid = b.get("id").and_then(Value::as_str).unwrap_or("");
                                        let text = if rid.is_empty() {
                                            format!("[tool_reference: {name}]")
                                        } else {
                                            format!("[tool_reference: {name} ({rid})]")
                                        };
                                        text_parts.push(text);
                                    }
                                    _ => {}
                                }
                            }
                            if !text_parts.is_empty() {
                                msgs.push(json!({"role":"user","content":text_parts.join("\n")}));
                            }
                        }
                        continue;
                    }
                    let content = convert_content_to_text(m.get("content"));
                    msgs.push(json!({"role":"user","content":content}));
                }
                "assistant" => {
                    let mut a = json!({"role":"assistant"});
                    if let Some(obj_a) = a.as_object_mut() {
                        let content = m.get("content");
                        if let Some(arr_c) = content.and_then(Value::as_array) {
                            let mut text_parts: Vec<String> = Vec::new();
                            let mut thinking_parts: Vec<String> = Vec::new();
                            let mut tool_calls = Vec::new();
                            for block in arr_c {
                                match block.get("type").and_then(Value::as_str) {
                                    Some("text") => {
                                        text_parts.push(
                                            block.get("text").and_then(Value::as_str).unwrap_or("").to_string(),
                                        );
                                    }
                                    Some("thinking") => {
                                        // Preserve previous reasoning in history —
                                        // dropping this causes the model to
                                        // contradict its earlier decisions.
                                        if let Some(t) = block.get("thinking").and_then(Value::as_str) {
                                            thinking_parts.push(t.to_string());
                                        }
                                    }
                                    Some("redacted_thinking") => {
                                        thinking_parts.push("[redacted thinking]".to_string());
                                    }
                                    Some("tool_use") => {
                                        let id =
                                            block.get("id").and_then(Value::as_str).unwrap_or("");
                                        let name =
                                            block.get("name").and_then(Value::as_str).unwrap_or("");
                                        tool_calls.push(json!({
                                            "id": id, "type": "function",
                                            "function": {"name": name, "arguments": block.get("input").map(|v| v.to_string()).unwrap_or_default()}
                                        }));
                                    }
                                    _ => {}
                                }
                            }
                            // Reasoning first, then content — mirrors how the
                            // model actually produced the response.
                            let mut full_content = String::new();
                            if !thinking_parts.is_empty() {
                                full_content.push_str("<thinking>\n");
                                full_content.push_str(&thinking_parts.join("\n"));
                                full_content.push_str("\n</thinking>\n");
                            }
                            if !text_parts.is_empty() {
                                full_content.push_str(&text_parts.join(""));
                            }
                            if !full_content.is_empty() {
                                obj_a.insert("content".into(), json!(full_content));
                            }
                            if !tool_calls.is_empty() {
                                obj_a.insert("tool_calls".into(), json!(tool_calls));
                            }
                        } else if let Some(s) = content.and_then(Value::as_str) {
                            obj_a.insert("content".into(), json!(s));
                        }
                    }
                    msgs.push(a);
                }
                "tool" | "tool_result" => {
                    // tool_result from Anthropic becomes role:tool
                    let tool_call_id = m.get("tool_use_id").and_then(Value::as_str).unwrap_or("");
                    msgs.push(
                        json!({"role":"tool","tool_call_id":tool_call_id,"content":tool_result_text(m)}),
                    );
                }
                _ => {}
            }
        }
    }

    // Prepend system if present
    if let Some(sys) = obj.remove("messages") {
        if let Some(sys_arr) = sys.as_array() {
            for s in sys_arr {
                msgs.insert(0, s.clone());
            }
        }
    }
    obj.insert("messages".into(), json!(msgs));

    // max_tokens
    if let Some(mt) = req.get("max_tokens").and_then(Value::as_u64) {
        obj.insert("max_tokens".into(), json!(mt.clamp(1, 65536)));
    }

    // Tools
    if let Some(tools) = req.get("tools").and_then(Value::as_array) {
        let converted: Vec<Value> = tools
            .iter()
            .filter_map(|t| {
                let name = t.get("name").and_then(Value::as_str)?;
                Some(json!({"type":"function","function":{"name":name,"description":t.get("description").and_then(Value::as_str).unwrap_or(""),"parameters":t.get("input_schema").cloned().unwrap_or(json!({}))}}))
            })
            .collect();
        if !converted.is_empty() {
            obj.insert("tools".into(), json!(converted));
            // tool_choice: auto | any→required | tool→forced function | none
            let choice = match req.pointer("/tool_choice/type").and_then(Value::as_str) {
                Some("any") => json!("required"),
                Some("none") => json!("none"),
                Some("tool") => {
                    let name = req
                        .pointer("/tool_choice/name")
                        .and_then(Value::as_str)
                        .unwrap_or("");
                    json!({"type":"function","function":{"name":name}})
                }
                _ => json!("auto"),
            };
            obj.insert("tool_choice".into(), choice);
        }
    }

    // Effort precedence: output_config.effort (native slider) → `:effort`
    // model suffix → thinking.budget_tokens coarse mapping.
    // The Grok API accepts reasoning_effort: low|medium|high
    if let Some(effort) = req
        .get("output_config")
        .and_then(|oc| oc.get("effort"))
        .and_then(Value::as_str)
        .filter(|e| matches!(*e, "low" | "medium" | "high"))
        .or(suffix_effort)
        .or_else(|| {
            // Fallback: thinking.budget_tokens mapped coarsely
            req.get("thinking")
                .and_then(|t| t.get("budget_tokens"))
                .and_then(Value::as_i64)
                .map(|b| {
                    if b < 4096 {
                        "low"
                    } else if b < 16384 {
                        "medium"
                    } else {
                        "high"
                    }
                })
        })
    {
        obj.insert("reasoning_effort".into(), json!(effort));
    }

    // stream_options for usage in final chunk
    if req.get("stream").and_then(Value::as_bool) != Some(false) {
        obj.insert("stream_options".into(), json!({"include_usage": true}));
        obj.insert("stream".into(), json!(true));
    } else {
        obj.insert("stream".into(), json!(false));
    }

    // stop_sequences → stop
    if let Some(ss) = req.get("stop_sequences").and_then(Value::as_array) {
        if !ss.is_empty() {
            obj.insert("stop".into(), json!(ss));
        }
    }

    out
}

fn convert_content_to_text(content: Option<&Value>) -> String {
    match content {
        Some(Value::String(s)) => s.clone(),
        Some(Value::Array(arr)) => arr
            .iter()
            .filter_map(|b| {
                match b.get("type").and_then(Value::as_str)? {
                    "text" => b.get("text").and_then(Value::as_str).map(String::from),
                    "tool_result" => {
                        // Inline tool results
                        let inner = b.get("content").and_then(Value::as_str).unwrap_or("");
                        Some(format!("[tool_result: {inner}]"))
                    }
                    "tool_reference" => {
                        // Claude Code 2.1.300+ tool references
                        let name = b.get("name").and_then(Value::as_str).unwrap_or("");
                        let rid = b.get("id").and_then(Value::as_str).unwrap_or("");
                        Some(if rid.is_empty() {
                            format!("[tool_reference: {name}]")
                        } else {
                            format!("[tool_reference: {name} ({rid})]")
                        })
                    }
                    "image" => Some("[image]".into()),
                    _ => None,
                }
            })
            .collect::<Vec<_>>()
            .join("\n"),
        _ => String::new(),
    }
}

// ─── SSE building ──────────────────────────────────────────────────────────

fn sse_event(event: &str, data: &Value) -> String {
    format!("event: {event}\ndata: {data}\n\n")
}

fn anthropic_error(kind: &str, message: &str) -> String {
    sse_event(
        "error",
        &json!({"type":"error","error":{"type":kind,"message":message}}),
    )
}

fn message_start(model: &str) -> String {
    sse_event(
        "message_start",
        &json!({
            "type":"message_start",
            "message":{"id":format!("msg_{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos()),"type":"message","role":"assistant","model":model,"content":[],"stop_reason":null,"stop_sequence":null,"usage":{"input_tokens":0,"output_tokens":0}}
        }),
    )
}

fn content_block_start(index: usize, block_type: &str) -> String {
    // NOTE: tool_use blocks go through content_block_start_tool —
    // they must carry id + name in the start event.
    let extra = if block_type == "thinking" {
        json!({"thinking":""})
    } else {
        json!({"text":""})
    };
    let mut block = json!({"type":block_type});
    if let Some(o) = block.as_object_mut() {
        for (k, v) in extra.as_object().unwrap() {
            o.insert(k.clone(), v.clone());
        }
    }
    sse_event(
        "content_block_start",
        &json!({"type":"content_block_start","index":index,"content_block":block}),
    )
}

/// tool_use blocks carry the tool id and name in content_block_start —
/// the client matches tool_result by id and dispatches by name.
fn content_block_start_tool(index: usize, id: &str, name: &str) -> String {
    sse_event(
        "content_block_start",
        &json!({"type":"content_block_start","index":index,"content_block":{"type":"tool_use","id":id,"name":name,"input":{}}}),
    )
}

fn content_block_delta_text(index: usize, text: &str) -> String {
    sse_event(
        "content_block_delta",
        &json!({"type":"content_block_delta","index":index,"delta":{"type":"text_delta","text":text}}),
    )
}

fn content_block_delta_thinking(index: usize, text: &str) -> String {
    sse_event(
        "content_block_delta",
        &json!({"type":"content_block_delta","index":index,"delta":{"type":"thinking_delta","thinking":text}}),
    )
}

fn content_block_delta_tool_json(index: usize, partial_json: &str) -> String {
    sse_event(
        "content_block_delta",
        &json!({"type":"content_block_delta","index":index,"delta":{"type":"input_json_delta","partial_json":partial_json}}),
    )
}

fn content_block_stop(index: usize) -> String {
    sse_event(
        "content_block_stop",
        &json!({"type":"content_block_stop","index":index}),
    )
}

fn message_delta(stop_reason: &str, input_tokens: u64, output_tokens: u64) -> String {
    sse_event(
        "message_delta",
        &json!({"type":"message_delta","delta":{"stop_reason":stop_reason,"stop_sequence":null},"usage":{"input_tokens":input_tokens,"output_tokens":output_tokens}}),
    )
}

fn message_stop() -> String {
    sse_event("message_stop", &json!({"type":"message_stop"}))
}

fn ping_event() -> String {
    sse_event("ping", &json!({"type":"ping"}))
}

// ─── take_line: UTF-8 safe line extraction from SSE buffer ────────────────

fn take_line(buf: &mut Vec<u8>) -> Option<String> {
    loop {
        let pos = buf.iter().position(|&b| b == b'\n')?;
        let line_bytes = buf.drain(..pos + 1).collect::<Vec<u8>>();
        let line_str = String::from_utf8_lossy(&line_bytes[..pos])
            .trim_end()
            .to_string();
        if line_str.is_empty() {
            continue;
        }
        return Some(line_str);
    }
}

// ─── Token estimation (same as claude-nim) ────────────────────────────────

fn estimate_tokens(s: &str) -> u64 {
    let ascii = s.chars().filter(|c| c.is_ascii()).count() as u64;
    let non_ascii = (s.chars().count() as u64).saturating_sub(ascii);
    (ascii / 4 + non_ascii / 2).max(1)
}

// ─── Constant-time auth check ─────────────────────────────────────────────

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

// ─── parse_addr: loopback-only ────────────────────────────────────────────

fn parse_addr() -> std::net::SocketAddr {
    let raw = std::env::var("GROK_PROXY_ADDR").unwrap_or_else(|_| "127.0.0.1:4112".into());
    raw.parse()
        .unwrap_or_else(|_| "127.0.0.1:4112".parse().unwrap())
}

// ─── Upstream call with chain ──────────────────────────────────────────────

/// Build the upstream request. reqwest's total timeout covers the whole
/// body read — a hard 300s cap would kill healthy long thinking generations
/// mid-stream. Streams get a generous 1h ceiling (the pump's MAX_STREAM_IDLE
/// watchdog handles dead streams); non-streaming JSON keeps the hard 300s.
fn build_upstream(cfg: &Cfg, url: &str, body: &Value) -> reqwest::RequestBuilder {
    let streaming = body.get("stream").and_then(Value::as_bool).unwrap_or(true);
    let rb = cfg
        .client
        .post(url)
        .headers(cfg.upstream_headers())
        .json(body);
    if streaming {
        rb.timeout(std::time::Duration::from_secs(3600))
    } else {
        rb.timeout(std::time::Duration::from_secs(300))
    }
}

async fn call_grok(cfg: &Cfg, body: &Value) -> Result<reqwest::Response, String> {
    let url = format!("{UPSTREAM_BASE}/chat/completions");

    // First attempt with current token
    let resp = build_upstream(cfg, &url, body)
        .send()
        .await
        .map_err(|e| format!("upstream: {e}"))?;

    // 401: the OIDC token expired (6h lifetime) — re-read auth.json
    // (the grok CLI refreshes it in place) and retry ONCE.
    if resp.status().as_u16() == 401 {
        eprintln!("[grok-proxy] upstream 401; re-reading auth.json");
        if cfg.reload_token() {
            let resp = build_upstream(cfg, &url, body)
                .send()
                .await
                .map_err(|e| format!("upstream retry: {e}"))?;
            return Ok(resp);
        }
        eprintln!("[grok-proxy] auth.json unchanged — token may be stale; run `grok login`");
    }

    Ok(resp)
}

/// Try the selected slug, then walk the chain on 429/5xx (non-quota).
async fn call_with_chain(cfg: &Cfg, chat_req: &Value) -> Result<reqwest::Response, String> {
    let selected = chat_req
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("grok-4.7");
    let mut last_err;

    let chain: Vec<String> = {
        let env_chain = std::env::var("GROK_CHAIN").unwrap_or_default();
        if env_chain.is_empty() {
            CHAIN_DEFAULT.iter().map(|s| s.to_string()).collect()
        } else {
            env_chain
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(String::from)
                .collect()
        }
    };

    let start = chain
        .iter()
        .position(|s| s == selected)
        .map(|i| i + 1)
        .unwrap_or(0);

    // First attempt: selected model
    let mut req_body = chat_req.clone();
    match call_grok(cfg, &req_body).await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            if status < 400 {
                return Ok(resp);
            }
            let body_text = resp.text().await.unwrap_or_default();
            if is_quota_exhaustion(&body_text) || status == 401 || status == 403 {
                return Err(format!("__TERMINAL__:{status}:{body_text}"));
            }
            last_err = format!("upstream {status}: {body_text}");
        }
        Err(e) => {
            return Err(e);
        }
    }

    // Chain walk
    for slug in &chain[start..] {
        eprintln!("[grok-proxy] chain fallback: {selected} → {slug}");
        req_body["model"] = json!(slug);
        match call_grok(cfg, &req_body).await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                if status < 400 {
                    return Ok(resp);
                }
                let body_text = resp.text().await.unwrap_or_default();
                // Auth failures are model-independent: stop walking.
                if is_quota_exhaustion(&body_text) || status == 401 || status == 403 {
                    return Err(format!("__TERMINAL__:{status}:{body_text}"));
                }
                last_err = format!("upstream {status}: {body_text}");
            }
            Err(e) => last_err = e,
        }
    }
    Err(last_err)
}

// ─── Main handler: POST /v1/messages ────────────────────────────────────────

/// `__TERMINAL__:401:body` → (401, "body") — the internal contract between
/// call_with_chain (fail-closed stop) and handle_messages (error mapping).
/// The body may itself contain ':' — only the first two separators split.
fn parse_terminal(err: &str) -> Option<(u16, &str)> {
    let rest = err.strip_prefix("__TERMINAL__:")?;
    let (status_raw, body) = rest.split_once(':').unwrap_or((rest, ""));
    let status = status_raw.parse::<u16>().ok()?;
    Some((status, body))
}

async fn handle_messages(cfg: Arc<Cfg>, body: Value) -> Response<BoxBody> {
    let model = body
        .get("model")
        .and_then(Value::as_str)
        .unwrap_or("claude-grok-4.7")
        .to_string();
    let raw_slug = resolve_slug(&model);
    let (slug, _suffix_effort) = split_effort(&raw_slug);

    if !is_valid_slug(slug) {
        let err = json!({"type":"error","error":{"type":"not_found_error","message":format!("unsupported model id: {model}")}});
        return Response::builder()
            .status(StatusCode::NOT_FOUND)
            .header("content-type", "application/json")
            .body(full_body(Bytes::from(err.to_string())))
            .unwrap();
    }

    let chat_req = convert_to_chat(&body);
    let is_stream = chat_req
        .get("stream")
        .and_then(Value::as_bool)
        .unwrap_or(true);

    // Call upstream with chain
    let resp = match call_with_chain(&cfg, &chat_req).await {
        Ok(r) => r,
        Err(e) => {
            // Terminal upstream errors (quota, auth): fail-closed — no chain
            // walk happened, no silent fallback.
            if let Some((status, upstream_body)) = parse_terminal(&e) {
                let (kind, retry_after) = match status {
                    401 | 403 => ("authentication_error", None),
                    _ => ("rate_limit_error", Some("3600")),
                };
                let err = json!({"type":"error","error":{"type":kind,"message":upstream_body}});
                let mut rb = Response::builder()
                    .status(StatusCode::from_u16(status).unwrap_or(StatusCode::TOO_MANY_REQUESTS))
                    .header("content-type", "application/json");
                if let Some(ra) = retry_after {
                    rb = rb.header("retry-after", ra);
                }
                return rb.body(full_body(Bytes::from(err.to_string()))).unwrap();
            }
            let err = json!({"type":"error","error":{"type":"api_error","message":e}});
            return Response::builder()
                .status(StatusCode::BAD_GATEWAY)
                .header("content-type", "application/json")
                .body(full_body(Bytes::from(err.to_string())))
                .unwrap();
        }
    };

    let upstream_status = resp.status().as_u16();
    if upstream_status >= 400 {
        let body_text = resp.text().await.unwrap_or_default();
        let kind = match upstream_status {
            429 => "rate_limit_error",
            401 | 403 => "authentication_error",
            400 => "invalid_request_error",
            _ => "api_error",
        };
        let err = json!({"type":"error","error":{"type":kind,"message":body_text}});
        return Response::builder()
            .status(StatusCode::from_u16(upstream_status).unwrap_or(StatusCode::BAD_GATEWAY))
            .header("content-type", "application/json")
            .body(full_body(Bytes::from(err.to_string())))
            .unwrap();
    }

    if !is_stream {
        // Non-streaming: collect and return as single Anthropic JSON
        let full: Value = match resp.json().await {
            Ok(v) => v,
            Err(e) => {
                let err = json!({"type":"error","error":{"type":"api_error","message":format!("parse upstream: {e}")}});
                return Response::builder()
                    .status(StatusCode::BAD_GATEWAY)
                    .header("content-type", "application/json")
                    .body(full_body(Bytes::from(err.to_string())))
                    .unwrap();
            }
        };
        let choice = full
            .pointer("/choices/0/message")
            .cloned()
            .unwrap_or(json!({}));
        let finish_reason = full
            .pointer("/choices/0/finish_reason")
            .and_then(Value::as_str)
            .unwrap_or("stop");

        // Build the content blocks: thinking + text + tool_use
        let mut blocks: Vec<Value> = Vec::new();
        if let Some(rc) = choice.get("reasoning_content").and_then(Value::as_str) {
            if !rc.is_empty() {
                blocks.push(json!({"type":"thinking","thinking":rc}));
            }
        }
        if let Some(text) = choice.get("content").and_then(Value::as_str) {
            if !text.is_empty() {
                blocks.push(json!({"type":"text","text":text}));
            }
        }
        if let Some(tool_calls) = choice.get("tool_calls").and_then(Value::as_array) {
            for tc in tool_calls {
                let id = tc.get("id").and_then(Value::as_str).unwrap_or("");
                let name = tc
                    .pointer("/function/name")
                    .and_then(Value::as_str)
                    .unwrap_or("");
                let args = tc
                    .pointer("/function/arguments")
                    .and_then(Value::as_str)
                    .unwrap_or("{}");
                let input: Value = serde_json::from_str(args).unwrap_or(json!({}));
                blocks.push(json!({"type":"tool_use","id":id,"name":name,"input":input}));
            }
        }
        if blocks.is_empty() {
            blocks.push(json!({"type":"text","text":""}));
        }
        let stop_reason = match finish_reason {
            "tool_calls" => "tool_use",
            "length" => "max_tokens",
            _ => "end_turn",
        };
        let usage = full.get("usage").cloned().unwrap_or(json!({}));
        let result = json!({
            "id": format!("msg_{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos()),
            "type": "message",
            "role": "assistant",
            "model": model,
            "content": blocks,
            "stop_reason": stop_reason,
            "stop_sequence": null,
            "usage": {
                "input_tokens": usage.get("prompt_tokens").and_then(Value::as_u64).unwrap_or(0),
                "output_tokens": usage.get("completion_tokens").and_then(Value::as_u64).unwrap_or(0),
            }
        });
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(full_body(Bytes::from(result.to_string())))
            .unwrap();
    }

    // Streaming: pump OAI SSE → Anthropic SSE
    let (tx, rx) = mpsc::channel::<String>(8);

    // Spawn the pump task that reads upstream and sends Anthropic events
    let model_clone = model.clone();
    tokio::spawn(async move {
        let mut upstream_stream = resp;
        let mut buf: Vec<u8> = Vec::new();
        let mut block_index: usize = 0;
        let mut in_thinking = false;
        let mut in_tool: Option<(usize, String)> = None; // (index, name)
        let mut text_block_index: Option<usize> = None; // text block needs explicit start
        let mut output_tokens: u64 = 0;
        let mut input_tokens: u64 = 0;
        let mut stop_reason = "end_turn";
        let mut last_activity = std::time::Instant::now();
        let mut send_failed = false;

        let _ = tx.send(message_start(&model_clone)).await;
        let _ = tx.send(ping_event()).await;

        loop {
            // Idle timeout
            if last_activity.elapsed() > MAX_STREAM_IDLE {
                let _ = tx
                    .send(anthropic_error("api_error", "upstream stream idle timeout"))
                    .await;
                send_failed = true;
                break;
            }

            let next = tokio::time::timeout(PING_INTERVAL, upstream_stream.chunk()).await;
            match next {
                Err(_elapsed) => {
                    let _ = tx.send(ping_event()).await;
                    continue;
                }
                Ok(Err(e)) => {
                    let _ = tx
                        .send(anthropic_error(
                            "api_error",
                            &format!("upstream stream error: {e}"),
                        ))
                        .await;
                    send_failed = true;
                    break;
                }
                Ok(Ok(None)) => break,
                Ok(Ok(Some(chunk))) => {
                    last_activity = std::time::Instant::now();
                    buf.extend_from_slice(&chunk);

                    while let Some(line) = take_line(&mut buf) {
                        if let Some(data) = line.strip_prefix("data: ") {
                            let data = data.trim();

                            if data == "[DONE]" {
                                // Close any open blocks
                                if in_thinking {
                                    let _ = tx.send(content_block_stop(block_index)).await;
                                    in_thinking = false;
                                    block_index += 1;
                                }
                                if let Some((idx, _)) = in_tool.take() {
                                    let _ = tx.send(content_block_stop(idx)).await;
                                }
                                if let Some(tbi) = text_block_index.take() {
                                    let _ = tx.send(content_block_stop(tbi)).await;
                                }
                                let _ = tx
                                    .send(message_delta(stop_reason, input_tokens, output_tokens))
                                    .await;
                                let _ = tx.send(message_stop()).await;
                                send_failed = true; // signal end
                                break;
                            }

                            let Ok(v) = serde_json::from_str::<Value>(data) else {
                                continue;
                            };

                            // Usage from final chunk
                            if let Some(u) = v.get("usage") {
                                if let Some(pt) = u.get("prompt_tokens").and_then(Value::as_u64) {
                                    input_tokens = pt;
                                }
                                if let Some(ot) = u.get("completion_tokens").and_then(Value::as_u64)
                                {
                                    output_tokens = ot;
                                }
                            }

                            let delta = v.pointer("/choices/0/delta").cloned().unwrap_or(json!({}));

                            // reasoning_content → thinking block
                            if let Some(rc) = delta.get("reasoning_content").and_then(Value::as_str)
                            {
                                if !rc.is_empty() {
                                    if !in_thinking {
                                        // A thinking block never shares an index
                                        // with an open text/tool block.
                                        if let Some(tbi) = text_block_index.take() {
                                            let _ = tx.send(content_block_stop(tbi)).await;
                                            block_index = tbi + 1;
                                        }
                                        if let Some((prev_idx, _)) = in_tool.take() {
                                            let _ = tx.send(content_block_stop(prev_idx)).await;
                                            block_index = prev_idx + 1;
                                        }
                                        let _ = tx
                                            .send(content_block_start(block_index, "thinking"))
                                            .await;
                                        in_thinking = true;
                                    }
                                    let _ = tx
                                        .send(content_block_delta_thinking(block_index, rc))
                                        .await;
                                }
                            }

                            // content → text block
                            if let Some(c) = delta.get("content").and_then(Value::as_str) {
                                if !c.is_empty() {
                                    if in_thinking {
                                        let _ = tx.send(content_block_stop(block_index)).await;
                                        in_thinking = false;
                                        block_index += 1;
                                    }
                                    if let Some((idx, _)) = in_tool.take() {
                                        let _ = tx.send(content_block_stop(idx)).await;
                                        block_index = idx + 1;
                                    }
                                    // Emit content_block_start("text") once, then deltas
                                    if text_block_index.is_none() {
                                        let _ =
                                            tx.send(content_block_start(block_index, "text")).await;
                                        text_block_index = Some(block_index);
                                    }
                                    let _ = tx.send(content_block_delta_text(block_index, c)).await;
                                }
                            }

                            // tool_calls → tool_use block
                            if let Some(tc) = delta.get("tool_calls").and_then(Value::as_array) {
                                for call in tc {
                                    if let Some(name) =
                                        call.pointer("/function/name").and_then(Value::as_str)
                                    {
                                        // Close every open block before starting
                                        // a new tool_use (parallel tool calls arrive
                                        // as successive named deltas).
                                        if in_thinking {
                                            let _ = tx.send(content_block_stop(block_index)).await;
                                            in_thinking = false;
                                            block_index += 1;
                                        }
                                        if let Some(tbi) = text_block_index.take() {
                                            let _ = tx.send(content_block_stop(tbi)).await;
                                            block_index = tbi + 1;
                                        }
                                        if let Some((prev_idx, _)) = in_tool.take() {
                                            let _ = tx.send(content_block_stop(prev_idx)).await;
                                            block_index = prev_idx + 1;
                                        }
                                        let id = call
                                            .get("id")
                                            .and_then(Value::as_str)
                                            .filter(|s| !s.is_empty());
                                        let id = id.map(String::from).unwrap_or_else(|| {
                                            format!(
                                                "toolu_{}",
                                                std::time::SystemTime::now()
                                                    .duration_since(std::time::UNIX_EPOCH)
                                                    .unwrap_or_default()
                                                    .as_nanos()
                                            )
                                        });
                                        let _ = tx
                                            .send(content_block_start_tool(block_index, &id, name))
                                            .await;
                                        in_tool = Some((block_index, name.to_string()));
                                        stop_reason = "tool_use";
                                    }
                                    if let Some(args) =
                                        call.pointer("/function/arguments").and_then(Value::as_str)
                                    {
                                        if let Some((bi, _)) = in_tool.as_ref() {
                                            let _ = tx
                                                .send(content_block_delta_tool_json(*bi, args))
                                                .await;
                                        }
                                    }
                                }
                            }

                            // finish_reason
                            if let Some(fr) = v
                                .pointer("/choices/0/finish_reason")
                                .and_then(Value::as_str)
                            {
                                match fr {
                                    "tool_calls" => stop_reason = "tool_use",
                                    "length" => stop_reason = "max_tokens",
                                    _ => stop_reason = "end_turn",
                                }
                            }
                        }
                    }
                    if send_failed {
                        break;
                    }
                }
            }
        }

        // Stream ended without [DONE]: close gracefully or error
        if !send_failed {
            if in_thinking {
                let _ = tx.send(content_block_stop(block_index)).await;
            }
            if let Some((idx, _)) = in_tool.take() {
                let _ = tx.send(content_block_stop(idx)).await;
            }
            if let Some(tbi) = text_block_index.take() {
                let _ = tx.send(content_block_stop(tbi)).await;
            }
            let _ = tx
                .send(message_delta(stop_reason, input_tokens, output_tokens))
                .await;
            let _ = tx.send(message_stop()).await;
        }
        drop(tx);
    });

    // Build streaming response body
    use tokio_stream::StreamExt as _;
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    let body =
        StreamBody::new(stream.map(|s| Ok::<_, std::io::Error>(Frame::data(Bytes::from(s)))));

    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .body(UnsyncBoxBody::new(body))
        .unwrap()
}

// ─── Route handler ─────────────────────────────────────────────────────────

async fn route(cfg: Arc<Cfg>, req: Request<Incoming>) -> Response<BoxBody> {
    let method = req.method().as_str();
    let path = req.uri().path().to_string();

    // HEAD probe → 200
    if method == "HEAD" {
        return Response::builder()
            .status(StatusCode::OK)
            .body(full_body(Bytes::new()))
            .unwrap();
    }

    // Health
    if method == "GET" && path == "/health" {
        return Response::builder()
            .status(StatusCode::OK)
            .body(full_body(Bytes::from("ok")))
            .unwrap();
    }

    // Auth check (constant-time Bearer)
    if !cfg.master_key.is_empty() {
        let auth = req
            .headers()
            .get("authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        let provided = auth.strip_prefix("Bearer ").unwrap_or("");
        if !constant_time_eq(provided.as_bytes(), cfg.master_key.as_bytes()) {
            let err = json!({"type":"error","error":{"type":"authentication_error","message":"invalid bearer token"}});
            return Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .header("content-type", "application/json")
                .body(full_body(Bytes::from(err.to_string())))
                .unwrap();
        }
    }

    // GET /v1/models
    if method == "GET" && path == "/v1/models" {
        let models: Vec<Value> = MODELS
            .iter()
            .map(|(slug, display)| {
                json!({"id":format!("claude-{slug}"),"display_name":display,"type":"model","created_at":"2026-10-15T00:00:00Z"})
            })
            .collect();
        let out = json!({"data":models});
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(full_body(Bytes::from(out.to_string())))
            .unwrap();
    }

    // POST /v1/messages/count_tokens
    if method == "POST" && path == "/v1/messages/count_tokens" {
        let (_, body) = req.into_parts();
        let bytes = match body.collect().await {
            Ok(c) => c.to_bytes(),
            Err(_) => {
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .body(full_body(Bytes::from("{\"error\":\"bad body\"}")))
                    .unwrap();
            }
        };
        if bytes.len() > BODY_CAP {
            return Response::builder()
                .status(StatusCode::PAYLOAD_TOO_LARGE)
                .body(full_body(Bytes::from("{\"error\":\"body too large\"}")))
                .unwrap();
        }
        let text = String::from_utf8_lossy(&bytes);
        let n = estimate_tokens(&text);
        let out = json!({"input_tokens": n});
        return Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .body(full_body(Bytes::from(out.to_string())))
            .unwrap();
    }

    // POST /v1/messages
    if method == "POST" && path == "/v1/messages" {
        let (_, body) = req.into_parts();
        let bytes = match body.collect().await {
            Ok(c) => c.to_bytes(),
            Err(_) => {
                let err = json!({"type":"error","error":{"type":"invalid_request_error","message":"bad body"}});
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .header("content-type", "application/json")
                    .body(full_body(Bytes::from(err.to_string())))
                    .unwrap();
            }
        };
        if bytes.len() > BODY_CAP {
            let err = json!({"type":"error","error":{"type":"invalid_request_error","message":"body too large"}});
            return Response::builder()
                .status(StatusCode::PAYLOAD_TOO_LARGE)
                .header("content-type", "application/json")
                .body(full_body(Bytes::from(err.to_string())))
                .unwrap();
        }
        let parsed: Value = match serde_json::from_slice(&bytes) {
            Ok(v) => v,
            Err(e) => {
                let err = json!({"type":"error","error":{"type":"invalid_request_error","message":format!("parse: {e}")}});
                return Response::builder()
                    .status(StatusCode::BAD_REQUEST)
                    .header("content-type", "application/json")
                    .body(full_body(Bytes::from(err.to_string())))
                    .unwrap();
            }
        };
        return handle_messages(cfg, parsed).await;
    }

    // 404
    let err = json!({"type":"error","error":{"type":"not_found_error","message":format!("no route: {method} {path}")}});
    Response::builder()
        .status(StatusCode::NOT_FOUND)
        .header("content-type", "application/json")
        .body(full_body(Bytes::from(err.to_string())))
        .unwrap()
}

// ─── Main ───────────────────────────────────────────────────────────────────

fn main() {
    let addr = parse_addr();
    let master_key = std::env::var("GROK_MASTER_KEY").unwrap_or_default();

    // Fail-closed: no token from grok = exit
    let (token, expiry) = match load_grok_token() {
        Ok(t) => t,
        Err(e) => {
            eprintln!("[grok-proxy] FATAL: {e}");
            std::process::exit(1);
        }
    };
    eprintln!("[grok-proxy] token loaded (expires: {expiry})");

    if master_key.is_empty() {
        eprintln!(
            "[grok-proxy] WARNING: GROK_MASTER_KEY unset; any local process can use this proxy"
        );
    }

    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(5))
        .timeout(std::time::Duration::from_secs(300))
        .pool_idle_timeout(std::time::Duration::from_secs(90))
        .no_gzip()
        .no_deflate()
        .no_brotli()
        .build()
        .expect("build client");

    let cfg = Arc::new(Cfg {
        client,
        token: std::sync::RwLock::new(token),
        auth_path: grok_home().join("auth.json"),
        master_key,
        addr,
    });

    eprintln!("[grok-proxy] listening on {addr} (loopback-only, fail-closed)");

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");

    rt.block_on(async move {
        let listener = tokio::net::TcpListener::bind(cfg.addr).await.expect("bind");
        loop {
            let Ok((stream, _peer)) = listener.accept().await else {
                continue;
            };
            let cfg = cfg.clone();
            tokio::spawn(async move {
                let service = service_fn(move |req| {
                    let cfg = cfg.clone();
                    async move { Ok::<_, std::convert::Infallible>(route(cfg, req).await) }
                });
                let _ = hyper::server::conn::http1::Builder::new()
                    .keep_alive(true)
                    .serve_connection(TokioIo::new(stream), service)
                    .await;
            });
        }
    });
}

// ─── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_slug() {
        assert_eq!(resolve_slug("claude-grok-4.7"), "grok-4.7");
        assert_eq!(resolve_slug("grok-4.7"), "grok-4.7");
        assert_eq!(
            resolve_slug("claude-grok-4.7-build-fast"),
            "grok-4.7-build-fast"
        );
    }

    #[test]
    fn test_is_valid_slug() {
        assert!(is_valid_slug("grok-4.7"));
        assert!(is_valid_slug("grok-4.7-build-fast"));
        assert!(!is_valid_slug("grok-99"));
        assert!(!is_valid_slug("claude-sonnet-4"));
    }

    #[test]
    fn test_convert_basic() {
        let req = json!({"model":"claude-grok-4.7","max_tokens":100,"stream":true,"messages":[{"role":"user","content":"hi"}]});
        let out = convert_to_chat(&req);
        assert_eq!(out["model"], "grok-4.7");
        assert_eq!(out["messages"][0]["role"], "user");
        assert_eq!(out["messages"][0]["content"], "hi");
        assert_eq!(out["stream"], true);
        assert_eq!(out["stream_options"]["include_usage"], true);
    }

    #[test]
    fn test_convert_system() {
        let req = json!({"model":"claude-grok-4.7","system":"You are helpful","max_tokens":10,"messages":[{"role":"user","content":"hi"}]});
        let out = convert_to_chat(&req);
        assert_eq!(out["messages"][0]["role"], "system");
        assert_eq!(out["messages"][0]["content"], "You are helpful");
        assert_eq!(out["messages"][1]["role"], "user");
    }

    #[test]
    fn test_convert_tool_result() {
        let req = json!({
            "model":"claude-grok-4.7","max_tokens":10,
            "messages":[
                {"role":"user","content":[{"type":"tool_result","tool_use_id":"t1","content":"42"}]}
            ]
        });
        let out = convert_to_chat(&req);
        assert_eq!(out["messages"][0]["role"], "tool");
        assert_eq!(out["messages"][0]["tool_call_id"], "t1");
        assert_eq!(out["messages"][0]["content"], "42");
    }

    #[test]
    fn test_convert_tools() {
        let req = json!({
            "model":"claude-grok-4.7","max_tokens":10,
            "tools":[{"name":"get_time","description":"Get time","input_schema":{"type":"object","properties":{}}}],
            "messages":[{"role":"user","content":"what time"}]
        });
        let out = convert_to_chat(&req);
        assert_eq!(out["tools"][0]["type"], "function");
        assert_eq!(out["tools"][0]["function"]["name"], "get_time");
        assert_eq!(out["tool_choice"], "auto");
    }

    #[test]
    fn test_effort_from_output_config() {
        let req = json!({"model":"claude-grok-4.7","max_tokens":10,"output_config":{"effort":"high"},"messages":[]});
        let out = convert_to_chat(&req);
        assert_eq!(out["reasoning_effort"], "high");
    }

    #[test]
    fn test_effort_from_thinking() {
        let req = json!({"model":"claude-grok-4.7","max_tokens":10,"thinking":{"type":"enabled","budget_tokens":32000},"messages":[]});
        let out = convert_to_chat(&req);
        assert_eq!(out["reasoning_effort"], "high");
    }

    #[test]
    fn test_effort_lixo_not_forwarded() {
        let req = json!({"model":"claude-grok-4.7","max_tokens":10,"output_config":{"effort":"ultracode"},"messages":[]});
        let out = convert_to_chat(&req);
        assert!(out.get("reasoning_effort").is_none());
    }

    #[test]
    fn test_quota_detection() {
        assert!(is_quota_exhaustion("The usage limit has been reached"));
        assert!(is_quota_exhaustion("{\"error\":\"spending limit\"}"));
        assert!(!is_quota_exhaustion("regular error"));
    }

    #[test]
    fn test_estimate_tokens() {
        assert_eq!(estimate_tokens("hello world!"), 3); // 12 ascii / 4
        assert!(estimate_tokens("你好世界") > 0); // CJK
    }

    #[test]
    fn test_take_line() {
        let mut buf = b"event: x\ndata: y\n\n".to_vec();
        assert_eq!(take_line(&mut buf).unwrap(), "event: x");
        assert_eq!(take_line(&mut buf).unwrap(), "data: y");
    }

    #[test]
    fn test_take_line_utf8_split() {
        // Simulate a UTF-8 multibyte split across chunks
        let mut buf = vec![0x68, 0x65, 0x6c, 0x6c, 0x6f]; // "hello"
        buf.push(b'\n');
        assert_eq!(take_line(&mut buf).unwrap(), "hello");
    }

    #[test]
    fn test_constant_time_eq() {
        assert!(constant_time_eq(b"abc", b"abc"));
        assert!(!constant_time_eq(b"abc", b"abd"));
        assert!(!constant_time_eq(b"abc", b"abcd"));
    }

    #[test]
    fn test_sse_event_format() {
        let ev = sse_event("ping", &json!({"type":"ping"}));
        assert!(ev.starts_with("event: ping\ndata: {\"type\":\"ping\"}\n\n"));
    }

    #[test]
    fn test_split_effort() {
        assert_eq!(split_effort("grok-4.7"), ("grok-4.7", None));
        assert_eq!(split_effort("grok-4.7:high"), ("grok-4.7", Some("high")));
        assert_eq!(
            split_effort("grok-4.7-build-fast:low"),
            ("grok-4.7-build-fast", Some("low"))
        );
        // Invalid effort stays part of the slug → fails model validation
        assert_eq!(
            split_effort("grok-4.7:ultracode"),
            ("grok-4.7:ultracode", None)
        );
        assert_eq!(split_effort("grok-4.7:"), ("grok-4.7:", None));
    }

    #[test]
    fn test_effort_suffix_model() {
        let req = json!({"model":"claude-grok-4.7:high","max_tokens":10,"messages":[]});
        let out = convert_to_chat(&req);
        assert_eq!(out["model"], "grok-4.7");
        assert_eq!(out["reasoning_effort"], "high");
    }

    #[test]
    fn test_effort_output_config_wins_over_suffix() {
        let req = json!({
            "model":"claude-grok-4.7:low",
            "max_tokens":10,
            "output_config":{"effort":"high"},
            "messages":[]
        });
        let out = convert_to_chat(&req);
        assert_eq!(out["reasoning_effort"], "high");
    }

    #[test]
    fn test_system_array_blocks() {
        // Modern Claude Code sends system as an array of text blocks
        let req = json!({
            "model":"claude-grok-4.7","max_tokens":10,
            "system":[{"type":"text","text":"You are Claude Code"},{"type":"text","text":"Be brief"}],
            "messages":[{"role":"user","content":"hi"}]
        });
        let out = convert_to_chat(&req);
        assert_eq!(out["messages"][0]["role"], "system");
        assert_eq!(
            out["messages"][0]["content"],
            "You are Claude Code\nBe brief"
        );
        assert_eq!(out["messages"][1]["role"], "user");
    }

    #[test]
    fn test_tool_result_with_text_not_dropped() {
        let req = json!({
            "model":"claude-grok-4.7","max_tokens":10,
            "messages":[
                {"role":"user","content":[
                    {"type":"tool_result","tool_use_id":"t1","content":"42"},
                    {"type":"text","text":"and also this"}
                ]}
            ]
        });
        let out = convert_to_chat(&req);
        assert_eq!(out["messages"][0]["role"], "tool");
        assert_eq!(out["messages"][0]["tool_call_id"], "t1");
        assert_eq!(out["messages"][0]["content"], "42");
        assert_eq!(out["messages"][1]["role"], "user");
        assert_eq!(out["messages"][1]["content"], "and also this");
    }

    #[test]
    fn test_tool_result_array_content() {
        let req = json!({
            "model":"claude-grok-4.7","max_tokens":10,
            "messages":[
                {"role":"user","content":[
                    {"type":"tool_result","tool_use_id":"t1","content":[{"type":"text","text":"part a"},{"type":"text","text":"part b"}]}
                ]}
            ]
        });
        let out = convert_to_chat(&req);
        assert_eq!(out["messages"][0]["role"], "tool");
        assert_eq!(out["messages"][0]["content"], "part apart b");
    }

    #[test]
    fn test_tool_choice_mapping() {
        let base = json!({
            "model":"claude-grok-4.7","max_tokens":10,
            "tools":[{"name":"get_time","input_schema":{"type":"object"}}],
            "messages":[{"role":"user","content":"hi"}]
        });
        let mut req = base.clone();
        req["tool_choice"] = json!({"type":"any"});
        assert_eq!(convert_to_chat(&req)["tool_choice"], "required");

        let mut req = base.clone();
        req["tool_choice"] = json!({"type":"none"});
        assert_eq!(convert_to_chat(&req)["tool_choice"], "none");

        let mut req = base.clone();
        req["tool_choice"] = json!({"type":"tool","name":"get_time"});
        let out = convert_to_chat(&req);
        assert_eq!(out["tool_choice"]["type"], "function");
        assert_eq!(out["tool_choice"]["function"]["name"], "get_time");

        let out = convert_to_chat(&base);
        assert_eq!(out["tool_choice"], "auto");
    }

    #[test]
    fn test_content_block_start_tool_has_id_and_name() {
        let ev = content_block_start_tool(3, "toolu_abc", "get_time");
        assert!(ev.contains("\"index\":3"));
        assert!(ev.contains("\"type\":\"tool_use\""));
        assert!(ev.contains("\"id\":\"toolu_abc\""));
        assert!(ev.contains("\"name\":\"get_time\""));
        assert!(ev.contains("\"input\":{}"));
    }

    #[test]
    fn test_message_delta_has_input_tokens() {
        let ev = message_delta("end_turn", 120, 45);
        assert!(ev.contains("\"input_tokens\":120"));
        assert!(ev.contains("\"output_tokens\":45"));
        assert!(ev.contains("\"stop_reason\":\"end_turn\""));
    }

    #[test]
    fn test_parse_terminal() {
        assert_eq!(
            parse_terminal("__TERMINAL__:401:token expired"),
            Some((401, "token expired"))
        );
        // body with colons stays intact
        assert_eq!(
            parse_terminal("__TERMINAL__:429:quota: is: exhausted"),
            Some((429, "quota: is: exhausted"))
        );
        assert_eq!(parse_terminal("__TERMINAL__:403:"), Some((403, "")));
        assert_eq!(parse_terminal("upstream: network error"), None);
    }
}
