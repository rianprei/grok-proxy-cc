# Architecture

Single-file Rust proxy (~1000 lines in `src/main.rs`), born at maximum
optimization. No axum, no tracing, no uuid, no chrono, no anyhow, no clap.

## Design principles

1. **hyper direct** — the HTTP server is `hyper::server::conn::http1` with
   `TokioIo`, no framework layer. Same approach as
   [claude-nim](https://github.com/rianprei/claude-nim).
2. **tokio `current_thread`** — loopback single-user proxy; the multi-thread
   scheduler's synchronization overhead is pure waste here.
3. **`futures-core`** — only the `Stream` trait, not the full futures-util.
4. **`eprintln!`** — direct stderr logging; `RUST_LOG` semantics are handled
   by a ~35-line init function, not the `env_logger` tree.
5. **Fail-closed everywhere** — quota is terminal, unknown models 404,
   no silent fallback to another provider, ever.

## Request flow

```
Claude Code (Anthropic Messages API)
    │
    │ POST /v1/messages  {model: "claude-grok-4.7", ...}
    ▼
route() ── constant-time Bearer check ──► 401 if no/wrong token
    │
    ├── HEAD /          → 200
    ├── GET /health     → 200 "ok"
    ├── GET /v1/models   → static catalog (4 models)
    ├── POST /count_tokens → estimate_tokens
    └── POST /v1/messages
        │
        ▼
    convert_to_chat()  Anthropic → OpenAI Chat Completions
        │              - system → messages[0].role=system
        │              - tool_result in role:user → role:tool
        │              - tool_use → tool_calls
        │              - output_config.effort → reasoning_effort
        │              - thinking.budget_tokens → reasoning_effort (coarse)
        │
        ▼
    call_with_chain()  try selected model, walk chain on 429/5xx
        │              - quota → terminal 429 + retry-after: 3600
        │              - 401 → reload_token() + retry once
        ▼
    pump (tokio::spawn)  OpenAI SSE chunks → Anthropic SSE events
        │              - reasoning_content → thinking block
        │              - content → text block (explicit content_block_start)
        │              - tool_calls → tool_use block
        │              - [DONE] → message_delta + message_stop
        │              - ping every 5s; idle timeout 300s
        ▼
Claude Code receives Anthropic-format SSE stream
```

## Auth model

Three sources, checked in order (fail-closed: exit 1 if none):

1. `~/.grok/auth.json` — the Grok CLI's OIDC session. A JSON **map** keyed
   by `"<issuer>::<client_id>"`; each entry has `key` (JWT), `refresh_token`,
   `expires_at`. The proxy reads `key` as the Bearer token.
2. `GROK_MASTER_KEY` env — the **local client-auth** secret that Claude Code
   must present (unrelated to the upstream token).
3. Empty `key` → exit(1) with a clear message.

### Token lifecycle

The OIDC JWT has a 6-hour lifetime. The proxy:

- Reads it once at boot.
- On upstream **401**: re-reads `~/.grok/auth.json` (the `grok` CLI refreshes
  the file in-place every ~6h) and retries **once**. This is `RwLock<String>`
  in `Cfg` — one write per rotation, reads on every request's header build.
- If the file hasn't changed (grok CLI not running): warns and surfaces
  the 401 to the client.

### Identity headers

The xAI inference proxy (`cli-chat-proxy.grok.com`) gates on CLI identity
headers — without them it returns "your CLI version is outdated":

```
x-xai-token-auth: xai-grok-cli
x-grok-client-identifier: grok-shell
x-grok-client-version: 1.0.52
```

## Chain fallback

Same-provider, opt-out via `GROK_CHAIN` env (default:
`grok-4.7,grok-4.7-build-fast,grok-4.6,grok-4.5`):

- Trigger: upstream 429/5xx that is **not** quota exhaustion.
- Walk: chain entries after the selected slug (selected model always
  tried first, never re-attempted).
- Quota anywhere → stop immediately, terminal 429.
- Exhaustion → surface the last upstream error.

## Binary size budget

| Stage | Size |
|-------|------|
| Source | ~1000 lines, 11 direct deps |
| `cargo build --release` (opt=3, LTO fat, codegen-units=1, strip) | 3.1 MB |
| `upx --best --lzma` | **1.2 MB** |

The `ring` crate (rustls) dominates the binary; everything else is a rounding
error. `target-cpu=native` adds AES-NI acceleration for the TLS hot path.

## Test coverage

15 unit tests covering:

- Model resolution (`claude-grok-4.7` → `grok-4.7`)
- Conversion (basic, system, tool_result, tools, effort from output_config
  and thinking, garbage effort rejection)
- Quota detection
- Token estimation (ASCII + CJK)
- UTF-8-safe SSE line extraction
- Constant-time auth comparison
- SSE event format
