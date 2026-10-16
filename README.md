# grok-proxy-cc

[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.75%2B-orange.svg)](https://www.rust-lang.org)
[![Build](https://img.shields.io/badge/build-passing-brightgreen.svg)](.github/workflows/ci.yml)
[![Binário](https://img.shields.io/badge/binary-1.2M%20UPX-blueviolet.svg)](#build)

An **Anthropic Messages-API-compatible** HTTP proxy, written in Rust, that lets
[Claude Code](https://docs.anthropic.com/en/docs/claude-code) drive **Grok (xAI)**
models through your existing **Grok CLI login** — no API key, no paid tokens.

Claude Code speaks Anthropic `/v1/messages`; the proxy translates each request
into an OpenAI Chat Completions call against `cli-chat-proxy.grok.com`, using
the **OIDC token** the `grok` CLI already stores in `~/.grok/auth.json`, and
streams the answer back as Anthropic SSE.

```
claude --model claude-grok-4.7
  → grok-proxy-cc :4112 (Anthropic Messages → OpenAI Chat Completions)
    → cli-chat-proxy.grok.com/v1 (com OIDC JWT do seu login)
      → grok-4.7 responde
```

## Features

- **Zero API key** — reuses the Grok CLI's OIDC session from `~/.grok/auth.json`
- **4 models**: `grok-4.7`, `grok-4.7-build-fast`, `grok-4.6`, `grok-4.5`
- **Streaming SSE**: `reasoning_content` → thinking blocks, content → text,
  `tool_calls` → tool_use — full Anthropic event stream
- **Tool calling**: `tool_use` ↔ `tool_calls`, `tool_result` → `role:tool`
- **Native effort control**: Claude Code's `/effort` slider → `reasoning_effort`
- **Chain fallback**: `grok-4.7 → 4.7-build-fast → 4.6 → 4.5` (same-provider)
- **Token auto-recovery**: re-reads `~/.grok/auth.json` on upstream 401 —
  the proxy survives the 6-hour OIDC token rotation without restart
- **Fail-closed**: quota exhaustion → terminal `429` with `retry-after: 3600`,
  never silently falls back
- **Loopback-only**: binds `127.0.0.1`, never exposed to the network
- **Constant-time auth**: local Bearer token checked in O(n) without timing leaks

## Born at maximum optimization

This proxy was designed from day one to need zero rework:

| Property | Value |
|----------|-------|
| HTTP framework | **hyper direct** (no axum layer) |
| Runtime | **tokio `current_thread`** (single-user loopback) |
| Futures | **`futures-core`** (trait only, not futures-util) |
| Profile | `opt-level=3`, `lto="fat"`, `codegen-units=1`, `panic="abort"`, `strip=true` |
| Build | `RUSTFLAGS="-C target-cpu=native"` (SIMD for your exact CPU) |
| Binary | **1.2 MB** with `upx --best --lzma` |
| Dependencies | **11** — zero uuid, chrono, anyhow, clap, tracing |
| Logging | `eprintln!` direct (no logger framework) |

## Prerequisites

- Rust toolchain (`cargo`)
- The Grok CLI, logged in: `grok login`
- `upx` (optional, for binary compression)

## Quick start

```sh
# 1. Clone and build
git clone https://github.com/rianprei/grok-proxy-cc.git
cd grok-proxy-cc
RUSTFLAGS="-C target-cpu=native" cargo build --release
upx --best --lzma target/release/grok-proxy-cc   # optional

# 2. Generate a local auth token
mkdir -p ~/.config/grok-proxy-cc
echo "GROK_MASTER_KEY=$(cat /proc/sys/kernel/random/uuid | tr -d '-')" \
  > ~/.config/grok-proxy-cc/proxy.env
chmod 600 ~/.config/grok-proxy-cc/proxy.env

# 3. Run
GROK_PROXY_ADDR=127.0.0.1:4112 \
GROK_MASTER_KEY=<your-key> \
./target/release/grok-proxy-cc
```

Then point Claude Code at it:

```sh
ANTHROPIC_BASE_URL="http://127.0.0.1:4112" \
ANTHROPIC_AUTH_TOKEN="<your-key>" \
CLAUDE_CODE_ENABLE_GATEWAY_MODEL_DISCOVERY=1 \
claude --model claude-grok-4.7
```

Or add to your Claude Code settings' `modelPicker`:

```json
{
  "model": "claude-grok-4.7",
  "label": "Grok 4.7",
  "description": "xAI Grok via login",
  "behavesAs": "claude-opus-4-8"
}
```

## Model ids

| Public id | Upstream slug | behavesAs | Description |
|-----------|---------------|-----------|-------------|
| `claude-grok-4.7` | `grok-4.7` | `claude-opus-4-8` | Frontier + vision |
| `claude-grok-4.7-build-fast` | `grok-4.7-build-fast` | `claude-sonnet-5` | Fast coding |
| `claude-grok-4.6` | `grok-4.6` | `claude-sonnet-5` | Previous frontier |
| `claude-grok-4.5` | `grok-4.5` | `claude-sonnet-4-6` | Stable |

An optional `:<effort>` suffix pins reasoning effort per-request:
`claude-grok-4.7:high`. The native Claude Code effort control
(`/effort`, `output_config.effort`) takes precedence.

## Chain fallback

Same-provider fallback (like [codex-proxy-cc](https://github.com/rianprei/codex-proxy-cc)):

```sh
GROK_CHAIN="grok-4.7,grok-4.7-build-fast,grok-4.6,grok-4.5" ./grok-proxy-cc
```

On upstream 429/5xx (non-quota), the proxy walks the chain. Quota exhaustion
is **terminal** — every model would fail identically.

## Auth: how it works

The Grok CLI stores an OIDC session in `~/.grok/auth.json`:

```json
{
  "https://auth.x.ai::<client-id>": {
    "key": "<JWT access token>",
    "refresh_token": "<opaque>",
    "expires_at": "2026-10-16T19:06:53Z",
    "auth_mode": "oidc"
  }
}
```

The proxy reads the `key` field at boot. When the upstream responds 401
(the JWT has a 6-hour lifetime), the proxy **re-reads the file** — the `grok`
CLI refreshes the token in-place — and retries once. No restart needed.

The proxy also sends the identity headers the xAI inference proxy gates on:

```
Authorization: Bearer <oidc-jwt>
x-xai-token-auth: xai-grok-cli
x-grok-client-identifier: grok-shell
x-grok-client-version: 1.0.52
```

## Systemd

See [deploy/grok-proxy-cc.service](deploy/grok-proxy-cc.service) for a
production unit with full hardening and resource tuning:

```sh
mkdir -p ~/.local/bin
cp target/release/grok-proxy-cc ~/.local/bin/
cp deploy/grok-proxy-cc.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemctl --user enable --now grok-proxy-cc
```

## API

| Endpoint | Description |
|----------|-------------|
| `HEAD /` | Health probe |
| `GET /health` | Returns `ok` |
| `GET /v1/models` | Anthropic-shaped model list (for gateway discovery) |
| `POST /v1/messages/count_tokens` | Token estimation |
| `POST /v1/messages` | Full Anthropic Messages API (streaming + non-streaming) |

## Architecture

See [docs/architecture.md](docs/architecture.md).

## Related projects

- [rianprei/codex-proxy-cc](https://github.com/rianprei/codex-proxy-cc) — same concept for OpenAI Codex (ChatGPT subscription)
- [rianprei/claude-nim](https://github.com/rianprei/claude-nim) — same concept for NVIDIA NIM

## License

[MIT](LICENSE) © rianprei
