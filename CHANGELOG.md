# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1] — 2026-10-16

### Fixed

- **tool_use blocks now carry `id` and `name` in `content_block_start`** —
  previously the start event was emitted with an empty name and no id,
  making streamed tool calls undispatchable by the client.
- **Streaming index collisions**: a second named `tool_calls` delta (parallel
  tool calls) re-used the open block's index without closing it; a
  `reasoning_content` delta arriving after text/tool did the same. Every
  block now closes the previous one before starting, keeping indexes
  sequential.
- **System prompt as text-block array** (the format modern Claude Code
  sends) was silently dropped — now converted like string system prompts.
- **Text blocks sibling to `tool_result` in `role:user` messages** are no
  longer discarded; they become a trailing user message.
- **`tool_choice` mapping**: `any` → `required`, `tool` → forced function,
  `none` → `none` (previously everything degraded to `auto`).
- **Non-streaming responses** now include thinking and `tool_use` content
  blocks and a real `stop_reason` (`tool_use`/`max_tokens`/`end_turn`)
  instead of always `end_turn` with text-only content.
- **Long streams no longer killed at 300s**: reqwest's total timeout covers
  the whole body read — healthy thinking generations over 5 minutes died
  mid-stream. Streams now run under a 1h ceiling with the existing 300s
  idle watchdog; non-streaming JSON keeps the hard 300s cap.
- **401/403 after token reload** surface as `authentication_error` instead
  of `rate_limit_error` with `retry-after: 3600` (a 1h wait on an auth
  problem); auth failures are now terminal in the chain walk too.
- **`count_tokens` respects the 32 MB body cap** like `/v1/messages`.
- **`message_delta` now reports `input_tokens`** from the upstream usage
  chunk alongside `output_tokens`.
- Model `:effort` suffix (`claude-grok-4.7:high`), documented since 0.1.0,
  actually works now — it previously 404'd as an unknown model. Precedence:
  `output_config.effort` > `:effort` suffix > `thinking.budget_tokens`.

### Changed

- `deploy/grok-proxy-cc.service`: removed `Nice=-10` and
  `OOMScoreAdjust=-500` — the unit runs under `systemctl --user`, whose
  manager lacks `CAP_SYS_NICE`/`CAP_SYS_RESOURCE`; negative values aborted
  the start ("Failed at step NICE").
- README: `mkdir -p` for the env-file dir and the binary install step for
  the systemd unit (`ExecStart=%h/.local/bin/grok-proxy-cc`).

## [0.1.0] — 2026-10-16

### Added

- **Initial release**: Anthropic Messages API proxy for Grok (xAI) models.
- Reuses `~/.grok/auth.json` OIDC session from the Grok CLI (no API key).
- 4 models: `grok-4.7`, `grok-4.7-build-fast`, `grok-4.6`, `grok-4.5`.
- Streaming SSE translation: `reasoning_content` → thinking blocks,
  content → text blocks (with explicit `content_block_start`),
  `tool_calls` → tool_use blocks.
- Tool calling: `tool_use` ↔ `tool_calls`, `tool_result` → `role:tool`
  (including tool_result blocks inside `role:user` messages).
- Native effort control: `output_config.effort` (Claude Code slider) →
  `reasoning_effort`; fallback to `thinking.budget_tokens` coarse mapping.
- Same-provider chain fallback: `4.7 → 4.7-build-fast → 4.6 → 4.5`.
- Token auto-recovery: re-reads `~/.grok/auth.json` on upstream 401 and
  retries once — survives the 6-hour OIDC rotation without restart.
- Fail-closed quota: upstream "usage limit"/"spending limit"/"quota" →
  terminal `429 rate_limit_error` with `retry-after: 3600`.
- Loopback-only bind, constant-time Bearer auth, 32 MB body cap.
- Keepalive ping every 5s, idle timeout at 300s.
- Born at maximum optimization: hyper direct (no axum), tokio
  `current_thread`, `futures-core`, `opt-level=3` + LTO fat +
  `codegen-units=1` + `panic=abort` + `strip`, `target-cpu=native`,
  UPX `--best --lzma` → 1.2 MB binary, 11 direct dependencies (zero
  uuid/chrono/anyhow/clap/tracing), `eprintln!` logging.
- 15 unit tests, clippy clean with `-D warnings`.
