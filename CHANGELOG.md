# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
