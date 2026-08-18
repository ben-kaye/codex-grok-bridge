# Codex Grok Bridge

Run native OpenAI models and Grok 4.6 simultaneously from your Codex GUI.

## Install

Requires macOS, `/Applications/ChatGPT.app`, authenticated Grok Build `1.0.x`,
and Rust 1.85 or newer.

```sh
git clone https://github.com/ben-kaye/codex-grok-bridge.git
cd codex-grok-bridge
cargo build --release --locked
```

The local build is ad-hoc signed by the macOS linker. To sign it explicitly,
use `-` for an ad-hoc signature or a certificate name from your Keychain:

```sh
codesign --force --sign - target/release/codex-grok-bridge
# codesign --force --sign "Developer ID Application: Your Name" \
#   target/release/codex-grok-bridge
codesign --verify --strict target/release/codex-grok-bridge
```

Quit ChatGPT/Codex if it is running, then launch:

```sh
./bin/launch-codex-grok
```

The launcher also builds the release binary if it is missing. It leaves the
signed ChatGPT application untouched and selects the bridge through
`CODEX_CLI_PATH`. For non-default locations, set `CODEX_GROK_APP`,
`CODEX_GROK_GROK`, or `CODEX_GROK_NATIVE_CODEX` before launching.

Grok models appear under the `grok/` namespace. Current compatibility is Codex
CLI `0.148.x` and Grok Build `1.0.x`; unsupported versions fail at startup.

## Verify

```sh
cargo test --locked
./bin/smoke-test
```

Grok-only routing state is stored under `$CODEX_HOME/grok-bridge/`; Grok
history and the task/session mapping are stored under
`~/.codex-acp-gateway/`. OpenAI-model tasks, including Sol tasks, are passed
through and stored solely by bundled Codex in its native format. They remain
readable and resumable when the unmodified Codex app is launched without this
bridge.

Protocol translation is derived from
[`mmonad/codex-acp-gateway`](https://github.com/mmonad/codex-acp-gateway) at
the commit recorded in `NOTICE`, under Apache-2.0. Grok Build's ACP endpoint is
implemented by [`xai-acp-lib`](https://github.com/xai-org/grok-build/tree/main/crates/codegen/xai-acp-lib).
