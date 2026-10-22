# Setup

Install rustup and Git. The repository's `rust-toolchain.toml` installs Rust `1.99.0` with rustfmt and Clippy. Run commands from the repository root.

The pinned crabber dependency uses an SSH URL. Configure GitHub SSH access, or use the same HTTPS transport rewrite as CI. To leave global Git configuration untouched, prefix dependency-fetching Cargo commands with:

```sh
export GIT_CONFIG_COUNT=1
export GIT_CONFIG_KEY_0=url.https://github.com/mattsp1290/crabber.git.insteadOf
export GIT_CONFIG_VALUE_0=ssh://git@github.com/mattsp1290/crabber.git
```

Build and validate the example:

```sh
cargo build -p crabber-coworkers --release --locked
target/release/crabber-coworkers check-config --config config.example.json
cargo xtask check
```

`check-config` prints each coworker's ID and name separated by a tab. It requires no API key. Copy `config.example.json` to the ignored `config.json` for local edits. Configuration is UTF-8 JSON, limited to 262,144 bytes. Unknown fields are rejected at every level. Validation reports all rule violations together and identifies fields without echoing their values.

| Field | Required/default | Validation |
| --- | --- | --- |
| `coworkers` | Required | 1–32 entries |
| `coworkers[].id` | Required | Unique; 1–64 bytes; lowercase ASCII letters and digits in groups separated by single hyphens |
| `coworkers[].name` | Required | Nonblank after trimming; at most 100 bytes |
| `coworkers[].role` | Required | Nonblank after trimming; at most 200 bytes |
| `coworkers[].instructions` | Required | Nonblank after trimming; at most 16,384 bytes |
| `limits` | Optional; defaults below | Object containing only the following fields |
| `limits.max_prompt_bytes` | 32,768 | 1–1,048,576 bytes |
| `limits.model_turn_seconds` | 120 | 1–3,600 seconds |

Provider and model are fixed to `opencode-go` and `deepseek-v4-flash`; neither is configurable in JSON. Credentials come only from `OPENCODE_GO_API_KEY`. See the [live smoke procedure](manual-smoke.md) to set it and run:

```sh
target/release/crabber-coworkers ask --config config.example.json scout "<prompt>"
```

The prompt must contain non-whitespace text and fit the configured byte limit. Each invocation starts a fresh in-memory session. The answer prints once the run commits, followed by a newline. Live deltas are withheld so a retry cannot duplicate partial text. The agent has no tools and an explicit deny-all policy.

| Outcome / stderr message | Exit code |
| --- | --- |
| Successful config validation or answer | 0 |
| Config read, parse, or validation error | 2 |
| `unknown coworker` | 2 |
| `prompt is empty` | 2 |
| `prompt exceeds max_prompt_bytes` | 2 |
| `OPENCODE_GO_API_KEY is not set` | 2 |
| `agent construction failed` | 1 |
| `model run failed` | 1 |
| `model run did not complete` | 1 |
| `model run produced no answer` | 1 |
| `model turn timed out` | 1 |
| `cannot write output` | 1 |

Config and request validation happen before the API-key check or any model request. A completed run without assistant text produces the no-answer error. The pinned runtime rejects a wholly empty model response as a run failure.

The tool never logs credential values, prompt text, answer text, or raw provider errors. Only the requested committed answer goes to stdout. Errors contain fixed messages or redacted configuration paths and rule names.

Tests need no credentials or database. If a local proxy is set, set `NO_PROXY=127.0.0.1,localhost` for direct test runs; `cargo xtask check` sets it on its test subprocess. The gate checks formatting, Clippy, workspace tests, the release build, the resolved crabber pin, and the example config, in that order. All dependency resolution is locked.
