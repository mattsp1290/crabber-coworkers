# Setup

Install rustup and Git. The repository's `rust-toolchain.toml` installs Rust `1.99.0` with rustfmt and Clippy. Run commands from the repository root. SQLite is bundled and needs a C compiler: macOS clang or `build-essential` on Debian and Ubuntu.

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
| `state_dir` | Required | Nonblank; at most 4,096 bytes; no control characters. Relative paths resolve against the working directory. `check-config` does not create it. |
| `coworkers` | Required | 1–32 entries |
| `coworkers[].id` | Required | Unique; 1–64 bytes; lowercase ASCII letters and digits in groups separated by single hyphens |
| `coworkers[].name` | Required | Nonblank after trimming; at most 100 bytes; no control characters |
| `coworkers[].role` | Required | Nonblank after trimming; at most 200 bytes; no control characters |
| `coworkers[].instructions` | Required | Nonblank after trimming; at most 16,384 bytes |
| `coworkers[].slack` | Optional | Object containing only the following three Slack fields |
| `coworkers[].slack.team_id` | Required with Slack | `T` followed by 1–31 uppercase ASCII letters or digits |
| `coworkers[].slack.allowed_channel_ids` | Required with Slack | 1–64 unique IDs; `C` or `G` followed by 1–31 uppercase ASCII letters or digits |
| `coworkers[].slack.allowed_user_ids` | Required with Slack | 1–256 unique IDs; `U` or `W` followed by 1–31 uppercase ASCII letters or digits |
| `limits.max_concurrent_turns` | 4 | 1–32 model turns across all Slack coworkers |
| `limits` | Optional; defaults below | Object containing only the following fields |
| `limits.max_prompt_bytes` | 32,768 | 1–1,048,576 bytes |
| `limits.model_turn_seconds` | 120 | 1–3,600 seconds |

Provider and model are fixed to `opencode-go` and `deepseek-v4-flash`; neither is configurable in JSON. The model credential comes only from `OPENCODE_GO_API_KEY`. See the [live smoke procedure](manual-smoke.md) to set it and run:

```sh
target/release/crabber-coworkers ask --config config.example.json scout "<prompt>"
```

The prompt must contain non-whitespace text and fit the configured byte limit. Without `--conversation`, each invocation starts a fresh in-memory session and writes nothing to disk. The answer prints once the run commits, followed by a newline. Live deltas are withheld so a retry cannot duplicate partial text. The agent has no tools and an explicit deny-all policy.

| Outcome / stderr message | Exit code |
| --- | --- |
| Successful config validation or answer | 0 |
| `cannot read configuration file` | 2 |
| `configuration file exceeds 262144 bytes` | 2 |
| `configuration file must be UTF-8` | 2 |
| `invalid configuration JSON at line L, column C (category)` | 2 |
| `state_dir must be non-empty and at most 4096 bytes` | 2 |
| `state_dir must not contain control characters` | 2 |
| `coworkers must contain 1–32 entries` | 2 |
| `coworkers[i].id must be lowercase alphanumeric groups separated by single hyphens, at most 64 bytes` | 2 |
| `coworkers[i].id must be unique` | 2 |
| `coworkers[i].field must not contain control characters` (`name`, `role`) | 2 |
| `coworkers[i].field must be non-empty and at most N bytes` (limits above) | 2 |
| `limits.max_prompt_bytes must be in 1..=1048576` | 2 |
| `limits.model_turn_seconds must be in 1..=3600` | 2 |
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
| `invalid conversation name` | 2 |
| `cannot open conversation store` | 1 |
| `cannot open conversation` | 1 |
| `conversation is busy` | 1 |
| `interrupted` | 130 |
| `cannot list conversations` | 1 |
| `cannot install signal handlers` | 1 |

Config and request validation happen before the API-key check or any model request. A completed run without assistant text produces the no-answer error. The pinned runtime rejects a wholly empty model response as a run failure.

The tool never logs credential values, prompt text, answer text, or raw provider errors. Only the requested committed answer goes to stdout. Errors contain fixed messages or redacted configuration paths and rule names.

Tests need no credentials or database server. They use temporary SQLite files. If a local proxy is set, set `NO_PROXY=127.0.0.1,localhost` for direct test runs; `cargo xtask check` sets it on its test subprocess. The gate checks formatting, Clippy, workspace tests, the release build, the resolved crabber pin, and the example config, in that order. All dependency resolution is locked.

For `ask`, `limits.model_turn_seconds` starts before the store is opened and covers store open, session creation, admission, every provider retry, and reading the answer. Admission itself is not cancellable; if it finishes after the deadline the run is interrupted at once. On timeout, SIGINT, SIGTERM, or SIGHUP the run is interrupted and the process waits up to 5 seconds for it to settle, so the conversation stays usable. Signal exits use code 130 with `interrupted`. `serve` and `ask` install signal handlers; `check-config` and `list-conversations` keep the default behavior. If the answer completed in the same instant a signal arrived, the answer is printed and the exit code is 0.

## Conversations

Start or continue a conversation with `--conversation <name>`. Names follow the coworker-ID rule above. The system prompt is rebuilt from the current config each turn, so editing instructions affects existing conversations.

```sh
target/release/crabber-coworkers ask --config config.example.json --conversation weekly scout "<prompt>"
target/release/crabber-coworkers list-conversations --config config.example.json scout
```

Each coworker uses `<state_dir>/<coworker-id>.sqlite`, created on first use. Missing directory components are created with mode `0700` on Unix; the database file is created at `0600` and set to `0600` on every use. An existing directory keeps its mode. Conversation text and the store's internal signing secret are stored in plaintext; the directory and file modes are the only protection. Do not point `state_dir` at a path tracked by git; only `/state/` is ignored.

`list-conversations` prints `name<TAB>updated-at` lines with RFC 3339 UTC timestamps. It needs no API key and prints nothing when the coworker has no file. It does not migrate the database or change permissions and works while a turn is running.

There is no per-conversation delete yet: crabber's session-owner cleanup protocol currently prevents deletion after a session has run a prompt. To remove a coworker's conversations, remove its `.sqlite`, `.sqlite-wal`, and `.sqlite-shm` files while no `ask` is running.

A hard crash (SIGKILL, power loss, or a panic) during a turn leaves that conversation reporting `conversation is busy`. Use a new conversation name or remove the coworker's file. Automatic recovery waits on a crabber reconciliation API. A crabber pin bump that changes the SQLite schema version makes existing files unusable (`cannot open conversation store`); archive or remove them before upgrading.

Prompts are currently a command-line argument. Operating-system argument limits can be smaller than `max_prompt_bytes`; for example, Linux commonly limits one argument to about 128 KiB. Such an oversized argument is rejected by the OS before the CLI runs.


## Serving Slack

Follow [Slack setup](slack-setup.md) to create one app per coworker. Slack credentials come only from environment variables:

| Variable | Token type |
| --- | --- |
| `SLACK_BOT_TOKEN_<ID>` | `xoxb-` bot token |
| `SLACK_APP_TOKEN_<ID>` | `xapp-` app-level token |

`<ID>` is the coworker ID uppercased with hyphens replaced by underscores: `code-review-2` becomes `CODE_REVIEW_2`. Rotating `xoxe.xapp-` tokens are rejected.

`serve --config config.json` validates configuration, checks the model key and every Slack token variable, installs signal handlers, checks every bot identity with `auth.test` and opens its Socket Mode URL with `apps.connections.open`, opens stores, then connects sockets. Missing inputs exit 2 before network or disk creation; rejected Slack tokens exit 1 before store creation. A stop during startup exits 130 with `interrupted`.

Only allowlisted `app_mention` events are answered. Unmentioned thread messages, other teams, users or channels, bot-authored events, and subtypes are acknowledged and ignored silently. A thread uses `slack-<channel lowercased>-<root timestamp with dots replaced by hyphens>`, for example `slack-c0000000001-1515449522-000016`. An explicit `thread_ts` continues the root's history; a root mention starts a new conversation.

Each thread has a queue of 4 waiting mentions plus its active turn. Overflow gets the busy notice below at most once per thread per 30 seconds. Turns in a thread run in arrival order. `limits.max_concurrent_turns` caps model turns globally; the permit is released before delivery or a rate-limit wait. The model-turn deadline starts after the permit is acquired.

Answers are escaped and split into at most 8 chunks of 3,500 Unicode scalar values, keeping escaped entities whole and preferring newlines. If more is needed, the last chunk ends with `[answer truncated]`. Slack rate limits retry a chunk once, after a wait capped at 30 seconds. Other delivery failures produce only a fixed log message.

SIGINT, SIGTERM, or SIGHUP stops intake, drops queued mentions, interrupts active turns, and waits up to 20 seconds for shutdown. A normal stop exits 0 with `stopped`. A fatal socket failure uses the same shutdown routine and exits 1. Answers go only to Slack; `serve` writes no stdout.

| Fixed message | Exit code / destination |
| --- | --- |
| `coworkers[i].slack.team_id must be a Slack team ID` | 2 |
| `coworkers[i].slack.allowed_channel_ids must contain 1–64 unique Slack channel IDs` | 2 |
| `coworkers[i].slack.allowed_user_ids must contain 1–256 unique Slack user IDs` | 2 |
| `limits.max_concurrent_turns must be in 1..=32` | 2 |
| `no coworker has a slack block` | 2 |
| `<VAR> is not set` | 2 |
| `<VAR> has the wrong token type` | 2 |
| `Slack authentication failed for <id>` | 1 |
| `Slack connection stopped for <id>` | 1 |
| `interrupted` | 130 during startup |
| `stopped` | 0 after serving |
| `Slack connected for <id>` | stderr log |
| `Slack reconnecting for <id>` | stderr log |
| `Slack delivery failed for <id>` | stderr log |
| `Slack event ignored for <id>` | stderr log |
| `Slack worker failed` | stderr log; worker task ended unexpectedly |
| `Slack shutdown timed out` | stderr log; remaining tasks aborted |

The transport's library errors are fixed categories: `Slack authentication failed`, `Slack rate limited`, `Slack request failed`, `Slack rejected the request`, and `Slack disabled the Socket Mode link`. The daemon maps them to the coworker-specific messages above. Slack notices are listed in [Slack setup](slack-setup.md).

Acknowledged mentions awaiting an answer are lost if `serve` crashes. Deduplication holds only the last 1,024 accepted event IDs in memory per coworker; redelivery after a restart can be answered twice. A chunk with an unknown delivery outcome is never retried, so a reply can have a missing tail. SIGKILL during a model turn can leave the thread's conversation busy with no automatic recovery; a new root mention starts a usable conversation.

An operator-named conversation starting with `slack-` can collide with a generated name. Using a thread's conversation simultaneously through `serve` and `ask --conversation` reports busy to whichever comes second. When two coworkers are mentioned under one root message, both answer there and keep separate histories in their respective coworker stores.

Stopping cancels delivery, including a rate-limit wait, without logging a delivery failure. An answer already committed to history may therefore be undelivered or only partly delivered when the process stops. A shutdown that exceeds the 20-second budget aborts remaining tasks and can leave a conversation busy, like a hard crash.
