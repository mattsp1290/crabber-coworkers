# crabber-coworkers

A Rust port of the OpenDots idea of always-on AI coworkers, built on crabber. It answers prompts from configured coworkers on the command line and Slack, with durable conversations.

```sh
cargo build -p crabber-coworkers --release --locked
target/release/crabber-coworkers check-config --config config.example.json
target/release/crabber-coworkers ask --config config.example.json scout "<prompt>"
target/release/crabber-coworkers ask --config config.example.json --conversation weekly scout "<prompt>"
target/release/crabber-coworkers list-conversations --config config.example.json scout
target/release/crabber-coworkers serve --config config.json
cargo xtask check
```

Set `OPENCODE_GO_API_KEY` in your environment before using `ask`. The provider is OpenCode Go and the model is `deepseek-v4-flash`.

See [setup](docs/setup.md) for configuration and [manual smoke testing](docs/manual-smoke.md) for an optional live check.

Not yet: tools and per-coworker workspaces.

Without `--conversation`, each `ask` uses a fresh in-memory session and writes nothing to disk. Named conversations persist under `state_dir` as one SQLite file per coworker.

A coworker with a `slack` block answers @mentions from allowlisted users in allowlisted channels, in the thread under the mention. Every turn needs a mention. Thread history is a named conversation under `state_dir` and survives restart. See [Slack setup](docs/slack-setup.md).
