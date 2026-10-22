# crabber-coworkers

A Rust port of the OpenDots idea of always-on AI coworkers, built on crabber. Today it answers one prompt from one configured coworker on the command line.

```sh
cargo build -p crabber-coworkers --release --locked
target/release/crabber-coworkers check-config --config config.example.json
target/release/crabber-coworkers ask --config config.example.json scout "<prompt>"
cargo xtask check
```

Set `OPENCODE_GO_API_KEY` in your environment before using `ask`. The provider is OpenCode Go and the model is `deepseek-v4-flash`.

See [setup](docs/setup.md) for configuration and [manual smoke testing](docs/manual-smoke.md) for an optional live check.

Not yet: Slack, durable history, tools, and per-coworker workspaces.
