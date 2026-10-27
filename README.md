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

For Kubernetes agent execution, see [running coworkers in borb](docs/borb.md). Slack mentions and replies use the same bot and threads.

See [setup](docs/setup.md) for configuration and [manual smoke testing](docs/manual-smoke.md) for an optional live check.

Coworkers can create, list, read, and revise text files in a persistent workspace. Set `workspace` to `read_write` for those four file tools, `read_only` for read/list, or `disabled` for no tools. Omitted grants default to `disabled`. The example enables Scout and disables Quill.

```sh
target/release/crabber-coworkers ask --config config.example.json scout "Create brief.md with a three-item project checklist. Report the saved relative path."
target/release/crabber-coworkers ask --config config.example.json scout "Read brief.md and show its contents."
target/release/crabber-coworkers ask --config config.example.json scout "Read brief.md, then add an item to review the draft."
```

All authorized users and conversations of a coworker share its files. Local files live in `<state_dir>/workspaces/<coworker-id>/`; borb workers keep them on their own persistent disk. The tools cannot run shell commands, fetch URLs, or publish files. Retrieve files through ordinary read/list requests and the resulting CLI answer or Slack reply. See [workspace access and limits](docs/setup.md#workspace-access-and-limits).

Without `--conversation`, each local `ask` uses fresh in-memory chat history. Workspace files still persist when enabled. With workspace access disabled, an unnamed local `ask` writes nothing to disk. Named conversations persist under `state_dir` as one SQLite file per coworker.

A coworker with a `slack` block answers @mentions from allowlisted users in allowlisted channels, in the thread under the mention. Every turn needs a mention. Thread history is a named conversation under `state_dir` and survives restart. See [Slack setup](docs/slack-setup.md).
