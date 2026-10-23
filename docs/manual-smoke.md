# Optional live smoke test

Build the executable following [setup](setup.md). Set the key only in your shell environment, without writing it to a file. Replace the literal placeholder below with your own key (the angle brackets are placeholder notation, not shell syntax):

```text
export OPENCODE_GO_API_KEY=<your-key>
```

Run a prompt against Scout:

```sh
target/release/crabber-coworkers ask --config config.example.json scout "<prompt>"
echo $?
```

Confirm that an answer arrives and the exit code is 0. Record only the observed outcome, with the prompt and answer redacted. Never record the key.

| Date | Commit SHA | Crabber rev | Command (prompt redacted) | Conversation | Exit code | Observed result |
| --- | --- | --- | --- | --- | --- | --- |

This live smoke test is optional evidence. It is outside `cargo xtask check` and is not required for the milestone's definition of done. The automated local wire test verifies the request path, authorization header, model, and coworker system prompt; only a live run verifies that the hosted model answers.

For durable history, run two prompts in separate processes using the same name:

```sh
target/release/crabber-coworkers ask --config config.example.json --conversation smoke scout "<first-prompt>"
target/release/crabber-coworkers ask --config config.example.json --conversation smoke scout "<follow-up-prompt>"
target/release/crabber-coworkers list-conversations --config config.example.json scout
```

Confirm the second answer uses the first exchange and `smoke` appears in the list. Whenever running this live smoke, also start another turn on `smoke`, press Ctrl-C during the turn, and confirm exit 130 with stderr `interrupted`. Then run another `ask` on `smoke` and confirm it succeeds. Record the conversation name and outcome only, with prompt and answer redacted.
