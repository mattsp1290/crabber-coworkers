# Live smoke tests

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

This CLI live smoke test is optional evidence. It is outside `cargo xtask check` and is not required for the milestone's definition of done. The automated local wire test verifies the request path, authorization header, model, and coworker system prompt; only a live run verifies that the hosted model answers.

For durable history, run two prompts in separate processes using the same name:

```sh
target/release/crabber-coworkers ask --config config.example.json --conversation smoke scout "<first-prompt>"
target/release/crabber-coworkers ask --config config.example.json --conversation smoke scout "<follow-up-prompt>"
target/release/crabber-coworkers list-conversations --config config.example.json scout
```

Confirm the second answer uses the first exchange and `smoke` appears in the list. Whenever running this live smoke, also start another turn on `smoke`, press Ctrl-C during the turn, and confirm exit 130 with stderr `interrupted`. Then run another `ask` on `smoke` and confirm it succeeds. Record the conversation name and outcome only, with prompt and answer redacted.

## Slack live check

Configure the app and environment as described in [Slack setup](slack-setup.md), then run `serve --config config.json`. Send the mentions from your Slack client. API-authored messages can carry `bot_id` even with a user token; the bot filter intentionally ignores them.

1. As an allowlisted user in an allowlisted channel, mention the coworker. Confirm a threaded answer.
2. Mention it inside that thread. Confirm the reply uses the first exchange. Record only the event object's **key names**, such as `type, user, text, ts, channel, event_ts, thread_ts`, never their values.
3. Stop with Ctrl-C. Confirm stderr `stopped` and exit 0.
4. Restart `serve` and mention the coworker in the same thread. Confirm it uses both earlier exchanges.
5. Optionally send an unmentioned thread message and confirm no answer.
6. Optionally have a non-allowlisted user mention it and confirm no answer.
7. Optionally confirm `list-conversations` shows the `slack-…` name.

Steps 1–4 are required evidence (gate G4) before the Slack milestone closes. The fake Slack proves host logic only; it cannot prove the real TLS path or Slack's threaded event shape. In particular, `thread_ts` on threaded mentions needs live confirmation. These steps run outside `cargo xtask check`; steps 5–7 are optional.

Record only outcomes and event key names. Never record token values, real team/channel/user IDs, or message text.

| Date | Commit SHA | Crabber rev | Coworker | Step | Observed result / event key names |
| --- | --- | --- | --- | --- | --- |

A live TLS/Socket Mode probe on 2026-10-23 at `0078fa2ced880c240ac9ea50363f000bf3d46a8c` observed these keys on an API-authored threaded `app_mention`: `app_id, blocks, bot_id, bot_profile, channel, event_ts, parent_user_id, team, text, thread_ts, ts, type, user`. The fake includes this shape and proves it is acknowledged and ignored, even when the user is allowlisted.

Required human-authored steps 1–4 remain pending; the API probe is not a successful G4 record.

Check the manifest template independently: its `bot_events` list must contain only `app_mention`, with no `message.*` event.
