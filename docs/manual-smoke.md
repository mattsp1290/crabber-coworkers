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
| 2026-10-24 | `416526d9655a515568a55965475bfcf9fb97de60` | `a32b1a83bce10649b56498d7967ab3383490aa51` | scout | 1 | Human root mention received a threaded model reply; user confirmed marker retention. |
| 2026-10-24 | `416526d9655a515568a55965475bfcf9fb97de60` | `a32b1a83bce10649b56498d7967ab3383490aa51` | scout | 2 | User confirmed recall within the same thread. Before restart that thread contained 3 user/assistant exchanges. Observed threaded event key names are recorded in the probe below. |
| 2026-10-24 | `416526d9655a515568a55965475bfcf9fb97de60` | `a32b1a83bce10649b56498d7967ab3383490aa51` | scout | 3 | SIGINT produced the fixed stderr line `stopped`; OS process-exit notification confirmed exit 0. |
| 2026-10-24 | `416526d9655a515568a55965475bfcf9fb97de60` | `a32b1a83bce10649b56498d7967ab3383490aa51` | scout | 4 | After restart on the same state, the user confirmed marker recall without repeating values. The same thread advanced from 3 to 4 user/assistant exchanges. |

A live TLS/Socket Mode probe on 2026-10-23 at `0078fa2ced880c240ac9ea50363f000bf3d46a8c` observed these keys on an API-authored threaded `app_mention`: `app_id, blocks, bot_id, bot_profile, channel, event_ts, parent_user_id, team, text, thread_ts, ts, type, user`. The fake includes this shape and proves it is acknowledged and ignored, even when the user is allowlisted.

Required human-authored steps 1–4 passed in the record above, completing gate G4. The API probe separately establishes the transport/event shape and the bot-event rejection boundary; it is not used as evidence of human-authored recall.

Check the manifest template independently: its `bot_events` list must contain only `app_mention`, with no `message.*` event.

## Optional workspace live check

This check uses the real model and Slack. Run it only with separate authorization
for those calls and any worker restart. Use disposable synthetic file contents.
The credential-free tests remain the required automated evidence. This procedure
does not authorize publishing an image or deploying to a cluster.

1. Configure Scout with `workspace: "read_write"` and a second coworker with
   `workspace: "disabled"`. For remote execution, use matching protocol-2 host
   and worker binaries with the approved immutable image digest.
2. From an allowlisted Slack account and channel, start a new root mention asking
   Scout to create a small UTF-8 file named `smoke/brief.md`. Ask it to create the
   parent directory and report the saved relative path. Confirm a threaded reply
   and verify that the file exists on the worker disk.
3. Drain active turns. Gracefully stop and restart the worker on the same disk.
   Start a new root mention asking Scout to list `smoke`, read `smoke/brief.md`,
   and revise one unique phrase. Inspect the worker file to confirm the revision.
   Confirm that remote execution created no workspace on the host.
4. Ask the disabled coworker to create a file. Confirm it has no file tools and
   creates no workspace. Drain work, enable that coworker, and restart the host
   and its worker. Start a new thread and ask it to access Scout's file through a parent path or absolute path. Confirm
   failure and verify that Scout's file remains unchanged.
5. Drain active work and change Scout to `read_only`, then restart the host and
   affected worker. In the existing workspace conversation, confirm a read works
   and a requested write or edit leaves the file unchanged.
6. To check local persistence, use a disposable local configuration with Scout
   set to `read_write`. Run two `ask` commands without `--conversation`, first
   to create a file and then to read it. Confirm that the file persists even
   though neither invocation saves chat history.
7. Restore the intended grants through another drain and restart. Keep any
   synthetic files until you decide to remove them manually. The model has no
   delete tool.

A model's claim that it saved or denied a file is not enough. Inspect the actual
file state without recording its contents. Do not infer rollback from a timeout,
interruption, failed cleanup, or missing Slack reply. A started write can finish
later. Read/list the files before retrying.

Record only outcome flags and code/dependency/image identities. Never record
message text, answer text, file contents, credentials, real Slack IDs, or raw
provider errors.

| Date | Commit SHA | Crabber rev | Worker image digest | Step | Outcome flags |
| --- | --- | --- | --- | --- | --- |

## HTTP API live check

This check uses the real model and needs `OPENCODE_GO_API_KEY`. The credential-free tests prove the routes, replay, restart, and crash settlement against a scripted provider. Only a live run proves the path through the hosted model. The check runs outside `cargo xtask check`.

Use a configuration whose only frontend is the API. Copy `config.example.json` to `config.json` and remove Scout's `slack` block. The `api` block listens on `127.0.0.1:8787`.

1. Generate the token into the environment without printing it. Start `serve` in the background of the same shell, so that the later commands share the token. Confirm stderr `API listening on 127.0.0.1:8787`.

```sh
export CRABBER_COWORKERS_API_TOKEN="$(openssl rand -hex 32)"
target/release/crabber-coworkers serve --config config.json &
```

2. Open a conversation and submit a request. Confirm `201` for the open and `202` with status `accepted` for the submission.

```sh
api=http://127.0.0.1:8787/v1
auth="Authorization: Bearer $CRABBER_COWORKERS_API_TOKEN"
json='Content-Type: application/json'
conversation=$(uuidgen | tr 'A-F' 'a-f')
request=$(uuidgen | tr 'A-F' 'a-f')
curl -sS -o /dev/null -w '%{http_code}\n' -H "$auth" -H "$json" -d "{\"conversationId\":\"$conversation\",\"coworkerId\":\"scout\"}" "$api/conversations"
curl -sS -H "$auth" -H "$json" -d "{\"requestId\":\"$request\",\"text\":\"<prompt>\"}" "$api/conversations/$conversation/requests"
```

3. Poll the receipt until its status is `completed`.

```sh
curl -sS -H "$auth" "$api/conversations/$conversation/requests/$request"
```

4. Resend the submission from step 2 unchanged. Confirm `202` and the same `completed` receipt.
5. Submit a long prompt under a new `requestId`. While its status is `running`, stop `serve` with `kill -TERM %1`. Confirm stderr `stopped` and exit code 0. Start `serve` again and read the receipt. Expect `failed` with reason `interrupted`. If the request completed, repeat the step with a longer prompt.
6. Optionally check crash settlement. Submit another long prompt under a new `requestId`. While its status is `running`, stop `serve` with `kill -KILL %1`, and start it again. Poll the receipt for up to 60 seconds. Expect `uncertain` with reason `process_stopped`. If the kill came before the turn started, the request runs again and ends `completed`. Then submit a short request in the same conversation and confirm that it completes.

Record the commit, the commands, the HTTP status codes, and each receipt's `status` and `reason`. Never record the token, the prompt text, or `result.text`.

| Date | Commit SHA | Crabber rev | Step | Observed result |
| --- | --- | --- | --- | --- |
| 2026-11-15 | `9c46c79cc9a113667940fa1c3b4e6ab205c43682` | `a32b1a83bce10649b56498d7967ab3383490aa51` | Steps 1 to 6 | Not run. No `OPENCODE_GO_API_KEY` was available. The live model path is unverified. |
| 2026-11-15 | `9c46c79cc9a113667940fa1c3b4e6ab205c43682` | `a32b1a83bce10649b56498d7967ab3383490aa51` | Credential-free loopback check | Passed. The observed results are listed below. |

The credential-free loopback check ran the release binary against a temporary `state_dir` on a free loopback port, with a token from `openssl rand -hex 32` that was never printed. `OPENCODE_GO_API_KEY` held a placeholder so that `serve` would start. No request was accepted, so no model call was made. It observed:

- Each startup refusal exited 2 with its fixed message. The refusals were a missing token, a short token, a token with a space, a non-loopback `api.listen`, and a configuration with no frontend. After the missing token and the short token, `state_dir` did not exist.
- `serve` logs `API listening on 127.0.0.1:<port>`.
- Without the token, a documented route, an unknown path, a wrong method, and an invalid body each return `401 unauthorized` with `WWW-Authenticate: Bearer` and no `Allow` header. A wrong token returns the same response.
- `health` returns `200` with `ready` and `outstandingAdmissions` 0. `capabilities` returns `200` with backend `local`. `listCoworkers` returns `200`.
- `openConversation` returns `201` with an empty snapshot, `201` again on repeat, `409 conflict` for another coworker, `404 not_found` for an unknown coworker, `400 invalid_input` for an uppercase UUID or an unknown field, and `400 invalid_json` for malformed JSON.
- `listConversations` and `conversation` return `200`. An unknown request returns `404 not_found` for status and cancel. A query parameter on `health`, `limit=0`, and a blank `text` each return `400 invalid_input`.
- A wrong method returns `405 method_not_allowed` with `Allow` set to `GET`, `POST`, or `GET,POST`. `HEAD` returns `405`. `POST /v1/conversations/{conversationId}/events` returns `404 not_found`.
- The state directory has mode `0700`. `api.sqlite` and `api.lock` have mode `0600`.
- A second `serve` on the same `state_dir` exits 1 with `API store is in use`.
- SIGTERM and SIGINT each log `stopped` and exit 0. The conversation is still present after a restart.
- The token appears in neither the stderr output nor the state files.
