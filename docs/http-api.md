# HTTP integration API v1

`serve` hosts an authenticated JSON API when the configuration has an `api` block. A consumer opens a conversation with a coworker, submits a request, and polls the request's receipt until the request ends. This document is the consumer contract. For configuration, startup, and operator messages, see [serving the HTTP API](setup.md#serving-the-http-api).

## Scope

Every `/v1` route is in [`openapi/crabber-coworkers.v1.json`](../openapi/crabber-coworkers.v1.json). That OpenAPI 3.1 document is generated from the types that the server serializes. Generate clients from it. Run `cargo xtask openapi` to regenerate the document. `cargo xtask check` fails when the committed document is stale.

The API does not cover the following.

- **AG-UI observation** (`POST /v1/conversations/{conversationId}/events`) is deferred, and the path returns `404 not_found`. The maintained AG-UI projection is crabber's `crabber-agui` crate. This repository cannot use that crate at the pinned revision without a `[patch]` section, which it forbids. There is no event stream until crabber makes the crate consumable. Poll the receipt instead.
- **Human decisions.** Coworkers never pause for approval. There is no `/human` route and no `waiting` status.
- **Coworker creation.** Coworkers come from `config.json` and load at startup.
- **Routines, memories, handoffs, instruction packs, usage, budgets, webhooks, and audit.** This repository has none of them.

The server does not terminate TLS, serve the OpenAPI document over HTTP, or send CORS headers. There is no client library.

## Authentication and trust

One bearer token authorizes every route. `serve` reads the token once at startup from `CRABBER_COWORKERS_API_TOKEN` and keeps only its SHA-256 digest. The token must be at least 32 bytes of printable ASCII with no spaces. To change the token, restart `serve`.

Send exactly one `Authorization: Bearer <token>` header with each request.

No route is unauthenticated, including health and capabilities. A request without the valid token gets `401 unauthorized` with `WWW-Authenticate: Bearer`. The server sends that response before it routes the request, parses the query, or reads the body. An unknown path, a wrong method, and an invalid body therefore all return `401` without the token, and that `401` carries no `Allow` header. A `401` shows only that the server is listening.

The token is an operator credential:

- It drives every configured coworker with that coworker's workspace grant, including `read_write`.
- Slack `allowed_user_ids` and `allowed_channel_ids` do not apply to the API.
- It reads the messages of every API conversation.

The token travels in plaintext HTTP. `api.listen` must be a loopback address unless `api.allow_remote` is `true`. Set `api.allow_remote` only behind a proxy that terminates TLS. The server does not check that the proxy exists.

## Conventions

- Every response is `application/json` with `Cache-Control: no-store`.
- Every success body carries `apiVersion: "1"`. Read `GET /v1/capabilities` first and reject an `apiVersion` or `requestContract` that your client does not support. The current `requestContract` is `sqlite-outbox-keyed-admission-v1`.
- The only error body is `{"error": "<code>"}`. See [error codes](#error-codes). No error body carries prompt text, answer text, or a raw provider or database error.
- `conversationId` and `requestId` are client-chosen UUIDs in lowercase hyphenated form. Any other form is `400 invalid_input`. On macOS, `uuidgen` prints uppercase, so pipe it through `tr 'A-F' 'a-f'`.
- A request body is one JSON object. An unknown, missing, or duplicate field is `400 invalid_input`. Anything other than one JSON object is `400 invalid_json`.
- Query parameters are strict. Only `GET /v1/conversations` accepts any. An unknown or repeated parameter is `400 invalid_input`.
- A body larger than `limits.maxPromptBytes * 6 + 4096` bytes is `413 payload_too_large`. The limits are in `GET /v1/capabilities`.
- A documented path with another method returns `405 method_not_allowed`. Its `Allow` header names only the documented methods. `HEAD` is not served.

## Routes

| Operation | Method and path | Success | Behavior |
| --- | --- | --- | --- |
| `capabilities` | `GET /v1/capabilities` | `200` | Contract identifiers, the backend (`local` or `borb`), the status and reason vocabularies, and the limits in force: `maxPromptBytes`, `maxQueuedPerConversation`, `modelTurnSeconds`, and `maxPageSize`. |
| `health` | `GET /v1/health` | `200` or `503` | Readiness of the gateway store. See [health](#health). |
| `listCoworkers` | `GET /v1/coworkers` | `200` | Every configured coworker in configuration order: `coworkerId`, `name`, `role`, `workspace`, whether it has a `slack` block, and the byte length and SHA-256 digest of its instructions. Never the instruction text or Slack IDs. |
| `openConversation` | `POST /v1/conversations` | `201` | Body `{"conversationId", "coworkerId"}`. Returns the conversation snapshot. A new conversation has no messages. Repeating the call with the same coworker returns `201` with the current snapshot. Another coworker is `409 conflict`. A coworker that is not configured is `404 not_found`. The call creates no model session. The first request does. |
| `listConversations` | `GET /v1/conversations` | `200` | Conversations opened through this API, in ascending `conversationId` order, which is not chronological. See [listing](#listing). |
| `conversation` | `GET /v1/conversations/{conversationId}` | `200` | The conversation snapshot. See [snapshots](#snapshots). |
| `submit` | `POST /v1/conversations/{conversationId}/requests` | `202` | Body `{"requestId", "text"}`. Returns the request's receipt. See [identity](#identity-and-equivalence) and [acceptance](#the-acceptance-boundary). |
| `requestStatus` | `GET /v1/conversations/{conversationId}/requests/{requestId}` | `200` | The request's receipt. A pure read that never starts a turn. |
| `cancel` | `POST /v1/conversations/{conversationId}/requests/{requestId}/cancel` | `202` | Body must be `{}`. An empty body is `400 invalid_json`. Returns the resulting receipt. See [cancellation](#cancellation). |

An unknown conversation or request is `404 not_found` on every route that names one.

### Health

`GET /v1/health` probes `api.sqlite` on a fresh connection, bounded to 2 seconds. It returns `200` when the store answers and `503` with the same body shape when it does not. A dependency failure appears in the body, never as an error code.

```json
{"apiVersion":"1","status":"ready","dependencies":{"database":{"status":"ready"}},"outstandingAdmissions":0}
```

`status` is `ready` or `not_ready`. The database status is `ready` or `unavailable`. `outstandingAdmissions` counts the turns whose admission crabber has not answered. See [outstanding admissions](#outstanding-admissions). The count does not affect `status`. Health does not probe the model provider, Slack, or borb.

### Listing

`GET /v1/conversations` accepts three optional parameters.

| Parameter | Rule |
| --- | --- |
| `limit` | Page size from 1 to 100, in decimal without a sign or leading zeros. The default is 50. |
| `after` | The previous page's `next`, unchanged. The cursor is opaque and does not carry the other parameters. Send the same `limit` and `coworkerId` with it. |
| `coworkerId` | Only this coworker's conversations. A coworker that is not configured yields an empty page. |

`next` is `null` on the last page. Each entry carries `conversationId`, `coworkerId`, and `phase`. `phase` is `running` while any request in the conversation is `accepted`, `running`, or `cancellation_requested`. Otherwise it is `idle`.

### Snapshots

A snapshot holds the conversation's `state` and the `messages` of its API requests, oldest first. Each request contributes a `user` message with its text. A `completed` request also contributes an `assistant` message with its answer. Both carry the `requestId`.

The snapshot covers the newest 100 requests and at most 4 MiB of JSON. The server drops whole requests, oldest first, to fit, and sets `truncated` to `true`. Turns taken through the CLI or Slack do not appear.

### Receipts

```json
{"apiVersion":"1","conversationId":"<uuid>","requestId":"<uuid>","acceptance":{"boundary":"sqlite_outbox"},"status":"completed","result":{"text":"<answer>"}}
```

`result` appears only when `status` is `completed`. `reason` appears only when `status` is `failed` or `uncertain`. `cancellation.requestedAt` appears once a cancellation was recorded, as an RFC 3339 UTC time.

## Error codes

| Status | Code | When |
| --- | --- | --- |
| 400 | `invalid_json` | The body is not one JSON object. |
| 400 | `invalid_input` | A field, path ID, or query parameter is invalid. The `text` is blank or contains NUL. |
| 401 | `unauthorized` | The bearer token is missing or wrong. |
| 404 | `not_found` | The path, coworker, conversation, or request is unknown. A submission to a conversation whose coworker was removed from the configuration also gets this code. |
| 405 | `method_not_allowed` | The path is documented and the method is not. |
| 409 | `conflict` | The conversation exists with another coworker, or the request exists with other text. |
| 409 | `queue_full` | The conversation already has `limits.maxQueuedPerConversation` unfinished requests, counting the running one. |
| 413 | `payload_too_large` | The body exceeds the body limit, or `text` exceeds `limits.maxPromptBytes`. |
| 500 | `internal` | A server defect. |
| 503 | `retry_same_request` | The outcome of an `openConversation`, `submit`, or `cancel` is unknown. `submit` also gets this code while the service is stopping. `openConversation` and `cancel` do not check for a stop. |
| 503 | `unavailable` | A read could not be completed. Repeating it is safe. |

The OpenAPI document lists every error status on every operation. This table says which codes the server sends.

## Identity and equivalence

A request's identity is `(conversationId, requestId)`. Its `text` is compared byte for byte.

- Resending the same identity with the same text returns `202` with the current receipt and never starts a second turn. The receipt can already be `completed`.
- Reusing the identity with other text is `409 conflict`.
- Never generate a new `requestId` to resolve an unknown outcome. A new `requestId` is a new request, and the coworker can run the work twice.

The server validates a resend against the current configuration before it looks up the retained receipt. After a restart with a lower `limits.maxPromptBytes`, an exact resend can get `413 payload_too_large`. After the coworker is removed, an exact resend gets `404 not_found`. Neither rejection shows that nothing ran. Read the original outcome with `requestStatus`, and keep the original identity.

`text` must contain a non-whitespace character and no NUL. It must be at most `limits.maxPromptBytes` bytes of UTF-8.

## The acceptance boundary

`202` with `status` `accepted` means that the request is committed in `<state_dir>/api.sqlite` and will run after the conversation's earlier requests. `acceptance.boundary` is `sqlite_outbox`. Acceptance does not mean that the turn started, that the model was reached, or that a workspace file changed.

Requests in one conversation run one at a time, in submission order. Conversations run concurrently up to the operator's `max_concurrent_turns` setting, which `capabilities` does not report. The API shares that cap with Slack. The turn deadline, `limits.modelTurnSeconds`, starts when a request's turn starts, not when the request is accepted.

## Statuses

| Status | Terminal | Meaning |
| --- | --- | --- |
| `accepted` | No | Durably queued. No turn started. |
| `running` | No | The dispatcher started the request's turn. |
| `cancellation_requested` | No | A cancellation is recorded for a running turn that has not ended. |
| `completed` | Yes | The committed answer is in `result.text`. A later cancellation does not change it. |
| `cancelled` | Yes | Cancelled before the turn started, or the cancellation interrupted the running turn. |
| `failed` | Yes | The turn ended without an answer. Its tools may have run first. See `reason`. |
| `uncertain` | Yes | The gateway cannot establish whether the turn committed. See `reason`. |

A terminal receipt never changes. A `running` request can return to `accepted` when a stop or crash came before crabber admitted the turn.

## Failure reasons

`reason` accompanies `failed` and `uncertain`. The status column gives the status with the local backend. The [borb backend](#borb-backend) reports some of these reasons as `uncertain`.

| Reason | Status | Meaning |
| --- | --- | --- |
| `conversation_busy` | `failed` | Another frontend or an unsettled run holds the conversation's session. The turn did not run. |
| `timeout` | `failed` | The turn exceeded `limits.modelTurnSeconds`. Its tools may have run first. |
| `interrupted` | `failed` | A graceful stop interrupted the turn. Its tools may have run first. |
| `workspace_unavailable` | `failed` | The coworker's workspace could not be admitted, or it no longer matches the conversation's session. After an operator enables or disables a coworker's workspace, open a new conversation. See [conversations](setup.md#conversations). |
| `store_unavailable` | `failed` | The coworker's conversation store or session could not be opened. |
| `agent_unavailable` | `failed` | The agent could not be built. |
| `cleanup_failed` | `failed` | The run ended, and cleanup did not confirm that its work had stopped. The answer may have committed, and its tools may have run. No result is returned. |
| `model_failed` | `failed` | The model run failed or did not complete. Its tools may have run first. |
| `no_answer` | `failed` | The run completed without assistant text. Its tools may have run. |
| `invalid_request` | `failed` | Part of the vocabulary. Submit validation rejects such a request before it is accepted. |
| `configuration_changed` | `uncertain` | The coworker's instructions or behavior version changed after crabber first admitted this request, and a later attempt to run or settle it was refused. |
| `coworker_removed` | `failed` or `uncertain` | The conversation's coworker is no longer configured. A request that never started is `failed`. A request that had started is `uncertain`. |
| `process_stopped` | `uncertain` | An earlier `serve` process ended while the turn was unsettled. |

## Failures and retries

- **Transport loss or `503 retry_same_request` on a command.** The outcome is unknown. Read the same identity with `requestStatus`, or `conversation` for an open. Then resend the identical body. A `404` on that read is not proof of absence, because the first send can still commit. Resending the identical body is always safe.
- **`503 unavailable` on a read.** Repeat the read.
- **`409 queue_full`.** The request was not recorded. Wait for an earlier request to end and resend it.
- **`409 conflict`.** The identity is already bound to other text or another coworker. Do not retry. Fix the client.
- **`failed`, and the turn ran nothing.** The reasons are `conversation_busy`, `workspace_unavailable`, `store_unavailable`, `agent_unavailable`, and `invalid_request`, and `coworker_removed` on a `failed` receipt. The `requestId` is spent. To run the work, submit it under a new `requestId`.
- **`failed`, and the turn may have run.** The reasons are `timeout`, `interrupted`, `model_failed`, `no_answer`, and `cleanup_failed`. The `requestId` is spent. The turn's tools may have run, and a `read_write` workspace may hold their file writes. `cleanup_failed` can also follow a committed answer. Inspect as for `uncertain` before you submit under a new `requestId`.
- **`uncertain`.** The turn may have reached the model, and its tools may have run. The answer, if one committed, is not in the receipt. Inspect before you replay. Read or list the coworker's workspace files through a new request, and decide whether a repeat is safe. Then submit under a new `requestId`. After `process_stopped`, the conversation's session can stay busy. If the new request fails with `conversation_busy`, open a new conversation and inspect from there.

## Cancellation

`cancel` records durable intent and returns the resulting receipt.

- An `accepted` request becomes `cancelled` and never runs.
- A `running` request becomes `cancellation_requested`, and the server interrupts its turn. `cancel` records the intent and does not decide the outcome.
- If the interruption takes effect, the request ends `cancelled`.
- A completed answer wins. If the answer committed before the interruption took effect, the request ends `completed` with its result.
- A failure wins in the same way. If the model run failed before the interruption took effect, the request ends `failed` with `model_failed`.
- A request ends `uncertain` when the gateway cannot establish what the turn did. See [crash](#crash) and the [borb backend](#borb-backend).
- A request that already ended keeps its receipt unchanged.
- Repeating `cancel` returns the current receipt and keeps the first `requestedAt`.

After `cancel`, poll `requestStatus` until the status is terminal. Do not wait for `cancelled`.

Cancellation does not roll back workspace file effects. A file write that started before the interruption can finish.

## Stops, restarts, and crashes

Only one `serve` with an `api` block can use a `state_dir`. Terminal receipts survive every case below, and a `completed` receipt returns the same `result.text` after a restart.

### Graceful stop

SIGINT, SIGTERM, or SIGHUP stops the service.

- Intake closes. The listener stops accepting connections, and open connections get 3 seconds to finish. A submission that is already in flight can still commit and return `202`, can get `503 retry_same_request`, or can lose its response. A later attempt fails to connect until the next start. Treat the `503`, the lost response, and the failed connection as an unknown outcome. After the next start, resend the same identity or poll its receipt, as [failures and retries](#failures-and-retries) describes.
- `serve` interrupts running turns. Each ends `failed` with `interrupted`, unless its answer committed first (`completed`) or its cancellation was requested (`cancelled`). Crabber keeps the interrupted run under the request's key, so the same `requestId` can never run again. To run the work again, follow [failures and retries](#failures-and-retries). A turn that `serve` does not see end before it exits leaves its request `running`. The next start settles that request as after a [crash](#crash), and the outcome can be `uncertain` with `process_stopped`.
- `accepted` requests stay `accepted` and run after the next start. That includes a request that was waiting for a free turn.

Shutdown has a 20-second budget. If the budget runs out, `serve` logs `serve shutdown timed out` and exits. The next start settles any request that is still `running`, as after a crash.

### Renewal

SIGUSR1 ends the process without interrupting work. Intake closes as in a graceful stop. Running turns finish and settle normally. No other request starts, and queued requests stay `accepted` for the next process. `serve` exits 0 when the running turns end.

The API is down from the signal until a replacement `serve` starts. `serve` holds `api.lock` until its running turns end, so a replacement that starts earlier exits 1 with `API store is in use`. That wait can last `limits.modelTurnSeconds`, and longer while an admission is outstanding.

### Crash

A crash is SIGKILL, power loss, a panic, or a shutdown that timed out. On the next start, `serve` settles each request that the crash left `running` or `cancellation_requested` before it runs that conversation's later requests. Other conversations are not held up. The outcome follows what crabber durably holds for the request.

| Crabber state | Outcome |
| --- | --- |
| No admission committed | The request returns to `accepted` and runs. If cancellation was requested, it ends `cancelled`. |
| The answer committed | `completed` with the answer. The model is not called again. |
| The run ended failed, or without text | `failed` with `model_failed` or `no_answer`. |
| The run was interrupted | `uncertain` with the recorded reason if an earlier start had begun to retire the run. Otherwise `cancelled` if cancellation was requested, and `failed` with `interrupted` if it was not. |
| Admitted, and the turn never started | After the lease expires, `serve` recovers the admission and runs the turn. If cancellation was requested, `serve` abandons the admission and the request ends `cancelled`. |
| The turn started | After the lease expires, `serve` retires the run. The request ends `uncertain` with `process_stopped`, whether or not cancellation was requested, because the model was reached and tool effects are unknown. The conversation then accepts its next request. |

Crabber's run lease is a fixed 30 seconds. `serve` cannot settle a request that was orphaned mid-turn until the dead process's lease has expired, so that request and the requests behind it wait up to about 30 seconds after the crash.

If crabber refuses the settlement, the request ends `uncertain` with `process_stopped` and `serve` logs `API reconciliation failed`. The conversation's session can then stay busy, and later requests in that conversation fail with `conversation_busy`. Open a new conversation. A crash while a tool was mid-execution was not tested and can end this way.

### Outstanding admissions

A turn that is given up by cancellation, a stop, or its deadline waits 10 more seconds for crabber to answer its admission. If crabber has not answered by then, the admission is outstanding. `serve` keeps it alive, because it can still start a run.

While an admission is outstanding:

- The request stays `running` or `cancellation_requested`, and the conversation's later requests wait behind it.
- The admission holds one of the `limits.max_concurrent_turns` turns.
- `serve` logs `API admission outstanding`, and `GET /v1/health` counts it in `outstandingAdmissions`.

The wait ends when crabber answers. If crabber never answers, the conversation is held until `serve` restarts. A graceful stop then runs out its 20-second budget, and the next start settles the request from crabber's state.

## Borb backend

With a `borb` block, `capabilities.backend` is `borb` and turns run in the coworker's borb worker. The worker runs unkeyed turns and holds no durable receipt, so the gateway cannot confirm what an unfinished turn did.

- A turn that the worker reports as interrupted, timed out, or failed ends `uncertain` with `interrupted`, `timeout`, or `model_failed`. The run may have completed on the worker.
- A cancellation or graceful stop that interrupts a running turn therefore ends `uncertain` with `interrupted`. An `accepted` request is still cancelled or kept as described above.
- A request that a gateway crash left `running` ends `uncertain` with `process_stopped`.

## CLI and Slack

With the local backend, an API conversation is a coworker session named by its `conversationId` in `<state_dir>/<coworker-id>.sqlite`. The first turn creates the session.

- `list-conversations <coworker-id>` lists it, and `ask --conversation <conversationId> <coworker-id>` continues it.
- Turns taken through the CLI do not appear in the API snapshot. The coworker still sees them in its history.
- Concurrent use reports busy to whichever comes second. The CLI prints `conversation is busy`. An API request ends `failed` with `conversation_busy`, and the conversation's next request still runs.
- Slack threads use `slack-` names and cannot collide with a UUID.

With the borb backend, the session lives in the worker's database. `list-conversations` and `ask --conversation` reach it when the configuration has the `borb` block.

API conversations share the coworker's workspace files with its CLI and Slack conversations. See [workspace access and limits](setup.md#workspace-access-and-limits).

## Consumer algorithm

1. Read `GET /v1/capabilities`. Stop if `apiVersion` or `requestContract` is unsupported.
2. Choose the `conversationId` and `requestId`, and store them durably with the exact text before you send anything.
3. Open the conversation. Repeat the identical call after any ambiguity.
4. Submit the request once.
5. After transport loss or `503 retry_same_request`, read the receipt. Then resend the identical body. Treat a `404` as "not recorded yet", not as "never sent". Wait at least 1 second before each retry, and lengthen the wait after each `503`.
6. Poll `requestStatus` until the status is terminal. Wait at least 1 second between polls, and lengthen the wait as the request runs. Allow at least `limits.modelTurnSeconds` after the request starts, plus the time that earlier requests need.
7. Record the terminal receipt. On `completed`, use `result.text`. On `failed` or `uncertain`, follow [failures and retries](#failures-and-retries) before you submit under a new `requestId`.

```sh
api=http://127.0.0.1:8787/v1
auth="Authorization: Bearer $CRABBER_COWORKERS_API_TOKEN"
json='Content-Type: application/json'
conversation=$(uuidgen | tr 'A-F' 'a-f')
request=$(uuidgen | tr 'A-F' 'a-f')

curl -sS -H "$auth" "$api/capabilities"
curl -sS -H "$auth" -H "$json" -d "{\"conversationId\":\"$conversation\",\"coworkerId\":\"scout\"}" "$api/conversations"
curl -sS -H "$auth" -H "$json" -d "{\"requestId\":\"$request\",\"text\":\"<prompt>\"}" "$api/conversations/$conversation/requests"
curl -sS -H "$auth" "$api/conversations/$conversation/requests/$request"
```

To cancel the request, send a body of exactly `{}`.

```sh
curl -sS -H "$auth" -H "$json" -d '{}' "$api/conversations/$conversation/requests/$request/cancel"
```

## Differences from Crew

This API follows the route shape of Crew's integration API v1.

| Crew operation | Here | Reason |
| --- | --- | --- |
| `GET /v1/capabilities`, `GET /v1/health` | Same paths | Health probes only `api.sqlite` and adds `outstandingAdmissions`. There is no Temporal or PostgreSQL. |
| `GET /v1/members`, `POST /v1/members` | `GET /v1/coworkers` only | Coworkers are configuration. Their IDs are configured strings, not UUIDs. |
| `POST /v1/conversations`, `GET /v1/conversations/{id}` | Same paths. The body uses `coworkerId`. | Snapshot messages are plain `{requestId, role, text}` objects, not AG-UI messages. |
| `GET /v1/conversations` | In the OpenAPI document | Crew excludes it from its document. Here no route is outside the document. |
| Submit, status, and cancel | Same paths and semantics | The acceptance boundary is `sqlite_outbox`. There is no `approvalDeadlineSeconds`, `initiator`, or `waiting` status. |
| `POST .../human` | Omitted | Coworkers never pause for decisions. |
| `POST .../events` | Deferred | See [scope](#scope). |
| Routines, memories, handoffs, instruction packs, usage, webhooks, audit | Omitted | This repository has none of them. |
| TypeScript client | Omitted | The OpenAPI document is the client contract. |
