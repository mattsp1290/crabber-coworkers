# Slack setup

Create one Slack app per coworker so each has its own bot identity. Start from this manifest template; replace the display names for other coworkers:

```yaml
display_information:
  name: Scout
features:
  bot_user:
    display_name: Scout
    always_online: false
oauth_config:
  scopes:
    bot:
      - app_mentions:read
      - chat:write
settings:
  event_subscriptions:
    bot_events:
      - app_mention
  interactivity:
    is_enabled: false
  socket_mode_enabled: true
  token_rotation_enabled: false
```

Subscribe only to `app_mention`. DMs, slash commands, interactivity, and unmentioned thread replies are outside this milestone.

Under Basic Information, generate an app-level token with `connections:write`. Install the app to the workspace and obtain its bot token. Invite the bot into every allowlisted channel; otherwise mentions never arrive and posting fails with `not_in_channel`. Private channels also need an invitation.

Find the team ID in the workspace's Slack web URL (the segment beginning with `T`). Channel details show the channel ID. For a user ID, open the member's profile and use “Copy member ID”. Use these IDs only in your ignored local config. The following values are synthetic and match the example:

```json
"slack": {
  "team_id": "T0000000001",
  "allowed_channel_ids": ["C0000000001"],
  "allowed_user_ids": ["U0000000001"]
}
```

Add this block to Scout in `config.json`; coworkers without a block are not served. Set these variables in the environment, keeping token values out of files and command output:

```text
OPENCODE_GO_API_KEY=<model-key>
SLACK_BOT_TOKEN_SCOUT=<bot-token>
SLACK_APP_TOKEN_SCOUT=<app-level-token>
```

The bot token must start with `xoxb-` and the app-level token with `xapp-`. Rotating `xoxe.xapp-` tokens are not supported. For another coworker, uppercase its ID and replace hyphens with underscores in the two Slack variable names.

```sh
target/release/crabber-coworkers check-config --config config.json
target/release/crabber-coworkers serve --config config.json
```

Mention the coworker as an allowlisted user in an allowlisted channel. It answers in the thread beneath that root message. Mention it again inside the thread for each follow-up; plain thread messages get no answer. Stop and restart the process and the next mentioned follow-up retains the conversation history. See [the live check](manual-smoke.md#slack-live-check) for qualification.

With `workspace: "read_write"`, ordinary mentions can create and revise files. For example, mention Scout with "Create brief.md with a project checklist and report the saved path." In another authorized thread, mention Scout with "Read brief.md, then add a review step." Ask "List your workspace files" or "Read brief.md" to retrieve the result through a normal reply.

The existing allowlists govern access to the whole coworker workspace. Authorized users in different threads or channels share those files, while their chat histories stay separate. `read_only` permits read/list requests and `disabled` exposes no file tools. There are no attachments or download links. File responses use the same Slack truncation limits as other answers. Ask for bounded line windows when a file is too large for one reply. See [workspace limits](setup.md#workspace-access-and-limits).

Drain active work and restart after changing grants. After enabling or disabling access, use a new root mention because an old thread's stored directory may no longer match. A mismatch produces the generic failure notice below. Completed file changes can survive a failed or missing reply. Read/list the files before retrying a turn.

The service silently ignores other teams, channels, users, bot-authored events, and event subtypes. It strips its own mention tokens from input and decodes Slack's text entities once. Thread names are `slack-<lowercase channel>-<root ts with dots replaced by hyphens>` and appear in `list-conversations`.

Model text escapes `&`, `<`, and `>` and disables parsing, link names, and unfurls so answers cannot ping people. Answers use at most 8 entity-safe chunks of 3,500 characters, ending with `[answer truncated]` if necessary. The fixed notices are:

| Condition | Slack notice |
| --- | --- |
| Empty mention | Mention me with a question and I will answer in this thread. |
| Above the configured prompt byte limit | That message is too long for me to answer. |
| Full queue or conversation busy | I am still working on earlier messages in this thread. Please try again shortly. |
| Model turn timed out | I ran out of time answering that. |
| Other turn failure | I could not answer that. |

Each thread queues 4 waiting mentions. A queue-overflow busy notice is sent at most once per 30 seconds per thread. See [setup](setup.md#serving-slack) for concurrency, shutdown, fixed stderr messages, and crash/delivery limitations.
