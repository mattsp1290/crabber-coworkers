# Run coworkers inside borb

The Slack host keeps its bot identities, authorization, acknowledgements, queues,
and threaded replies. One persistent borb per coworker runs the complete crabber
agent and owns that coworker's SQLite database and workspace files. A mention
creates or wakes the borb. Independent Slack threads remain separate conversations in that database.
The host never falls back to a local agent when borb fails.

## Prepare the worker and access

1. Run `cargo xtask check` and `python3 -m unittest discover -s scripts -p 'test_*.py'`.
2. Run `scripts/build-worker-image.sh arm64` for the current cluster worker.
   This requires cargo-zigbuild, Zig, Docker, Python 3, and a CA bundle at
   `SSL_CERT_FILE` or `/etc/ssl/cert.pem`. The script builds locally without publishing.
   The image installs ripgrep at build time and provides `/bin/sh` for catalog
   validation. The runtime stays unprivileged and exposes neither executable as
   a model tool. Check the image's UID, `/bin/sh`, `rg --version`, and worker
   `--version` in a disposable container with no credentials or network.
3. Obtain the owner's deployment approval before registry or cluster writes.
4. Publish the image under the cluster's allowed registry prefix and record the
   immutable manifest digest. The observed prefix is `10.0.0.106:30001/borb/`.
5. Create the dedicated identity in `deploy/worker-access.yaml`. Add its subject
   to borbd's client allowlist using `deploy/borb-client-values.yaml` as a reviewed
   overlay. Compare the overlay with the current clients before applying it.
   Preserve all other release values. The current external-platform release
   requires a fresh database backup and the bounded `borb-system/borb-helm`
   identity for Helm upgrades. Do not use the legacy deployment mutators.
6. Provision a Secret named `coworkers-model` in `borb-sandboxes`, labeled
   `borb.dev/mountable=true`. Read `OPENCODE_GO_API_KEY` from the provisioning
   process environment and submit it through stdin. Never put it in command
   arguments, files, Helm values, or logs. The worker receives it through
   `SecretRef` environment injection. Slack tokens stay on the Slack host.
7. Establish an authenticated route from the Slack host to borbd. For the current
   host, an SSH local forward can reach the internal Service without exporting
   the admin kubeconfig.

```sh
ssh -N -o ExitOnForwardFailure=yes \
  -L 127.0.0.1:18080:10.43.85.21:8080 infra-admin@100.82.43.93
```

For a maintained host, stop the manual forward and run the supervisor with the
existing Slack credentials in its environment.

```sh
python3 scripts/serve-borb.py --config /private/config.json \
  --binary /absolute/path/to/crabber-coworkers
```

The supervisor owns the SSH forward and requests a dedicated audience-`borb`
token for the child environment. It renews halfway through the token lifetime.
SIGUSR1 stops Slack admission and finishes acknowledged messages and replies
before the host restarts with the replacement token. Explicit SIGINT or SIGTERM
retains bounded cancellation. The supervisor verifies its private SSH control
master before starting the credential-bearing child and refuses occupied ports.
It retries transient failures without recording credentials or message content.

Keep the original Slack credential holder alive until the supervised parent has
inherited its environment. A failed temporary cutover process must not be the
only holder of those credentials. The supervisor stores no credential files.
After a workstation reboot, restore its Slack environment before starting it.

For one-off CLI and verification commands, obtain a token with audience `borb`
and set `BORB_TOKEN` directly in the command's environment. The application does
not read token files. The worker receives neither this token nor a Kubernetes
service-account token.

All control-host kubectl calls use
`--kubeconfig=/etc/rancher/k3s/k3s.yaml`. The default context rejects authentication.
Host sudo is owner-run. The admin kubeconfig stays on the control host.

## Configure remote execution

Add this block to the existing private configuration. Replace the image digest
with the published value.

```json
"borb": {
  "url": "http://127.0.0.1:18080",
  "name_prefix": "coworkers",
  "image": "10.0.0.106:30001/borb/coworkers@sha256:<published-digest>",
  "model_secret": "coworkers-model"
}
```

Keep existing coworker definitions, Slack allowlists, and turn limits. Add
`"workspace": "read_write"` to each coworker that may create or revise files.
Use `read_only` for read/list access. Omission or `disabled` grants no file tools.

Production `serve` requires `BORB_TOKEN` and its existing Slack token variables. It does not
need the model key. The worker requires the model key in its environment.

`ask --conversation` and `list-conversations` use the remote database when this
block is present. Remote `ask` requires `--conversation`. Without the block,
commands retain their existing local behavior. The provider-injection library
entry points deliberately remain local for credential-free callers and tests.
`serve_remote_with` injects a remote executor for integration tests.

The worker admits files only beneath its own
`<state_dir>/workspaces/<coworker-id>/`. With the default worker state path,
Scout's files live at `/home/borb/workspace/state/workspaces/scout/` and its
database remains `/home/borb/workspace/state/scout.sqlite`. The host sends the
workspace grant, never a root path. Remote turns create no local workspace, and
`list-conversations` does not create a remote workspace. All authorized users and
threads of one coworker share that worker's files. The [workspace limits and
failure semantics](setup.md#workspace-access-and-limits) also apply remotely.

Host and worker now require protocol 2. Health reports version 2, and turn,
cancellation, and conversation routes use `/v2/`. Version 1 requests and routes
fail closed. Upgrade the host binary and worker image together. Drain active
work, stop the host and affected workers, update the immutable image digest,
and restart with matching versions. Keep workers behind the existing
authenticated borb proxy. This is not a public file API.

A changed worker image or Secret reference causes an explicit failure while the
old worker is running. Drain and restart that borb during a controlled upgrade.
The host does not restart active work automatically. Borb's idle sleep preserves
its disk. Polling active turns keeps the borb active. The configured cluster
allows four running borbs, so concurrent coworker capacity remains bounded.

To change workspace grants, drain active work and restart the host and affected
worker. Revocation keeps existing files and does not undo completed writes.
After enabling or disabling workspace use, start a new named conversation or
root Slack mention. Existing sessions whose canonical directory differs fail
with `cannot use workspace`. Read-only and read/write share the same directory.
Moving a state directory to a new canonical path also requires new conversations.

To roll back binaries, drain and stop both host and workers, preserve the disk
and SQLite snapshots, then restore matching host and worker protocol versions
and the corresponding image digest. Leave workspace files in place. A binary
rollback does not undo completed file edits. Never reset state to make an old
conversation's directory match.

## Back up workspace files

The SQLite snapshot and import helpers copy conversation databases only. They do
not copy `workspaces/`. While all writers are stopped, back up the whole persistent
coworker disk or copy the workspace directories separately with their private
ancestor permissions. Do not import links to credentials or private host files.

When files must correspond to their chat history, restore files and the database
from the same quiesced backup. File replacements and chat commits are independent,
so a live database snapshot alone cannot establish that correspondence. Keep
existing files and databases until the replacement has been verified.

## Move existing conversation history

Stop the live Slack host cleanly before cutover and confirm every local writer
has exited. Keep the original database as the backup. Create a complete snapshot
of each coworker's database. The helper refuses to overwrite an existing destination. After all writers exit,
use `--offline` only when WAL, SHM, and journal sidecars are absent. Without that
flag the backup API includes committed WAL data. The offline flag also supports
checkpointed WAL databases whose read-only open requires missing sidecars.

```sh
python3 scripts/borb-store.py snapshot --offline /private/state/scout.sqlite /private/scout-backup.sqlite
```

Create the target borb before activating the Slack host. With remote configuration
and an environment token, `list-conversations --config /private/config.json scout`
creates or wakes the exact worker specification. An empty listing leaves an
absent database untouched, so do not submit a turn yet.
Set `BORB_URL`, `BORB_TOKEN`, and the target `BORB_ID` in the environment. Import
the snapshot through borb's authenticated exec endpoint.

```sh
python3 scripts/import-borb-store.py --source /private/scout-backup.sqlite --coworker-id scout
```

The importer sends encoded chunks on exec stdin, verifies the transferred checksum,
and publishes the database without replacing an existing target or sidecar.
A failed import can leave a private `.import-*` staging file. Inspect it before
retrying. The destination is `/home/borb/workspace/state/scout.sqlite`.
Never copy a live main database while ignoring its WAL.

Compare `list-conversations` with the pre-cutover inventory. Listing initializes
an existing imported database through its worker-owned connection. Then start the host
with remote configuration. After any remote turn, rollback requires stopping
remote writes and exporting a fresh SQLite-consistent remote snapshot. Switching
back to the old local database would lose newly committed history. Copy workspace
files separately while writers are stopped if you also move deliverables. A
change of canonical workspace directory requires new conversation names.

## Verify a real Slack turn

1. Record the borb and pod inventory before the mention. An absent or sleeping
   worker establishes the launch or wake baseline.
2. Send an authorized human mention through the existing Slack client. Confirm
   the coworker answers in the same thread.
3. Run the read-only process verifier with environment credentials. It inspects
   actual Kubernetes PID 1 executable and arguments, rather than trusting image
   names or readiness alone.

```sh
python3 scripts/verify-borb-runtime.py --name-prefix coworkers --coworker-id scout \
  --ssh-host infra-admin@100.82.43.93
```

4. Send a mentioned follow-up in that thread and confirm prior context remains.
5. Gracefully stop and restart the worker, then repeat the follow-up. Confirm
   persisted history remains available.
6. Record only outcomes, borb IDs, pod identities, and node names. Never record
   message text, answer text, credentials, or raw provider errors.

A fake Slack test exercises Socket Mode, the borb client, the actual worker HTTP
routes, SQLite history, and reply delivery without credentials. It cannot prove
the live cluster, mounted model key, or human-authored Slack event. The process
verifier proves worker placement, but requires a real completed turn to establish
that crabber ran there.

The worker owns turns independently of HTTP connections. Submission occurs once;
ambiguous transport failures produce a fixed failure and an explicit cancellation
attempt. Requests have an absolute expiry and worker-owned deadline. Existing
crabber interruption and SQLite Busy behavior remain authoritative. The worker's
bounded in-memory result registry is not a durable queue. Crash recovery,
exactly-once Slack delivery, and automatic replay are not promised. A crashed
unfinished session can require controlled recovery before reuse.
