# Completion Report B - Multi-account WeChat Core

Date: 2026-09-01 (Asia/Shanghai)

Branch: `feat/multi-account-core`

Gate result: **the source-based multi-account Core implementation is deployed on
Unraid and its real two-account read path is proven. Account-scoped key
extraction, decryption, chat/contact/member normalization, API events and media
streaming all ran against Package A's live Runtime. The two WeChat clients are
currently logged out, so the guarded post-fix text/image send and database echo
proof remains pending. Gate 1 is therefore read-side PASS, write-side pending;
no X11 action performed on the login screen is claimed as a successful send.**

## Upstream used

```text
https://github.com/xiaoguiwucan/linux-wechat-agent.git
@ 58b2c43ff18597c6d0c9ec47270eb40e4fb0b2bb
```

The derivative checkout remains on the required source lineage, retains the
real `upstream` remote, and keeps the upstream license/attribution files.

## Reused code

| Upstream source | Reused symbols/behavior | New/modified location | Adaptation |
|---|---|---|---|
| `memory/sync_worker.py` | decrypt -> ingest -> media order; malformed-index REINDEX/integrity recovery | `memory/sync_repair.py`, `core/account_worker.py` | Recovery primitive is shared by both legacy and multi-account workers; each account gets isolated paths/status. |
| `memory/decrypt_sync.py` | `refresh_decrypted`, source/WAL signatures, WAL patching, SQLite verification | reused directly from `core/account_worker.py` | Passed account-specific source, decrypted, key and state paths. |
| `memory/memory_ingest.py` | contacts/sessions, `iter_message_tables`, `ingest_chat`, `ingest_memory` | reused directly as per-account staging; normalized by `core/normalize.py` | Existing parser is not rewritten; normalized store adds `account_id` to every identity key. |
| `memory/media_sync.py` | `sync_media`, image/sticker/video handling, `upsert_media` | reused directly as per-account staging; mapped by `core/normalize.py` | Account runtime/media roots are isolated; ready media is linked back to normalized messages. |
| `memory/message_parse.py` | `message_display_parts` and sender/content parsing | `core/normalize.py` | Preserves upstream content parsing while adding Core message types, direction and author fields. |
| `agent_console/daily_report.py` | contact DB joins and chatroom member `ext_buffer` behavior | `core/normalize.py` | Extracted only the member-import behavior; report rendering/LLM code is not made a Core dependency. |
| `tools/wechat-decrypt/find_all_keys_linux.py`, `key_scan_common.py` | `/proc/<pid>/maps`, `/proc/<pid>/mem`, `scan_memory_for_keys`, HMAC key verification | modified scanner plus `core/key_extract.py` | Added backward-compatible account DB/output options and repeated PID filters; Core accepts Runtime-style `pids[]`. |
| `agent_console/wechat_controller.py` | window discovery, open-chat, paste, blue mention, image paste, submit/status | modified controller plus `core/sender.py` | `DISPLAY`/preferred window become account-aware; multi-account mode fails closed without an account-resolved window. |
| `agent_console/app.py` | durable reply-outbox concepts, send states, validation-before-submit, confirmation discipline | `core/store.py`, `core/sender.py` | New account-aware durable outbox/events/idempotency without carrying Console/AI DB coupling into Core. |
| `docker-compose.yml`, `.env.example` | original single-account deployment inputs | same files | Legacy services remain available under a `legacy` profile; new Core service/registry settings are added. |

## New code

The upstream repository has no account registry, cross-account normalized
identity, durable Core event contract or account-aware HTTP outbox. Those gaps
justify the following new modules:

- `core/registry.py` - validated registry and account-scoped path derivation;
- `core/runtime_bridge.py` - live package-A UID -> PID/window/source-path bridge;
- `core/account_worker.py` - per-account reuse of decrypt/ingest/media stages;
- `core/normalize.py` - staging SQLite -> account-normalized Core records;
- `core/store.py` - normalized durable SQLite, events, acks, media and outbox;
- `core/sender.py` - account-aware controller adapter and display locking;
- `core/app.py` - Core Interface Contract V1 HTTP server;
- `core/key_extract.py` - account/PID-scoped wrapper for the existing scanner;
- `core/tests/test_core.py` - contract, regression, isolation and sender tests;
- `core/accounts.example.json` and `core/README.md` - operator handoff.

`memory/sync_repair.py` is a small extraction of behavior already present in
the upstream worker, not a replacement algorithm.

## Multi-account behavior implemented

- Stable opaque `account_id` scopes chats, messages, members, media and sends.
- Account IDs accept the package-A Runtime character set, including `.`.
- Two accounts can contain identical local `chat_id`, `message_id` and
  `media_id` values without collision.
- Every account gets isolated source/decrypt/key/staging/media/status paths.
- Existing decrypt/ingest/media code runs once per registered account.
- Private message direction uses upstream `origin_source` behavior instead of
  incorrectly treating every no-prefix private message as self-sent.
- Ready media is attached to its normalized message and still emits
  `media.ready`; later media readiness can produce `message.updated`.
- Durable events are cursor ordered and acknowledged per consumer.
- `/v1/send/text|image|file` is durable/idempotent at acceptance time.
- Idempotency reuse is accepted only for the same account/chat/kind/request;
  conflicting reuse returns HTTP 409 rather than a cross-account receipt.
- Interrupted `sending` rows are lease-recovered to explicit `failed`/unknown
  delivery state and are not blindly retried.
- A successful HTTP send response means queued, not WeChat-confirmed.
- Session rows remain visible as chats even when WeChat has not materialized a
  corresponding `Msg_*` table.
- Normalization runs in one thread-local transaction per account and group chats
  are upserted once with their final member count.
- Timestamp-only sync changes do not emit duplicate `account.status` events.
- Core media sync uses local sticker cache only; remote sticker CDN fetches are
  disabled in the periodic worker so an expired URL cannot stall account sync.
- Runtime PID lists may contain same-UID helper processes, but key extraction
  requires at least one verified WeChat process and scans only valid candidates.

## Runtime/Core handoff compatibility

Package A was inspected after its Gate-0 implementation. B now reads A's
persisted `/config/wechat-runtime/accounts.json` shape directly and translates
its stable metadata. Ephemeral values are resolved live:

```text
account_id with letters/digits/_/./-
account UID + shared PID namespace -> current pids[] for key scan
account UID + shared X11            -> current account-owned window_id
account HOME                         -> current db_storage/base path
display                              -> WECHAT_DISPLAY + shared flock path
```

`stack/docker-compose.yml` now joins Core to Runtime's PID namespace, grants
`SYS_PTRACE`, shares `/tmp/.X11-unix`, and mounts the same
`/run/wechat-runtime` volume. Core therefore takes the same Runtime flock inode
in addition to its in-process per-display lock. The B image installs
`xdotool`, `xclip` and `x11-utils` (`xprop`) required by its reused controller.
The Dockerfile also has a production `CMD`; the total stack additionally gives
Core an explicit `core.app` command.

For more than one registered account, Core refuses display-global WeChat window
discovery unless the account has `window_id` or a custom controller explicitly
declares `controller_resolves_account=true`. This is fail-closed behavior to
reduce cross-account mis-send risk.

The reused controller also fails closed when the resolved surface is a login or
other non-chat window. A `280x380` login surface reports
`chat_ready=false`, `login_required=true`; open/paste/image/submit operations are
rejected before input is delivered.

### Registry hot reload and Runtime lifecycle management

The Runtime registry is no longer a permanent startup snapshot. Core keeps one
thread-safe `AccountRegistry` object shared by sync and sender loops, watches
the persisted Runtime registry by content fingerprint (default every 1 second),
and atomically replaces that object's account snapshot when the file changes.
Direct Runtime CLI register/unregister changes therefore join/leave subsequent
Core sync/sender cycles without restarting Core.

For Console operator actions, Core also consumes Runtime's private
`/run/wechat-runtime/control.sock` Unix socket and exposes an additive
`/v1/runtime/*` HTTP extension. Console-driven create/remove forces the same
registry reload synchronously. Core does not receive Docker Socket access.

Removal semantics are deliberately conservative: the removed account is hidden
from active `/v1/accounts` while its historical normalized rows remain in Core;
accepted/queued sends that never reached the sender fail explicitly. A row
already in `sending` state is not guessed or automatically retried because GUI
delivery may already have happened. A sync cycle that finishes after removal
also checks the live registry before updating status, preventing the account
from being resurrected by a stale worker iteration.

## Sender capability boundary

Verified from the existing source primitives and host fakes:

```text
plain text routing
single verified blue mention routing
image paste routing
account DISPLAY/window environment
durable send state/events
```

Not falsely implemented:

```text
native target_message_id quoted reply
arbitrary file-paste delivery
hard/verified WeChat echo confirmation
```

The current upstream X11 controller does not provide verified primitives for
the last three. Core therefore preserves the request in its durable outbox but
marks execution failed instead of silently sending a semantically different
message. These items remain Gate 3 integration work. Core now also exposes
these concrete limitations through the additive V1 `/health`
`sender_capabilities` field so adapters can avoid knowingly queuing unsupported
operations; the request schemas themselves remain unchanged.

Post-integration review added a conservative **plain-text echo reconciliation**
path for the already-synced outgoing database record: it links a `sent` outbox
row only when account, chat, exact text and a short time window produce exactly
one candidate, and deliberately refuses ambiguous/mention matches. The
resulting `send.updated` event is emitted before the matching
`message.created`, which lets C learn the `send_id -> echo_message_id` alias
before deciding whether an outgoing message is native or its own echo. This is
still not advertised as hard echo confirmation and remains a Gate-3 live
validation item until proven against real logged-in WeChat traffic.

## HTTP contract implemented

The service implements the frozen V1 endpoints used by C/D/E:

```text
GET  /health
GET  /v1/accounts
GET  /v1/accounts/{account_id}/chats
GET  /v1/events/poll
POST /v1/events/ack
GET  /v1/media/{media_id}?account_id=...
POST /v1/send/text
POST /v1/send/image
POST /v1/send/file
```

It additionally implements the optional operator-only V1 extension used by the
Console when Runtime management is available:

```text
GET    /v1/runtime/accounts
POST   /v1/runtime/accounts
POST   /v1/runtime/accounts/{account_id}/start|stop|restart
DELETE /v1/runtime/accounts/{account_id}
GET    /v1/runtime/accounts/{account_id}/login
GET    /v1/runtime/accounts/{account_id}/login/snapshot
```

C/E do not depend on this extension; absence remains compatible with their
frozen Core V1 data/message boundary.

The login endpoints remain operator-only and ephemeral. Core requests an
account-scoped window snapshot from Runtime's private Unix socket, validates
the bounded PNG, and serves it with `Cache-Control: no-store`; Core does not
write the login image to its DB or media storage.

C/D/E do not need and must not receive Core SQLite access.

## Regression validation

Current host commands passed:

```text
python -m py_compile \
  core/__init__.py core/registry.py core/runtime_bridge.py core/store.py core/normalize.py \
  core/account_worker.py core/sender.py core/app.py core/key_extract.py \
  memory/sync_worker.py memory/sync_repair.py \
  agent_console/wechat_controller.py \
  tools/wechat-decrypt/find_all_keys_linux.py

python -m unittest core.tests.test_core -q
# 24 tests passed

PyYAML parse: docker-compose.yml and ../../stack/docker-compose.yml
git diff --check
python -m core.app --help
python -m core.key_extract --help
```

The tests cover:

- V1 health/accounts/chats HTTP behavior;
- account-scoped event polling and acknowledgements;
- media streaming and inline-media sends;
- text/image/file acceptance and idempotency;
- two accounts sharing identical local identifiers;
- private incoming/outgoing message direction;
- contact and group-member visibility;
- message -> media binding;
- Runtime-style multi-PID key-scanner routing;
- package-A persisted-registry translation and live source-directory discovery;
- UID-filtered Runtime PID discovery;
- idempotency conflict on cross-account/different-request key reuse;
- lease recovery of interrupted `sending` rows without unsafe auto-retry;
- legacy malformed-index REINDEX primitive reuse;
- Runtime-compatible account-ID/path isolation;
- account-aware controller DISPLAY/window routing;
- refusal of unverified native quoted replies;
- refusal of unsafe multi-account global-window discovery;
- preservation of session-only chats without `Msg_*` tables;
- event deduplication across repeated normalization/status updates;
- local-only sticker sync routing;
- login-window sender rejection.

## Source regression meaning

The regression fixture uses the actual table shapes produced by the retained
`memory/memory_ingest.py` and `memory/media_sync.py` staging pipeline. It proves
that chat, message, member/contact and image/media visibility survives the new
normalization/account scope logic, including two accounts with colliding local
IDs.

It does **not** prove a real current WeChat database/key can be decrypted on
this Windows host. That requires the Linux Runtime, logged-in official WeChat
accounts and process-memory access.

## Real Gate-1 validation on Unraid

Deployment used the already-running Gate-0 Runtime without restarting it:

```text
Runtime container: wechat-hub-a-gate0-runtime
Core container:    wechat-core-b-gate1
Core image:        wechat-core:gate1
Core API:          127.0.0.1:18081
PID namespace:     container:<Gate-0 Runtime container id>
Capability:        SYS_PTRACE
Core restarts:     0
Runtime restarts:  0
```

Core uses the Runtime's X11 socket and display-lock inode. Its writable runtime
data is bound directly from
`/mnt/disk3/appdata/wechat-hub-b-gate1/session-b/runtime`; the direct disk path
avoids SQLite WAL `fdatasync` stalls observed through `/mnt/user` FUSE.

No database key value was printed or copied into this report. Metadata-only
verification found:

| Account | Valid DB key entries | Decrypted DB files | SQLite quick checks |
|---|---:|---:|---|
| `gate0-a` | 16 | 13 | session/contact/message: `ok` |
| `gate0-b` | 15 | 12 | session/contact/message: `ok` |

Normalized real-data results:

| Account | Chats | Contacts | Members | Messages | Ready synced media |
|---|---:|---:|---:|---:|---:|
| `gate0-a` | 101 | 4921 | 5112 | 1408 | 91 |
| `gate0-b` | 100 | 3961 | 4512 | 0 | 0 |

`gate0-b`'s zero messages is not an ingest omission. Its decrypted
`SessionTable` has 100 rows, while `message_0.db` has an empty `Name2Id` and no
`Msg_*` tables. The live WAL header/frame salts also differ, so the observed
frame is stale/invalid and was correctly not applied. The truthful claim is
session/contact/member synchronization, not historical-message synchronization.

API verification passed for `/health`, `/v1/accounts`, account-scoped chats,
event poll/ack and media GET. The sampled media response was `200 image/jpeg`
with 9903 bytes. Two event-count samples 15 seconds apart were identical after
the deduplication fixes, showing no continuing account/chat event storm.

Both resolved window IDs currently report the login surface:

```text
gate0-a / 12582967: 280x380, chat_ready=false, login_required=true
gate0-b / 14680103: 280x380, chat_ready=false, login_required=true
```

Four earlier self-test outbox rows had been incorrectly marked `sent` by the old
controller while those login surfaces were active. They are now `failed`, retain
their original unconfirmed controller evidence, and carry this audit error:

```text
login screen detected after verification; X11 actions did not reach a logged-in chat
```

The deployed Runtime also contains Package A's minimized-window recovery fix:
`start <account_id>` activates the account's existing `Weixin` surface through
the shared display lock and reports `action: restored`; it does not launch a
duplicate process.

## Not reused

| Upstream code/behavior | Reason |
|---|---|
| Console HTTP/UI as Core boundary | It is coupled to AI DB, Docker container names and single-account files; D owns Console migration. |
| Console direct SQLite consumers | C/D/E must use the frozen HTTP contract, not Core storage internals. |
| Global `DISPLAY = ":1"` / display-global window choice | Unsafe with several WeChat accounts. |
| Raw DB keys in normalized Core DB/API | Keys remain filesystem-only privileged material. |
| Any ComWechat/Windows Hook backend | B is derived from the Linux source baseline. |
| A new decryptor/media decoder | Existing upstream algorithms were retained instead of reimplemented. |

## Remaining Gate-1 handoff

Only the logged-in write-path proof remains:

1. Log both official clients back in and leave their main chat windows restored.
2. Re-run controller status and require `chat_ready=true` plus
   `login_required=false` for both account-owned window IDs.
3. Send a new uniquely marked text and PNG only to each account's own
   `filehelper`, under the Runtime display lock.
4. Treat X11 completion as an attempt, then wait for the corresponding database
   increment/echo before declaring WeChat-confirmed delivery.
5. If `gate0-b` materializes `Msg_*` tables after login, verify its message count
   advances from zero and remains isolated from `gate0-a`.

Until that login-dependent proof is observed, the correct status is **real
two-account Core read path complete; real guarded X11 send/echo pending**.

## 2026-09-01 sender incident superseding the write-path plan

The planned live `filehelper` test exposed an unsafe upstream-controller
assumption. `chat_search_query("文件传输助手")` returned `文件`, and `open_chat()`
pressed Return on the first result without verifying the active chat title. The
user reported that test content appeared in a group instead of File Transfer
Assistant. Therefore the Gate-1 write path is **failed and disabled**, not
pending a retry.

Containment and correction:

- stopped only `wechat-core-b-gate1`; the Gate-0 Runtime and both official
  clients remained running;
- corrected the four incident outbox rows from `sent` to `failed` with an audit
  reason and confirmed no pending/sending rows remained;
- preserved the old container as `wechat-core-b-gate1-incident-stopped`;
- deployed `wechat-core:gate1-safe` with `--send-interval 0`;
- changed the controller `open` action to fail before any X11 input;
- made Core require `controller_verifies_chat_target=true` before text/image
  dispatch; the current Runtime/controller does not provide that capability;
- changed advertised text/image/file capabilities to false;
- passed 26/26 local regression tests.

No further live send testing is permitted until a replacement controller can
independently verify the exact active chat before paste and submit. Database
echo matching is post-send evidence and cannot compensate for unsafe target
selection.
