# Completion Report B - Multi-account WeChat Core

Date: 2026-08-31 (Asia/Shanghai)

Branch: `feat/multi-account-core`

Gate result: **source-based multi-account Core implementation is complete for
host-testable work. Package A has separately passed real two-account Gate 0 on
Unraid, but Gate 1 real-WeChat proof for this B build remains pending. The
current Windows execution host has no Docker, and this tool session's attempted
`ssh unraid` connection returned exit 255, so the revised B image has not yet
been deployed against A's live processes/databases. No fixture/Mock result below
is claimed as a real WeChat result.**

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
WeChat echo confirmation
```

The current upstream X11 controller does not provide verified primitives for
the last three. Core therefore preserves the request in its durable outbox but
marks execution failed instead of silently sending a semantically different
message. These items remain Gate 3 integration work.

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

python -m unittest discover -s core/tests -v
# 15 tests passed

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
- refusal of unsafe multi-account global-window discovery.

## Source regression meaning

The regression fixture uses the actual table shapes produced by the retained
`memory/memory_ingest.py` and `memory/media_sync.py` staging pipeline. It proves
that chat, message, member/contact and image/media visibility survives the new
normalization/account scope logic, including two accounts with colliding local
IDs.

It does **not** prove a real current WeChat database/key can be decrypted on
this Windows host. That requires the Linux Runtime, logged-in official WeChat
accounts and process-memory access.

## Real Gate-1 validation still pending

```text
docker --version
-> 'docker' is not recognized as an internal or external command

ssh unraid ...
-> exit 255 in the current coding-tool session
```

Therefore not yet proven here:

- Docker image build / Compose runtime startup;
- live `/proc/<pid>/mem` key extraction;
- real WeChat encrypted DB -> decrypted DB regression;
- real two-account incremental sync;
- real X11 text/image send;
- real WeChat echo/reply/file proof.

## Not reused

| Upstream code/behavior | Reason |
|---|---|
| Console HTTP/UI as Core boundary | It is coupled to AI DB, Docker container names and single-account files; D owns Console migration. |
| Console direct SQLite consumers | C/D/E must use the frozen HTTP contract, not Core storage internals. |
| Global `DISPLAY = ":1"` / display-global window choice | Unsafe with several WeChat accounts. |
| Raw DB keys in normalized Core DB/API | Keys remain filesystem-only privileged material. |
| Any ComWechat/Windows Hook backend | B is derived from the Linux source baseline. |
| A new decryptor/media decoder | Existing upstream algorithms were retained instead of reimplemented. |

## Gate-1 handoff

On a Linux Docker host after package A is integrated:

1. Start the integrated stack against the already proven two-account Runtime;
   Core should load A's registry without a separately maintained B registry.
2. Verify Core dynamically resolves each account's current `pids[]`,
   `db_storage`, DISPLAY/window and the shared display-lock inode.
3. Run the automatic/account-scoped key extraction in Runtime's PID namespace.
4. Run one Core sync and compare the old single-account pipeline against Core
   for chats, messages, contacts/members and media.
5. Repeat with both accounts enabled and verify overlapping local IDs remain
   isolated by `account_id`.
6. Only then enable the sender and run real X11 text/image tests with the shared
   Runtime display lock.

Until those steps run on Linux, the correct status is **implementation ready,
real-WeChat Gate-1 evidence pending**.
