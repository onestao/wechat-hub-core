# Source Audit B — Multi-account Core

Date: 2026-08-31 (Asia/Shanghai)
Worktree: `work/core` on `feat/multi-account-core`

## Source provenance

- Baseline repository: `https://github.com/xiaoguiwucan/linux-wechat-agent.git`
- Locked upstream commit: `58b2c43ff18597c6d0c9ec47270eb40e4fb0b2bb`
- This worktree retains the upstream remote and its licence/attribution files. `upstream/` remains an unmodified read-only reference.

The shared contracts reviewed before implementation were:

- `../../docs/UPSTREAM_LOCK.md`
- `../../docs/SOURCE_MAP.md`
- `../../docs/INTERFACE_CONTRACT_V1.md`
- `../../docs/WORK_PACKAGE_HANDOFFS.md`
- `../../stack/contracts/openapi.yaml`

## Source files actually read

| Area | Files read | Reusable implementation |
|---|---|---|
| Sync orchestration | `memory/sync_worker.py` | `run_once()` preserves the order decrypt → ingest → media and its corrupted-index recovery path. The REINDEX/integrity primitive is now shared through `memory/sync_repair.py` so the multi-account worker reuses the same behavior. |
| Decryption | `memory/decrypt_sync.py` | `refresh_decrypted()`, `source_signature()`, WAL patching and SQLite verification are the account worker's decrypt stage. |
| Message, contact and session ingest | `memory/memory_ingest.py` | `load_contact_names()`, `load_sessions()`, `iter_message_tables()`, `ingest_chat()` and `ingest_memory()` are retained as the per-account staging ingest. |
| Media | `memory/media_sync.py` | `sync_media()`, `sync_image()`, `sync_sticker()`, `sync_video()` and `upsert_media()` remain the per-account media preparation path. |
| Linux key extraction | `tools/wechat-decrypt/find_all_keys_linux.py`, `tools/wechat-decrypt/key_scan_common.py` | `get_pids()`, `_get_readable_regions()`, `scan_memory_for_keys()` and HMAC `verify_enc_key()` retain the existing `/proc/<pid>/mem` workflow. The scanner now accepts repeated registered PIDs plus per-account DB/output paths, matching Runtime's `pids[]` handoff. |
| GUI sender | `agent_console/wechat_controller.py` | `find_main_window()`, `open_chat()`, `paste_active()`, `paste_mention_active()`, `paste_image_active()` and `window_status()` are the account sender primitives. |
| Group members | `agent_console/daily_report.py` (`decode_chatroom_members_buffer()`, `contact_names()`) | Existing contact-db joins and `ext_buffer` decoder provide the Core member import path. |
| Durable outbox | `agent_console/app.py` (sender/outbox ranges, including `paste_reply_to_wechat()`, `create_reply_outbox()`, `update_reply_outbox()` and `reply_outbox_list()`) | Its validation-before-submit and durable status/attempt model inform the Core outbox/sender adapter. |
| Deployment configuration | `docker-compose.yml`, `.env.example` | Existing single `WECHAT_ACCOUNT_DIR_NAME` service wiring identifies exactly what must become registry-based while legacy one-account defaults remain possible. |

## Current constraints found

- `memory/sync_worker.py` has one source/decrypted/status/memory path and one `memory_db`.
- `memory/memory_ingest.py` keys `chats`, `messages`, and `message_media` by account-local identifiers only.
- `memory/media_sync.py` stores media under one `runtime/media` root.
- The key scanner can see several WeChat PIDs but `config.py` and output association are single-account. Reading `/proc/<pid>/mem` requires root or `CAP_SYS_PTRACE`; it must be treated as a privileged operational capability.
- `agent_console/wechat_controller.py` hard-codes `DISPLAY = ":1"`, locates a global main window and shares clipboard state.
- `agent_console/app.py` provides useful durable outbox semantics, but its `AI_DB`, session probe, fixed controller path and global sender state cannot become the Core boundary.
- `docker-compose.yml` and `.env.example` expose only `WECHAT_ACCOUNT_DIR_NAME` and fixed runtime locations.

## Planned changes and file migration

| Existing source path | Treatment | New Core path / role |
|---|---|---|
| `memory/sync_worker.py` | Reuse functions; preserve its standalone legacy CLI and corrupted-index recovery | `memory/sync_repair.py` contains the reused REINDEX/integrity primitive; `core/account_worker.py` invokes `refresh_decrypted()`, `ingest_memory()`, repair-on-malformed and `sync_media()` per registered account. |
| `memory/decrypt_sync.py` | Reuse unchanged algorithm | `core/account_worker.py` supplies account-namespaced source, key, decrypt and state paths. |
| `memory/memory_ingest.py` | Reuse as account-local staging ingest | `core/normalize.py` copies its output into a normalized database with `account_id` in every identity key. |
| `memory/media_sync.py` | Reuse as account-local staging media extraction | `core/normalize.py` maps ready media into account-scoped normalized media rows. |
| `agent_console/daily_report.py` group-member helpers | Extract the source's SQLite query and `ext_buffer` format handling without importing report rendering dependencies | `core/normalize.py` imports account-scoped group-member identities. |
| `tools/wechat-decrypt/find_all_keys_linux.py` | Reuse scanner and permission checks; add backward-compatible `--db-dir`, `--out-file` and repeated `--pid` filters | `core/key_extract.py` invokes it with the account DB directory, key output and live PIDs resolved from package A's UID in the shared PID namespace. `core/registry.py` stores paths/metadata only and Core DB never copies raw keys. |
| `agent_console/wechat_controller.py` | Make display/window selection account-aware without changing its X11 interaction primitives | `core/runtime_bridge.py` resolves the current account-owned X11 window by UID; `core/sender.py` supplies `WECHAT_DISPLAY`/`WECHAT_WINDOW_ID`, fails closed on unresolved multi-account windows, and takes Runtime's shared `display_lock` flock. |
| `agent_console/app.py` reply outbox functions | Reuse their durable-state concepts, validation evidence and attempt tracking, not their Console DB coupling | `core/store.py`, `core/sender.py` and `core/app.py` own account-aware outbox, request-scoped idempotency, stale-`sending` recovery and `send.updated` events. |
| `docker-compose.yml`, `.env.example` | Preserve legacy single-account variables as a bootstrap route | Add documented Core registry/API configuration; do not make C/D/E read any SQLite file. |

## New functionality justified by the audit

No existing upstream module provides all-account registry, cross-account normalized identity, durable HTTP events/cursors, Core HTTP endpoints, consumer acknowledgements, or HTTP-level idempotency. The following modules are therefore added:

- `core/registry.py`: validated account registry, account-scoped filesystem paths, runtime metadata and legacy bootstrap.
- `core/runtime_bridge.py`: package-A registry companion that resolves live PID/window/source paths from the shared Runtime namespaces.
- `core/account_worker.py`: existing sync/decrypt/media functions scheduled per account.
- `core/normalize.py`: account-local staging data → normalized Core records and durable change events.
- `core/store.py`: Core SQLite schema, cursor/event log, acknowledgements, media references and durable send outbox.
- `core/sender.py`: account-aware controller adapter with a lock for a shared X display.
- `core/app.py`: dependency-free HTTP implementation of Contract V1.

Runtime/Core handoff details were checked against package A's implemented Runtime. Core accepts the same account-ID character set (including `.`) and now consumes A's persisted registry directly. The integrated Compose wiring joins A's PID namespace, shares `/tmp/.X11-unix`, mounts the same `/run/wechat-runtime` lock filesystem and grants `SYS_PTRACE`. Live PIDs and account-owned windows are re-resolved by UID instead of being persisted as stale identifiers. This wiring is host-validated structurally but still requires a real Linux Docker run for Gate-1 evidence.

## Explicitly not reused

- No Windows Hook/ComWechat code is introduced; this package remains based on the Linux source baseline.
- The Console's direct reads of `runtime/memory/*.sqlite`, hard-coded Docker containers and AI-specific `reply_outbox` DB are not a Core dependency.
- Raw decrypted database keys are not stored in Core SQLite or exposed through the API.
- The old global `DISPLAY = ":1"` sender selection is not retained for registry-driven requests; it is accepted only as the legacy default when a registry has one account.

## Test and regression plan

- Unit test registry validation and account path isolation.
- Unit test normalized import with two accounts sharing the same chat/message local identifiers; identities must not collide.
- HTTP contract tests for health, accounts/chats, events/poll/ack, media and text/image/file outbox idempotency/errors.
- Test the sender adapter with a fake controller command and verify `account_id` selects the configured display/window metadata; multi-account sends without an explicit account window fail closed.
- Regression fixtures exercise upstream-shaped `chats`, `messages`, `contacts/members`, and `message_media` staging tables so old single-account visibility remains available in the new Core. Real WeChat GUI, decrypted databases and process-memory scanning require a Linux runtime and credentials and will not be claimed from host-only tests.

Current host regression result: 15 tests passed. Coverage includes two-account ID isolation, private incoming/outgoing direction, contact/member import, message-to-media binding, HTTP poll/ack/idempotency, cross-account idempotency conflicts, stale-`sending` recovery, package-A registry translation/source discovery, UID-scoped Runtime PID routing, legacy REINDEX recovery reuse, account-aware sender routing and fail-closed multi-account window selection.

## Operational risks

- Key scanning reads live process memory and requires root or `CAP_SYS_PTRACE`; it must run only in a controlled trusted container.
- GUI sends depend on X11 focus, clipboard and window state. Same-display accounts must serialize sends; acceptance by the API is not a confirmed WeChat delivery.
- Runtime window IDs and PIDs are dynamic after restart. `core/runtime_bridge.py` refreshes them from the shared PID/X11 namespaces on use; unresolved windows fail closed rather than enabling sender fallback to display-global discovery.
- Existing WeChat database layouts may evolve. The normalizer will skip unsupported source columns/tables with account-level status rather than misattribute data.
- Host Docker is unavailable on this Windows workstation. Package A's real Gate-0 Runtime exists on Unraid, but the current coding-tool session's `ssh unraid` attempt exits 255, so this revised B image still needs its real Gate-1 run there.
