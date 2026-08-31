# WeChat Core

`core/` is the account-aware service derived from the existing
`linux-wechat-agent` decrypt, memory-ingest, media-sync and X11-controller
modules. It implements the frozen Core Interface Contract V1.

## Registry

The integrated production path reads package A's registry directly from
`/app/config/wechat-runtime/accounts.json`. `core/registry.py` translates its
stable account id/UID/HOME/DISPLAY fields, while `core/runtime_bridge.py`
discovers current WeChat PIDs, account-owned X11 window and the account-local
`db_storage` at use time. PID/window IDs are therefore not copied into a static
Core registry and survive Runtime restarts without manual edits.

The integrated Compose stack mounts A's `/config`, joins A's PID namespace,
shares `/tmp/.X11-unix` and `/run/wechat-runtime`, and grants Core
`SYS_PTRACE`. Runtime accounts therefore map as follows:

```text
account id / UID / HOME / DISPLAY -> stable registry handoff
UID + shared /proc                -> current WeChat pids[]
UID + shared X11                  -> current WECHAT_WINDOW_ID
HOME                              -> account-local db_storage/media tree
DISPLAY                           -> shared Runtime/Core flock path
```

Same-display accounts are serialized inside `core/sender.py` through the same
Runtime lock inode because clipboard and focus are global resources. If an
account-owned window cannot be resolved, a multi-account send fails closed
rather than falling back to a display-global WeChat window search.

For standalone migration/testing, `core/accounts.example.json` remains a
supported native Core registry shape. Its derived runtime files are isolated
below `runtime/accounts/<account_id>/` and `runtime.sender_enabled` defaults to
`false`.

The key scanner is privileged. Run `python -m core.key_extract --account <id>`
only in the trusted integrated Runtime PID namespace with root or
`CAP_SYS_PTRACE`. The production Compose wiring now provides that namespace and
capability. `core/account_worker.py` also invokes account-scoped key extraction
automatically when a Runtime-backed account has no key file. Keys remain in the
account runtime directory and are never written to Core SQLite or the HTTP API.

## Run

```text
python -m core.app \
  --registry /app/config/wechat-runtime/accounts.json \
  --require-registry --sync-interval 5 --send-interval 1
```

The service queues sends through `/v1/send/*`. A `202` response only means the
request entered the durable Core outbox. Even after an X11 submit operation,
the sender records that no WeChat echo confirmation has been observed until a
real normalized echo is available.

If Core exits while an outbox row is `sending`, the row is recovered after the
sending lease as `failed` with unknown delivery state. It is intentionally not
auto-retried because the GUI submit might have completed immediately before the
crash. Reusing an idempotency key for a different account, chat, kind, or
request body returns `409 idempotency_conflict` instead of another account's
receipt.

The reused X11 controller currently has verified primitives for plain text,
one blue mention, and image paste. It does not have a verified native quoted
reply or arbitrary file-paste primitive. Core therefore fails those sender
operations rather than silently dropping `target_message_id`/file semantics.

`python -m core.app --legacy-bootstrap` exposes the old
`WECHAT_ACCOUNT_DIR_NAME` configuration as a single `legacy` account for
migration; it does not make the legacy layout multi-account.
