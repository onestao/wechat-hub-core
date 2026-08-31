# Gate 1 Unraid POC Result

Date: 2026-09-01 (Asia/Shanghai)

## Result

The multi-account Core is running against the real two-account Gate-0 Runtime on
Unraid. The read path is proven end to end: Runtime account discovery, PID-scoped
key extraction, database decryption, upstream ingest/media reuse, normalized
Core storage, HTTP reads, events and media streaming.

The final write-path proof is pending because both official WeChat clients are
logged out. The controller now detects that state and refuses to send.

## Deployment

```text
Runtime container: wechat-hub-a-gate0-runtime
Core container:    wechat-core-b-gate1
Core image:        wechat-core:gate1
API bind:          127.0.0.1:18081
Core database:     /app/runtime/core/wechat_core.sqlite
Runtime data bind: /mnt/disk3/appdata/wechat-hub-b-gate1/session-b/runtime
```

Core shares the Runtime PID namespace, has `SYS_PTRACE`, and shares the X11
socket and Runtime display-lock inode. Neither container was restarted during
the final verification.

## Key And Database Evidence

No key material is recorded here.

| Account | Valid DB key entries | Decrypted DB files | Selected `PRAGMA quick_check` |
|---|---:|---:|---|
| `gate0-a` | 16 | 13 | session/contact/message: `ok` |
| `gate0-b` | 15 | 12 | session/contact/message: `ok` |

## Normalized Data

| Account | Chats | Contacts | Members | Messages | Ready synced media |
|---|---:|---:|---:|---:|---:|
| `gate0-a` | 101 | 4921 | 5112 | 1408 | 91 |
| `gate0-b` | 100 | 3961 | 4512 | 0 | 0 |

For `gate0-b`, the decrypted `SessionTable` contains 100 sessions, but
`message_0.db` contains an empty `Name2Id` and no `Msg_*` tables. Its WAL frame
salt does not match the WAL header salt. The zero-message result is therefore a
source-state limitation, not a dropped-ingest claim.

The Core media table additionally contains one uploaded outbox asset per account
from the invalid pre-guard send attempts. Those uploads are not counted as
synced inbound media in the table above.

## API And Stability Evidence

- `/health` returned healthy with two accounts.
- `/v1/accounts` and both account-scoped chat lists returned real data.
- Event poll and acknowledgement passed.
- Media GET returned `200 image/jpeg`, 9903 bytes.
- Two complete event-count samples 15 seconds apart were identical.
- Local regression: 19 tests passed; `py_compile` and `git diff --check` passed.

Historical event totals still include the earlier pre-fix event storm. Stability
is based on no growth after deployment, not on those historical totals being
small.

## Sender Guard And Audit Correction

Current controller status:

```text
gate0-a / window 12582967: 280x380, chat_ready=false, login_required=true
gate0-b / window 14680103: 280x380, chat_ready=false, login_required=true
```

The four old login-screen attempts are corrected from `sent` to `failed` through
`CoreStore.transition_send()`, which also emitted `send.updated` audit events:

```text
send-792e640aca414197bd17727c4ece4155
send-0f81f451557a4f2c8ad6a8f5c82c38da
send-b9f62138dd73490da74885c9ff29024f
send-f4b7e7c8f7174cd490775653f6256fef
```

Error:

```text
login screen detected after verification; X11 actions did not reach a logged-in chat
```

## Remaining Proof

After both clients are logged in, require `chat_ready=true` for both windows,
send one new text and one PNG to each account's own `filehelper`, and wait for a
database echo/increment. X11 submit alone is not sufficient evidence of WeChat
delivery.
