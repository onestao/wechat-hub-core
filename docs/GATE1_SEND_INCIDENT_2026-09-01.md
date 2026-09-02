# Gate 1 Wrong-Chat Send Incident

Date: 2026-09-01 (Asia/Shanghai)

## Summary

At approximately 10:11, four live Gate-1 requests were intended for the two
accounts' File Transfer Assistant chats. The Core outbox correctly carried
`chat_id=filehelper`, but the X11 controller did not select by that stable ID.
It searched the display name using only `文件`, pressed Return on the first
result, and did not verify the selected chat title. The user reported that test
content appeared in an added group.

This is a wrong-target safety incident. X11 completion and the original `sent`
statuses are invalid as delivery evidence.

## Affected Requests

```text
send-0dc9050a46464cc5a25f6eac900eca95  gate0-a text
send-4b612d570a8f440aa1bbc9a9280a1b55  gate0-a image
send-4caeff2645724823b02812a969a28450  gate0-b text
send-65ae1d176d564bfda5955de599784bc8  gate0-b image
```

Run marker:

```text
gate1-live-20260901-71d2e4a5
```

The exact affected group and final delivery count could not be reconstructed
from the Core/staging databases before containment. The user-visible report is
the authoritative impact signal. Any recall must be performed manually; no
automated recall was attempted.

## Root Cause

The reused controller implemented fuzzy UI navigation:

```text
文件传输助手 -> 文件 -> first search result -> Return
```

It verified the account window and pasted text, but never verified the selected
chat. Account scoping, display locking and `chat_id=filehelper` in the API did
not protect the final UI target.

## Containment

- Stopped `wechat-core-b-gate1` immediately.
- Left `wechat-hub-a-gate0-runtime` and both WeChat clients running.
- Corrected all four outbox rows to `failed` through CoreStore and emitted audit
  events.
- Confirmed no accepted, queued or sending outbox rows remained.
- Preserved the original stopped container and a stopped sync-only predecessor.
- Started `wechat-core:gate1-safe` with `--send-interval 0` for read-only sync/API.

## Corrective Controls

- Controller `open` now refuses all chat switching before X11 input.
- Search queries are no longer shortened.
- AccountSender requires `controller_verifies_chat_target=true` before dispatch.
- Current sender capabilities advertise text/image/file as false and
  `verified_chat_target=false`.
- Regression suite passed 26/26 tests.

## Re-Enable Condition

Do not re-enable live sending until a replacement primitive can independently
read and verify the exact active chat identity before paste/submit. A screenshot
guess, fuzzy display-name search, fixed click coordinates, or post-send database
echo is not sufficient.
