#!/usr/bin/env python3
"""Small X11 controller for the Linux WeChat window.

This script intentionally does not inspect screenshots or write WeChat databases.
It drives the main Linux WeChat window through X11 and closes embedded web
windows that would otherwise steal focus.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import re
import shutil
import subprocess
import sys
import time


# Runtime/Core supplies this per account.  The legacy display remains the
# default so existing single-account Console usage is unchanged.
DISPLAY = os.environ.get("WECHAT_DISPLAY", ":1")
PREFERRED_WINDOW_ID = os.environ.get("WECHAT_WINDOW_ID", "").strip()
MIN_CHAT_WINDOW_WIDTH = 600
MIN_CHAT_WINDOW_HEIGHT = 500


def run(args: list[str], input_text: str | None = None, check: bool = True) -> subprocess.CompletedProcess:
    env = None
    full_args = args
    if args and args[0] in {"xdotool", "xclip", "xprop"}:
        env = {**os.environ, "DISPLAY": DISPLAY}
    result = subprocess.run(
        full_args,
        input=input_text,
        text=True,
        capture_output=True,
        env=env,
    )
    if check and result.returncode != 0:
        raise RuntimeError((result.stderr or result.stdout or f"{args[0]} failed").strip())
    return result


def b64_decode(value: str) -> str:
    if not value:
        return ""
    return base64.b64decode(value.encode("ascii")).decode("utf-8")


def xdotool(*args: str, check: bool = True) -> str:
    result = run(["xdotool", *args], check=check)
    return result.stdout.strip()


def xprop(window_id: str) -> str:
    return run(["xprop", "-id", window_id, "WM_CLASS", "_NET_WM_NAME", "WM_NAME"], check=False).stdout


def window_geometry(window_id: str) -> dict:
    output = xdotool("getwindowgeometry", "--shell", window_id)
    parsed: dict[str, int | str] = {"window_id": window_id}
    for line in output.splitlines():
        if "=" not in line:
            continue
        key, raw = line.split("=", 1)
        try:
            parsed[key.lower()] = int(raw)
        except ValueError:
            parsed[key.lower()] = raw
    return parsed


def window_name(window_id: str) -> str:
    return xdotool("getwindowname", window_id, check=False).strip()


def close_non_main_wechat_windows() -> None:
    result = run(["xdotool", "search", "--onlyvisible", "--name", "微信"], check=False)
    for window_id in result.stdout.splitlines():
        window_id = window_id.strip()
        if not window_id:
            continue
        try:
            geom = window_geometry(window_id)
            props = xprop(window_id)
        except Exception:
            continue
        width = int(geom.get("width") or 0)
        height = int(geom.get("height") or 0)
        if width * height < 20_000:
            continue
        if "wechat" in props:
            continue
        xdotool("windowclose", window_id, check=False)
        sleep_seconds(0.2)


def find_main_window() -> dict:
    if PREFERRED_WINDOW_ID:
        try:
            geom = window_geometry(PREFERRED_WINDOW_ID)
            width = int(geom.get("width") or 0)
            height = int(geom.get("height") or 0)
            props = xprop(PREFERRED_WINDOW_ID)
        except Exception as exc:
            raise RuntimeError(f"指定的微信窗口不可用: {PREFERRED_WINDOW_ID}") from exc
        if width < 240 or height < 320 or "wechat" not in props.lower():
            raise RuntimeError(f"指定窗口不是可控制的微信主聊天窗口: {PREFERRED_WINDOW_ID}")
        geom["name"] = window_name(PREFERRED_WINDOW_ID)
        geom["area"] = width * height
        return geom
    close_non_main_wechat_windows()
    result = run(["xdotool", "search", "--onlyvisible", "--class", "wechat"], check=False)
    candidates: list[dict] = []
    for window_id in result.stdout.splitlines():
        if not window_id.strip():
            continue
        geom = window_geometry(window_id.strip())
        width = int(geom.get("width") or 0)
        height = int(geom.get("height") or 0)
        name = window_name(window_id.strip())
        props = xprop(window_id.strip())
        if name != "微信" or width < 240 or height < 320 or "wechat" not in props:
            continue
        geom["name"] = name
        geom["area"] = width * height
        candidates.append(geom)
    if not candidates:
        raise RuntimeError("未找到可控制的微信主聊天窗口")
    return max(candidates, key=lambda item: int(item.get("area") or 0))


def chat_window_ready(window: dict) -> bool:
    return (
        int(window.get("width") or 0) >= MIN_CHAT_WINDOW_WIDTH
        and int(window.get("height") or 0) >= MIN_CHAT_WINDOW_HEIGHT
    )


def require_chat_window(window: dict) -> None:
    if not chat_window_ready(window):
        raise RuntimeError(
            "微信窗口当前是登录/非聊天界面；请先完成登录并恢复主聊天窗口，发送已拒绝"
        )


def sleep_seconds(seconds: float) -> None:
    if seconds > 0:
        time.sleep(seconds)


def start_clipboard(text: str) -> subprocess.Popen:
    proc = subprocess.Popen(
        ["xclip", "-selection", "clipboard", "-loops", "8"],
        stdin=subprocess.PIPE,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "DISPLAY": DISPLAY},
    )
    assert proc.stdin is not None
    proc.stdin.write(text)
    proc.stdin.close()
    return proc


def start_image_clipboard(path: str) -> subprocess.Popen:
    if not os.path.exists(path):
        raise RuntimeError(f"图片文件不存在: {path}")
    mime = "image/png" if path.lower().endswith(".png") else "image/jpeg"
    proc = subprocess.Popen(
        ["xclip", "-selection", "clipboard", "-target", mime, "-loops", "8", "-i", path],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.PIPE,
        text=True,
        env={**os.environ, "DISPLAY": DISPLAY},
    )
    return proc


def finish_clipboard(proc: subprocess.Popen) -> None:
    try:
        proc.wait(timeout=0.6)
    except subprocess.TimeoutExpired:
        proc.terminate()
        try:
            proc.wait(timeout=1)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=1)
        return
    if proc.returncode not in (0, None):
        stderr = ""
        if proc.stderr:
            try:
                stderr = proc.stderr.read()
            except Exception:
                stderr = ""
        raise RuntimeError((stderr or "剪贴板写入失败").strip())


def paste_text(text: str) -> None:
    proc = start_clipboard(text)
    sleep_seconds(0.12)
    key("ctrl+v")
    sleep_seconds(0.35)
    finish_clipboard(proc)


def type_text(text: str, delay_ms: int = 18) -> None:
    if not text:
        return
    xdotool("type", "--clearmodifiers", "--delay", str(max(0, delay_ms)), text)
    sleep_seconds(0.12)


def paste_image(path: str) -> None:
    proc = start_image_clipboard(path)
    sleep_seconds(0.18)
    key("ctrl+v")
    sleep_seconds(0.6)
    finish_clipboard(proc)


def read_clipboard_text() -> str:
    try:
        result = subprocess.run(
            ["xclip", "-selection", "clipboard", "-o"],
            text=True,
            capture_output=True,
            timeout=2,
            env={**os.environ, "DISPLAY": DISPLAY},
        )
    except subprocess.TimeoutExpired:
        return ""
    return result.stdout if result.returncode == 0 else ""


def verify_focused_text(expected: str) -> dict:
    key("ctrl+a")
    sleep_seconds(0.08)
    key("ctrl+c")
    sleep_seconds(0.18)
    copied = read_clipboard_text()
    key("Right")
    return {
        "ok": copied == expected,
        "copied_length": len(copied),
        "expected_length": len(expected),
        "copied_preview": copied[:80],
    }


def verify_mention_text(display_name: str, alias: str, body: str) -> dict:
    key("ctrl+a")
    sleep_seconds(0.08)
    key("ctrl+c")
    sleep_seconds(0.18)
    copied = read_clipboard_text()
    key("Right")
    compact = copied.replace("\u2005", " ").replace("\u2004", " ").strip()
    display = display_name.strip()
    alias_text = alias.strip().lstrip("@")
    body_text = body.strip()
    has_body = bool(body_text and body_text in compact)
    has_display_mention = bool(display and f"@{display}" in compact)
    has_raw_alias = bool(alias_text and f"@{alias_text}" in compact)
    return {
        "ok": bool(has_body and has_display_mention and not has_raw_alias),
        "copied_length": len(copied),
        "copied_preview": copied[:120],
        "has_body": has_body,
        "has_display_mention": has_display_mention,
        "has_raw_alias": has_raw_alias,
        "expected_display": display,
        "alias": alias_text,
    }


def click(window: dict, rel_x: int, rel_y: int) -> None:
    x = int(window["x"]) + int(rel_x)
    y = int(window["y"]) + int(rel_y)
    xdotool("mousemove", str(x), str(y), "click", "1")


def key(*keys: str) -> None:
    xdotool("key", *keys)


def activate(window: dict) -> None:
    xdotool("windowactivate", "--sync", str(window["window_id"]))
    xdotool("windowraise", str(window["window_id"]), check=False)
    sleep_seconds(0.15)


def chat_tab_point(window: dict) -> tuple[int, int]:
    width = int(window.get("width") or 0)
    if width <= 360:
        return 28, 92
    return 31, 104


def search_box_point(window: dict) -> tuple[int, int]:
    width = int(window.get("width") or 0)
    if width <= 360:
        return max(70, int(width * 0.42)), 42
    return 115, 42


def clear_focused_text() -> None:
    key("ctrl+a")
    sleep_seconds(0.05)
    key("BackSpace")
    sleep_seconds(0.08)


def chat_search_query(chat_name: str) -> str:
    compact = re.sub(r"\s+", "", chat_name.strip())
    return compact or chat_name.strip()


def open_chat(chat_name: str, switch_delay: float) -> dict:
    chat_name = chat_name.strip()
    if not chat_name:
        raise RuntimeError("缺少目标群名")
    raise RuntimeError(
        "当前 X11 控制器无法验证搜索结果是否为精确目标聊天；已拒绝切换和发送"
    )


def paste_active(text: str, send: bool, send_delay: float) -> dict:
    if not text.strip():
        raise RuntimeError("回复内容为空")
    window = find_main_window()
    require_chat_window(window)
    activate(window)
    width = int(window["width"])
    height = int(window["height"])
    input_x = max(80, int(width * 0.57))
    input_y = max(120, height - 70)
    click(window, input_x, input_y)
    sleep_seconds(0.12)
    clear_focused_text()
    paste_text(text)
    sleep_seconds(0.15)
    input_verify = verify_focused_text(text)
    if not input_verify.get("ok"):
        raise RuntimeError("微信输入框内容校验失败，粘贴内容未出现在当前输入框")
    if send:
        sleep_seconds(max(send_delay, 0))
        key("Return")
    return {
        "window": window,
        "sent": bool(send),
        "send_delay_seconds": send_delay if send else 0,
        "input": {"x": input_x, "y": input_y},
        "input_verify": input_verify,
    }


def strip_plain_mention_prefix(text: str, display_name: str) -> str:
    body = text.strip()
    name = display_name.strip()
    if name and body.startswith(f"@{name}"):
        body = body[len(name) + 1 :].lstrip(" \t\r\n:：,，")
    return body


def paste_mention_active(text: str, mention_alias: str, mention_display: str, send: bool, send_delay: float) -> dict:
    body = strip_plain_mention_prefix(text, mention_display)
    if not body:
        raise RuntimeError("回复内容为空")
    alias = mention_alias.strip().lstrip("@")
    if not alias:
        raise RuntimeError("缺少可用于蓝色@的 alias")
    window = find_main_window()
    require_chat_window(window)
    activate(window)
    width = int(window["width"])
    height = int(window["height"])
    input_x = max(80, int(width * 0.57))
    input_y = max(120, height - 70)
    click(window, input_x, input_y)
    sleep_seconds(0.12)
    clear_focused_text()

    attempts = []
    input_verify = {}
    selected_strategy = ""
    selected_point = {}

    clear_focused_text()
    paste_text(f"@{alias}")
    sleep_seconds(0.82)
    key("Return")
    sleep_seconds(0.45)
    paste_text(f" {body}")
    sleep_seconds(0.18)
    input_verify = verify_mention_text(mention_display, alias, body)
    attempts.append({"strategy": "paste_alias_return_then_body", "candidate_click": None, "input_verify": input_verify})
    if input_verify.get("ok"):
        selected_strategy = "paste_alias_return_then_body"
    else:
        clear_focused_text()
        sleep_seconds(0.12)
        candidate_points = [
            ("near_caret_high", min(width - 120, max(120, input_x + 54)), max(80, input_y - 94)),
            ("near_caret_low", min(width - 120, max(120, input_x + 54)), max(80, input_y - 60)),
            ("caret_left", min(width - 120, max(120, input_x - 8)), max(80, input_y - 78)),
            ("legacy_left", max(170, int(width * 0.34)), max(80, input_y - 55)),
        ]
        for strategy, candidate_x, candidate_y in candidate_points:
            clear_focused_text()
            paste_text(f"@{alias}")
            sleep_seconds(0.82)
            click(window, candidate_x, candidate_y)
            sleep_seconds(0.48)
            paste_text(f" {body}")
            sleep_seconds(0.18)
            input_verify = verify_mention_text(mention_display, alias, body)
            attempt = {
                "strategy": strategy,
                "candidate_click": {"x": candidate_x, "y": candidate_y},
                "input_verify": input_verify,
            }
            attempts.append(attempt)
            if input_verify.get("ok"):
                selected_strategy = strategy
                selected_point = attempt["candidate_click"]
                break
            clear_focused_text()
            sleep_seconds(0.12)

    if not input_verify.get("ok"):
        clear_focused_text()
        raise RuntimeError(f"微信蓝色@校验失败，已清空输入框，拒绝发送裸 alias；候选点击尝试 {len(attempts)} 次")

    if send:
        sleep_seconds(max(send_delay, 0))
        key("Return")
    return {
        "window": window,
        "sent": bool(send),
        "send_delay_seconds": send_delay if send else 0,
        "input": {"x": input_x, "y": input_y},
        "mention": {
            "alias": alias,
            "display": mention_display,
            "body_length": len(body),
            "strategy": f"paste_alias_click_candidate_then_body:{selected_strategy}",
            "candidate_click": selected_point,
            "attempts": attempts,
        },
        "input_verify": input_verify,
    }


def paste_image_active(path: str, send: bool, send_delay: float) -> dict:
    if not os.path.exists(path):
        raise RuntimeError(f"图片文件不存在: {path}")
    window = find_main_window()
    require_chat_window(window)
    activate(window)
    width = int(window["width"])
    height = int(window["height"])
    input_x = max(80, int(width * 0.57))
    input_y = max(120, height - 70)
    click(window, input_x, input_y)
    sleep_seconds(0.12)
    clear_focused_text()
    paste_image(path)
    if send:
        sleep_seconds(max(send_delay, 0))
        key("Return")
    return {
        "window": window,
        "sent": bool(send),
        "send_delay_seconds": send_delay if send else 0,
        "input": {"x": input_x, "y": input_y},
        "image_path": path,
    }


def submit_active(send_delay: float) -> dict:
    window = find_main_window()
    require_chat_window(window)
    activate(window)
    sleep_seconds(max(send_delay, 0))
    key("Return")
    return {"window": window, "sent": True, "send_delay_seconds": send_delay}


def focus_active() -> dict:
    window = find_main_window()
    activate(window)
    return {"window": window, "focused": True}


def window_status() -> dict:
    window = find_main_window()
    ready = chat_window_ready(window)
    return {"window": window, "available": True, "chat_ready": ready, "login_required": not ready}


def clear_active() -> dict:
    window = find_main_window()
    activate(window)
    width = int(window["width"])
    height = int(window["height"])
    click(window, max(80, int(width * 0.57)), max(120, height - 70))
    sleep_seconds(0.1)
    clear_focused_text()
    return {"window": window, "cleared": True}


def login_guard_click(ack_ratio_y: float = 0.56, login_ratio_y: float = 0.76) -> dict:
    window = find_main_window()
    activate(window)
    width = int(window["width"])
    height = int(window["height"])
    center_x = max(30, width // 2)
    clicked = []

    click(window, center_x, max(40, int(height * ack_ratio_y)))
    sleep_seconds(0.9)
    clicked.append({"target": "ack", "x": center_x, "y": int(height * ack_ratio_y)})

    click(window, center_x, max(40, int(height * login_ratio_y)))
    sleep_seconds(0.8)
    clicked.append({"target": "login", "x": center_x, "y": int(height * login_ratio_y)})

    return {
        "window": window,
        "clicked": clicked,
        "ack_ratio_y": ack_ratio_y,
        "login_ratio_y": login_ratio_y,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "action",
        choices=["open", "paste", "mention-paste", "image", "submit", "focus", "status", "clear", "login-guard-click"],
    )
    parser.add_argument("--chat-name-b64", default="")
    parser.add_argument("--text-b64", default="")
    parser.add_argument("--mention-alias-b64", default="")
    parser.add_argument("--mention-display-b64", default="")
    parser.add_argument("--image-path-b64", default="")
    parser.add_argument("--send", action="store_true")
    parser.add_argument("--switch-delay", type=float, default=1.0)
    parser.add_argument("--send-delay", type=float, default=0.0)
    parser.add_argument("--ack-ratio-y", type=float, default=0.56)
    parser.add_argument("--login-ratio-y", type=float, default=0.76)
    args = parser.parse_args()
    try:
        if args.action == "open":
            payload = open_chat(b64_decode(args.chat_name_b64), args.switch_delay)
        elif args.action == "paste":
            payload = paste_active(b64_decode(args.text_b64), args.send, args.send_delay)
        elif args.action == "mention-paste":
            payload = paste_mention_active(
                b64_decode(args.text_b64),
                b64_decode(args.mention_alias_b64),
                b64_decode(args.mention_display_b64),
                args.send,
                args.send_delay,
            )
        elif args.action == "image":
            payload = paste_image_active(b64_decode(args.image_path_b64), args.send, args.send_delay)
        elif args.action == "submit":
            payload = submit_active(args.send_delay)
        elif args.action == "focus":
            payload = focus_active()
        elif args.action == "status":
            payload = window_status()
        elif args.action == "login-guard-click":
            payload = login_guard_click(args.ack_ratio_y, args.login_ratio_y)
        else:
            payload = clear_active()
        print(json.dumps({"ok": True, **payload}, ensure_ascii=False))
        return 0
    except Exception as exc:
        print(json.dumps({"ok": False, "error": str(exc)}, ensure_ascii=False), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
