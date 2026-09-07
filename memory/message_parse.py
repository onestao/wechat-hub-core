"""Shared WeChat message parsing helpers for display and memory indexing."""

from __future__ import annotations

import html
import re
import xml.etree.ElementTree as ET


TAG_RE = re.compile(r"<[^>]+>")


def split_group_sender(content: str | None) -> tuple[str, str]:
    if not content:
        return "", ""
    if ":\n" in content:
        sender, body = content.split(":\n", 1)
        if re.fullmatch(r"[A-Za-z0-9_\-@.]+", sender or ""):
            return sender, body
    match = re.match(r"^([A-Za-z0-9_\-@.]+):(<\?xml|<msg|<sysmsg|<voipmsg)", content)
    if match:
        sender = match.group(1)
        return sender, content[len(sender) + 1 :]
    return "", content


def clean_text(value: str | None) -> str:
    text = html.unescape(str(value or ""))
    text = text.replace("\x00", " ")
    return re.sub(r"\s+", " ", text).strip()


def xml_node_text(node: ET.Element | None, path: str) -> str:
    if node is None:
        return ""
    found = node.find(path)
    return clean_text(found.text if found is not None else "")


def parse_xml(value: str | None) -> ET.Element | None:
    text = str(value or "").strip()
    if not text or "<" not in text:
        return None
    try:
        return ET.fromstring(text.encode("utf-8"))
    except ET.ParseError:
        return None


def extract_xml_text(value: str | None) -> str:
    root = parse_xml(value)
    if root is not None:
        pieces = []
        for path in (
            ".//appmsg/title",
            ".//appmsg/des",
            ".//appmsg/sourcedisplayname",
            ".//appmsg/appattach/fileext",
            ".//appmsg/appname",
        ):
            text = xml_node_text(root, path)
            if text:
                pieces.append(text)
        if pieces:
            return " ".join(dict.fromkeys(pieces))
    text = str(value or "")
    if "<" in text and ">" in text:
        cleaned = TAG_RE.sub(" ", html.unescape(text))
        return clean_text(cleaned)[:500]
    return clean_text(text)


def parse_refer_content(raw: str | None) -> str:
    text = clean_text(raw)
    if not text:
        return ""
    if "<" in text and ">" in text:
        return extract_xml_text(text)
    return text


def parse_app_message(body: str | None) -> dict:
    root = parse_xml(body)
    if root is None:
        return {"display_content": extract_xml_text(body), "semantic_text": extract_xml_text(body)}

    appmsg = root.find(".//appmsg")
    title = xml_node_text(appmsg, "title")
    description = xml_node_text(appmsg, "des")
    app_type = xml_node_text(appmsg, "type")
    url = xml_node_text(appmsg, "url")
    source_name = xml_node_text(appmsg, "sourcedisplayname")
    app_name = xml_node_text(appmsg, "appname")
    source_username = xml_node_text(appmsg, "sourceusername")
    app_id = xml_node_text(appmsg, "appid") or xml_node_text(appmsg, "weappinfo/appid")
    display = title or description or extract_xml_text(body)
    semantic_parts = [display]
    if source_name:
        semantic_parts.append(f"来源 {source_name}")
    if description and description != display:
        semantic_parts.append(description)
    result = {
        "display_content": display,
        "semantic_text": display,
        "app_type": app_type,
        "app_url": url,
        "app_source_name": source_name,
        "app_name": app_name,
        "app_source_username": source_username,
        "app_id": app_id,
    }

    refer = appmsg.find("refermsg") if appmsg is not None else None
    if refer is not None:
        quoted_sender = xml_node_text(refer, "displayname") or xml_node_text(refer, "chatusr")
        quoted_content = parse_refer_content(xml_node_text(refer, "content"))
        quoted_type = xml_node_text(refer, "type")
        quote = {
            "sender": quoted_sender,
            "content": quoted_content,
            "type": quoted_type,
            "create_time": xml_node_text(refer, "createtime"),
        }
        result["quote"] = quote
        result["semantic_type"] = "quote"
        if quoted_content:
            quoted_sender_desc = quoted_sender or xml_node_text(refer, "chatusr") or ""
            semantic_parts.append(f"引用 {quoted_sender_desc}: {quoted_content}" if quoted_sender_desc else f"引用: {quoted_content}")

    result["semantic_text"] = clean_text("；".join(part for part in semantic_parts if part))
    return result


def message_display_parts(
    message_content: str | None,
    compress_content: str | None,
    type_label: str | None,
    source: str | None = None,
) -> dict:
    content = message_content or compress_content or ""
    sender, body = split_group_sender(content)
    msg_type = type_label or "unknown"
    body = body or content
    result: dict = {"sender_hint": sender, "semantic_type": msg_type}

    if msg_type == "text":
        display = clean_text(body)
    elif msg_type == "image":
        display = "[图片]"
    elif msg_type == "video":
        display = "[视频]"
    elif msg_type == "sticker":
        display = "[表情]"
    elif msg_type == "system":
        display = clean_text(body) or "[系统消息]"
    elif msg_type == "voice":
        display = "[语音]"
    elif msg_type == "link_or_file":
        app = parse_app_message(body)
        result.update({key: value for key, value in app.items() if value not in (None, "")})
        display = app.get("display_content") or "[链接/文件]"
    else:
        display = extract_xml_text(body) or f"[{msg_type}]"

    result["display_content"] = display
    result["semantic_text"] = result.get("semantic_text") or display or extract_xml_text(source)
    return result


def message_index_text(row: dict) -> tuple[str, str]:
    parts = message_display_parts(
        row.get("message_content"),
        row.get("compress_content"),
        row.get("type_label"),
        row.get("source"),
    )
    text = parts.get("semantic_text") or parts.get("display_content") or ""
    text = clean_text(text)
    if len(text) > 3000:
        text = text[:3000]
    return parts.get("sender_hint", ""), text
