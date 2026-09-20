#!/usr/bin/env python3
"""Sync local WeChat media caches into the read-only memory store.

The source WeChat directory is only read. Decoded or copied media is written to
runtime/media, and the memory SQLite database stores only references to those
owned copies.
"""

from __future__ import annotations

import argparse
import glob
import hashlib
import html
import json
import mimetypes
import os
import re
import shutil
import sqlite3
import struct
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.request import Request, urlopen

from Crypto.Cipher import AES
from Crypto.Util import Padding


V2_MAGIC = b"\x07\x08\x56\x32"
V2_MAGIC_FULL = b"\x07\x08V2\x08\x07"
V1_MAGIC_FULL = b"\x07\x08V1\x08\x07"
IMAGE_MAGIC = {
    "png": b"\x89PNG",
    "gif": b"GIF8",
    "webp": b"RIFF",
    "jpg": b"\xff\xd8\xff",
    "tif": b"II*\x00",
}
MAX_STICKER_BYTES = 10 * 1024 * 1024

# ``appmsg`` subtype that denotes a real file transfer (see
# ``memory.message_parse.APPMSG_FILE_TYPE``).
APPMSG_FILE_TYPE = "6"
# Local cache layout written by the WeChat client for received files:
#   <wechat_base_dir>/msg/file/<YYYY-MM>/<original filename>
FILE_CACHE_DIR = ("msg", "file")
FILE_MONTH_WINDOW_DAYS = 31
MAX_FILE_SCAN_ENTRIES = 20000
MAX_BASENAME_CHARS = 180
# Voice payloads live only inside the decrypted ``media_N.db`` shard, as a
# SILK blob in ``VoiceInfo.voice_data`` -- the client never writes a voice file
# to disk.  There is no SILK decoder in this image, so the original is
# published verbatim with its true container and mime type.
VOICE_MEDIA_DB_DIR = "message"
VOICE_MEDIA_DB_GLOB = "media_*.db"
VOICE_MIME_TYPE = "audio/silk"
VOICE_FILE_SUFFIX = ".silk"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def sqlite_ro(path: Path) -> sqlite3.Connection:
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    return conn


def normalize_chat_username(value: object) -> str:
    if value is None:
        return ""
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="strict")
    return str(value)



def aligned_aes_block_size(aes_size: int) -> int:
    if aes_size % 16:
        return aes_size + (16 - aes_size % 16)
    return aes_size + 16


def detect_image_format(data: bytes) -> str | None:
    if data[:3] == b"\xff\xd8\xff":
        return "jpg"
    if data[:4] == b"\x89PNG":
        return "png"
    if data[:3] == b"GIF":
        return "gif"
    if data[:4] == b"RIFF" and len(data) >= 12 and data[8:12] == b"WEBP":
        return "webp"
    if data[:2] == b"BM":
        return "bmp"
    if data[:4] == b"II*\x00":
        return "tif"
    if data[:4].lower() == b"wxgf":
        return "hevc"
    return None


def detect_xor_key(data: bytes) -> int | None:
    if len(data) < 4 or data[:4] == V2_MAGIC:
        return None
    for magic in IMAGE_MAGIC.values():
        key = data[0] ^ magic[0]
        if bytes(b ^ key for b in data[: len(magic)]) == magic:
            return key
    return None


def decrypt_v2_head(data: bytes, aes_key: str | bytes | None) -> tuple[bytes, int, int] | None:
    """AES-decrypt the head chunk of a V1/V2 ``.dat``.

    Returns ``(plain_head, body_offset, xor_size)``.  The head is independent
    of the trailing XOR byte, so it is decrypted once and then reassembled
    against candidate XOR bytes.
    """

    if len(data) < 15:
        return None
    sig = data[:6]
    if sig not in (V2_MAGIC_FULL, V1_MAGIC_FULL):
        return None
    if sig == V1_MAGIC_FULL:
        key = b"cfcd208495d565ef"
    elif aes_key:
        key = aes_key.encode("ascii")[:16] if isinstance(aes_key, str) else aes_key[:16]
    else:
        return None
    if len(key) < 16:
        return None

    aes_size, xor_size = struct.unpack_from("<LL", data, 6)
    aligned_size = aligned_aes_block_size(aes_size)
    offset = 15
    if offset + aligned_size > len(data):
        return None
    try:
        cipher = AES.new(key, AES.MODE_ECB)
        dec_aes = Padding.unpad(cipher.decrypt(data[offset : offset + aligned_size]), AES.block_size)
    except (ValueError, KeyError):
        return None
    return dec_aes, offset + aligned_size, xor_size


def assemble_v2(data: bytes, head: tuple[bytes, int, int], xor_key: int) -> tuple[bytes | None, str | None]:
    """Reassemble a V1/V2 ``.dat`` with ``xor_key`` applied to its tail region."""

    dec_aes, body_offset, xor_size = head
    raw_end = len(data) - xor_size
    raw_data = data[body_offset:raw_end] if body_offset < raw_end else b""
    dec_xor = bytes(b ^ xor_key for b in data[raw_end:])
    decrypted = dec_aes + raw_data + dec_xor
    fmt = detect_image_format(decrypted[:16])
    if not fmt:
        return None, None
    if fmt == "jpg" and len(decrypted) >= 2 and decrypted[-2:] != b"\xff\xd9":
        return None, None
    if fmt == "png" and b"IEND" not in decrypted[-12:]:
        return None, None
    return decrypted, fmt


def derive_xor_byte(data: bytes, plain_head: bytes, xor_size: int) -> int | None:
    """Recover the per-account XOR byte from known image plaintext at the tail.

    The XOR byte is account-specific and is not stored by upstream's key
    extractor, so the configured default can be wrong.  Only formats with a
    deterministic trailing signature are used; a blind 0..255 scan is
    deliberately avoided because the image magic lives in the AES region and
    would validate for every candidate byte.
    """

    if xor_size < 2 or len(data) < 2:
        return None
    if plain_head[:2] == b"\xff\xd8":
        # JPEG ends with FF D9.
        c1 = data[-2] ^ 0xFF
        c2 = data[-1] ^ 0xD9
        return c1 if c1 == c2 else None
    if plain_head[:4] == b"GIF8":
        # GIF ends with 00 3B.
        c1 = data[-2] ^ 0x00
        c2 = data[-1] ^ 0x3B
        return c1 if c1 == c2 else None
    if plain_head[:4] == b"\x89PNG" and xor_size >= 8 and len(data) >= 8:
        # PNG ends with the IEND chunk plus its CRC.
        expected = b"\x49\x45\x4e\x44\xae\x42\x60\x82"
        tail = data[-8:]
        candidate = tail[0] ^ expected[0]
        if all((tail[i] ^ candidate) == expected[i] for i in range(1, 8)):
            return candidate
    return None


def decrypt_v2(data: bytes, aes_key: str | bytes | None, xor_key: int) -> tuple[bytes | None, str | None]:
    head = decrypt_v2_head(data, aes_key)
    if head is None:
        return None, None
    decrypted, fmt = assemble_v2(data, head, xor_key)
    if decrypted:
        return decrypted, fmt
    derived = derive_xor_byte(data, head[0], head[2])
    if derived is None or derived == xor_key:
        return None, None
    return assemble_v2(data, head, derived)


def decrypt_dat(dat_path: Path, aes_key: str | bytes | None, xor_key: int) -> tuple[bytes | None, str | None]:
    data = dat_path.read_bytes()
    if data[:6] in (V2_MAGIC_FULL, V1_MAGIC_FULL):
        return decrypt_v2(data, aes_key, xor_key)
    key = detect_xor_key(data)
    if key is None:
        return None, None
    decrypted = bytes(b ^ key for b in data)
    return decrypted, detect_image_format(decrypted[:16])


def extract_md5_from_packed_info(blob: bytes | None) -> str | None:
    if not blob:
        return None
    marker = b"\x12\x22\x0a\x20"
    idx = blob.find(marker)
    if idx >= 0 and idx + len(marker) + 32 <= len(blob):
        candidate = blob[idx + len(marker) : idx + len(marker) + 32]
        try:
            value = candidate.decode("ascii")
            int(value, 16)
            return value
        except (UnicodeDecodeError, ValueError):
            pass
    match = re.search(rb"[0-9a-f]{32}", blob)
    return match.group(0).decode("ascii") if match else None


def parse_xml_payload(content: str | None) -> ET.Element | None:
    if not content:
        return None
    body = content.split(":\n", 1)[1] if ":\n<" in content else content
    body = body.strip()
    candidates = [body]
    if body.startswith("&lt;"):
        candidates.insert(0, html.unescape(body))
    for candidate in candidates:
        try:
            return ET.fromstring(candidate)
        except ET.ParseError:
            continue
    return None


def sticker_info(content: str | None) -> dict:
    root = parse_xml_payload(content)
    if root is None:
        return {}
    emoji = root.find(".//emoji")
    if emoji is None:
        return {}
    value = emoji.get("md5") or emoji.get("androidmd5")
    md5 = value if value and re.fullmatch(r"[0-9a-f]{32}", value) else None
    return {
        "md5": md5,
        "aeskey": emoji.get("aeskey") or "",
        "cdnurl": html.unescape(emoji.get("cdnurl") or ""),
        "encrypturl": html.unescape(emoji.get("encrypturl") or ""),
        "externurl": html.unescape(emoji.get("externurl") or ""),
        "thumburl": html.unescape(emoji.get("thumburl") or ""),
    }


def sticker_md5(content: str | None) -> str | None:
    return sticker_info(content).get("md5")


def image_dimensions(data: bytes, fmt: str | None) -> tuple[int | None, int | None]:
    try:
        if fmt == "png" and len(data) >= 24:
            return struct.unpack(">II", data[16:24])
        if fmt == "gif" and len(data) >= 10:
            return struct.unpack("<HH", data[6:10])
        if fmt == "webp" and len(data) >= 30:
            if data[12:16] == b"VP8X":
                w = int.from_bytes(data[24:27], "little") + 1
                h = int.from_bytes(data[27:30], "little") + 1
                return w, h
        if fmt == "jpg":
            i = 2
            while i + 9 < len(data):
                if data[i] != 0xFF:
                    i += 1
                    continue
                marker = data[i + 1]
                if marker in (0xC0, 0xC1, 0xC2, 0xC3, 0xC5, 0xC6, 0xC7, 0xC9, 0xCA, 0xCB, 0xCD, 0xCE, 0xCF):
                    return struct.unpack(">HH", data[i + 5 : i + 9])[::-1]
                size = struct.unpack(">H", data[i + 2 : i + 4])[0]
                i += 2 + size
    except Exception:
        return None, None
    return None, None


def safe_write(path: Path, data: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_bytes(data)
    tmp.replace(path)


def relative_to_runtime(path: Path, runtime_dir: Path) -> str:
    return str(path.resolve().relative_to(runtime_dir.resolve()))


def load_config(config_path: Path) -> dict:
    if not config_path.exists():
        return {}
    with config_path.open(encoding="utf-8") as f:
        return json.load(f)


def load_resource_map(resource_db: Path) -> dict[tuple[str, int], str]:
    if not resource_db.exists():
        return {}
    out: dict[tuple[str, int], str] = {}
    with sqlite_ro(resource_db) as conn:
        rows = conn.execute(
            """
            SELECT c.user_name AS chat_username, r.message_local_id, r.message_create_time, r.packed_info
            FROM MessageResourceInfo r
            JOIN ChatName2Id c ON c.rowid = r.chat_id
            WHERE r.message_local_type IN (3, 43)
               OR r.message_local_type % 4294967296 IN (3, 43)
            ORDER BY r.message_create_time
            """
        ).fetchall()
    for row in rows:
        md5 = extract_md5_from_packed_info(row["packed_info"])
        if md5:
            out[(normalize_chat_username(row["chat_username"]), int(row["message_local_id"]))] = md5
    return out


def ensure_media_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS message_media (
            message_uid TEXT PRIMARY KEY,
            chat_username TEXT NOT NULL,
            local_id INTEGER NOT NULL,
            media_type TEXT NOT NULL,
            original_md5 TEXT,
            source_path TEXT,
            media_path TEXT,
            thumb_path TEXT,
            mime_type TEXT,
            width INTEGER,
            height INTEGER,
            status TEXT NOT NULL,
            error TEXT,
            updated_at TEXT NOT NULL,
            FOREIGN KEY(message_uid) REFERENCES messages(message_uid)
        );

        CREATE INDEX IF NOT EXISTS idx_message_media_chat
            ON message_media(chat_username, local_id);
        CREATE INDEX IF NOT EXISTS idx_message_media_status
            ON message_media(status);
        """
    )


def find_dat_files(wechat_base_dir: Path, chat_username: object, media_md5: str) -> list[Path]:
    normalized_chat = normalize_chat_username(chat_username)
    if not normalized_chat or not media_md5:
        return []
    chat_hash = hashlib.md5(normalized_chat.encode("utf-8")).hexdigest()
    attach_dir = wechat_base_dir / "msg" / "attach" / chat_hash
    if not attach_dir.exists():
        return []
    return [Path(p) for p in sorted(glob.glob(str(attach_dir / "*" / "Img" / f"{media_md5}*.dat")))]


def choose_dat(dat_files: list[Path], prefer_thumb: bool) -> Path | None:
    if not dat_files:
        return None
    if prefer_thumb:
        for path in dat_files:
            if path.name.endswith("_t.dat"):
                return path
    for path in dat_files:
        if path.name.endswith("_h.dat"):
            return path
    for path in dat_files:
        stem = path.stem
        if not stem.endswith("_t") and not stem.endswith("_h"):
            return path
    if prefer_thumb:
        return dat_files[0]
    # A thumbnail is a distinct media role. When full-image mode is active,
    # keep the message pending instead of publishing low-resolution bytes as
    # if they were the original image.
    return None


def find_sticker_cache(wechat_base_dir: Path, media_md5: str) -> Path | None:
    candidates = sorted((wechat_base_dir / "cache").glob(f"*/Emoticon/{media_md5[:2]}/{media_md5}"))
    return candidates[-1] if candidates else None


def copy_if_displayable(source: Path, target_base: Path) -> tuple[str | None, str | None, int | None, int | None]:
    data = source.read_bytes()
    fmt = detect_image_format(data[:32])
    if not fmt or fmt == "hevc":
        return None, None, None, None
    target = target_base.with_suffix(f".{fmt}")
    if not target.exists() or target.stat().st_size != len(data):
        safe_write(target, data)
    width, height = image_dimensions(data, fmt)
    return str(target), fmt, width, height


def existing_media(target_base: Path) -> tuple[str | None, str | None, int | None, int | None]:
    for ext in ("gif", "png", "jpg", "jpeg", "webp", "bmp"):
        candidate = target_base.with_suffix(f".{ext}")
        if not candidate.exists():
            continue
        data = candidate.read_bytes()
        fmt = detect_image_format(data)
        if fmt:
            width, height = image_dimensions(data, fmt)
            return str(candidate), fmt, width, height
    return None, None, None, None


def fetch_url(url: str) -> bytes | None:
    if not url.startswith(("http://", "https://")):
        return None
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=15) as resp:
        data = resp.read(MAX_STICKER_BYTES + 1)
    if len(data) > MAX_STICKER_BYTES:
        return None
    return data if len(data) >= 4 else None


def decrypt_sticker_payload(data: bytes, aeskey: str) -> bytes | None:
    if not re.fullmatch(r"[0-9a-fA-F]{32}", aeskey or ""):
        return None
    key = bytes.fromhex(aeskey)
    try:
        decrypted = AES.new(key, AES.MODE_CBC, iv=key).decrypt(data)
    except ValueError:
        return None
    pad = decrypted[-1] if decrypted else 0
    if 1 <= pad <= 16 and decrypted[-pad:] == bytes([pad]) * pad:
        decrypted = decrypted[:-pad]
    return decrypted


def write_displayable_media(data: bytes, target_base: Path) -> tuple[str | None, str | None, int | None, int | None]:
    fmt = detect_image_format(data)
    if not fmt or fmt == "hevc":
        return None, None, None, None
    target = target_base.with_suffix(".jpg" if fmt == "jpeg" else f".{fmt}")
    if not target.exists() or target.stat().st_size != len(data):
        safe_write(target, data)
    width, height = image_dimensions(data, fmt)
    return str(target), fmt, width, height


def download_sticker(info: dict, target_base: Path) -> tuple[str | None, str | None, int | None, int | None, str | None]:
    for url_key in ("cdnurl", "externurl", "thumburl"):
        url = info.get(url_key) or ""
        if not url:
            continue
        try:
            data = fetch_url(url)
        except Exception:
            data = None
        if not data:
            continue
        copied, fmt, width, height = write_displayable_media(data, target_base)
        if copied:
            return copied, fmt, width, height, url_key

    encrypt_url = info.get("encrypturl") or ""
    aeskey = info.get("aeskey") or ""
    if encrypt_url and aeskey:
        try:
            encrypted = fetch_url(encrypt_url)
        except Exception:
            encrypted = None
        if encrypted:
            decrypted = decrypt_sticker_payload(encrypted, aeskey)
            if decrypted:
                copied, fmt, width, height = write_displayable_media(decrypted, target_base)
                if copied:
                    return copied, fmt, width, height, "encrypturl"
    return None, None, None, None, None


def upsert_media(conn: sqlite3.Connection, item: dict, runtime_dir: Path) -> None:
    now = utc_now_iso()
    values = {
        "message_uid": item["message_uid"],
        "chat_username": normalize_chat_username(item.get("chat_username")),
        "local_id": int(item["local_id"]),
        "media_type": item["media_type"],
        "original_md5": item.get("original_md5"),
        "source_path": item.get("source_path"),
        "media_path": item.get("media_path"),
        "thumb_path": item.get("thumb_path"),
        "mime_type": item.get("mime_type"),
        "width": item.get("width"),
        "height": item.get("height"),
        "status": item["status"],
        "error": item.get("error"),
        "updated_at": now,
    }
    for key in ("media_path", "thumb_path"):
        if values[key]:
            values[key] = relative_to_runtime(Path(values[key]), runtime_dir)
    conn.execute(
        """
        INSERT INTO message_media (
            message_uid, chat_username, local_id, media_type, original_md5,
            source_path, media_path, thumb_path, mime_type, width, height,
            status, error, updated_at
        )
        VALUES (
            :message_uid, :chat_username, :local_id, :media_type, :original_md5,
            :source_path, :media_path, :thumb_path, :mime_type, :width, :height,
            :status, :error, :updated_at
        )
        ON CONFLICT(message_uid) DO UPDATE SET
            chat_username=excluded.chat_username,
            local_id=excluded.local_id,
            media_type=excluded.media_type,
            original_md5=excluded.original_md5,
            source_path=excluded.source_path,
            media_path=excluded.media_path,
            thumb_path=excluded.thumb_path,
            mime_type=excluded.mime_type,
            width=excluded.width,
            height=excluded.height,
            status=excluded.status,
            error=excluded.error,
            updated_at=excluded.updated_at
        """,
        values,
    )


def sync_image(row: sqlite3.Row, args, resource_map: dict[tuple[str, int], str], cfg: dict) -> dict:
    chat_username = normalize_chat_username(row["chat_username"])
    local_id = int(row["local_id"])
    media_md5 = resource_map.get((chat_username, local_id))
    if not media_md5:
        return {
            "message_uid": row["message_uid"],
            "chat_username": chat_username,
            "local_id": local_id,
            "media_type": "image",
            "status": "missing_metadata",
            "error": "message_resource.db has no md5 for this image",
        }
    dat_files = find_dat_files(args.wechat_base_dir, chat_username, media_md5)
    selected = choose_dat(dat_files, prefer_thumb=args.prefer_thumbnails)
    if not selected:
        thumbnail_only = bool(dat_files) and all(path.name.endswith("_t.dat") for path in dat_files)
        if not thumbnail_only:
            return {
                "message_uid": row["message_uid"],
                "chat_username": chat_username,
                "local_id": local_id,
                "media_type": "image",
                "original_md5": media_md5,
                "status": "missing_file",
                "error": "local .dat cache not found",
            }
        thumbnail = choose_dat(dat_files, prefer_thumb=True)
        data, fmt = decrypt_dat(thumbnail, cfg.get("image_aes_key"), int(cfg.get("image_xor_key", 0x88)))
        if not data or not fmt or fmt == "hevc":
            return {
                "message_uid": row["message_uid"],
                "chat_username": chat_username,
                "local_id": local_id,
                "media_type": "image",
                "original_md5": media_md5,
                "source_path": str(thumbnail),
                "status": "original_pending",
                "error": "only thumbnail cache is available but it is not browser-displayable",
            }
        target = args.media_dir / "images" / f"{media_md5}_thumb.{fmt}"
        if not target.exists() or target.stat().st_size != len(data):
            safe_write(target, data)
        width, height = image_dimensions(data, fmt)
        return {
            "message_uid": row["message_uid"],
            "chat_username": chat_username,
            "local_id": local_id,
            "media_type": "image",
            "original_md5": media_md5,
            "source_path": str(thumbnail),
            "thumb_path": str(target),
            "mime_type": mimetypes.guess_type(str(target))[0] or "application/octet-stream",
            "width": width,
            "height": height,
            "status": "ready",
        }
    data, fmt = decrypt_dat(selected, cfg.get("image_aes_key"), int(cfg.get("image_xor_key", 0x88)))
    if not data or not fmt:
        return {
            "message_uid": row["message_uid"],
            "chat_username": chat_username,
            "local_id": local_id,
            "media_type": "image",
            "original_md5": media_md5,
            "source_path": str(selected),
            "status": "decode_failed",
            "error": "unable to decode local .dat cache",
        }
    target = args.media_dir / "images" / f"{media_md5}.{fmt}"
    if not target.exists() or target.stat().st_size != len(data):
        safe_write(target, data)
    width, height = image_dimensions(data, fmt)
    return {
        "message_uid": row["message_uid"],
        "chat_username": chat_username,
        "local_id": local_id,
        "media_type": "image",
        "original_md5": media_md5,
        "source_path": str(selected),
        "media_path": str(target),
        "mime_type": mimetypes.guess_type(str(target))[0] or "application/octet-stream",
        "width": width,
        "height": height,
        "status": "ready" if fmt != "hevc" else "unsupported_hevc",
        "error": None if fmt != "hevc" else "decoded HEVC is not browser-displayable yet",
    }


def sync_sticker(row: sqlite3.Row, args) -> dict:
    info = sticker_info(row["message_content"])
    media_md5 = info.get("md5")
    chat_username = normalize_chat_username(row["chat_username"])
    local_id = int(row["local_id"])
    base = {
        "message_uid": row["message_uid"],
        "chat_username": chat_username,
        "local_id": local_id,
        "media_type": "sticker",
        "original_md5": media_md5,
    }
    if not media_md5:
        return {**base, "status": "missing_metadata", "error": "sticker md5 not found in XML"}
    target_base = args.media_dir / "stickers" / media_md5
    copied, fmt, width, height = existing_media(target_base)
    if copied:
        return {
            **base,
            "media_path": copied,
            "mime_type": mimetypes.guess_type(copied)[0] or "application/octet-stream",
            "width": width,
            "height": height,
            "status": "ready",
        }
    source = find_sticker_cache(args.wechat_base_dir, media_md5)
    if source:
        copied, fmt, width, height = copy_if_displayable(source, target_base)
        if copied:
            return {
                **base,
                "source_path": str(source),
                "media_path": copied,
                "mime_type": mimetypes.guess_type(copied)[0] or "application/octet-stream",
                "width": width,
                "height": height,
                "status": "ready",
            }

    if not getattr(args, "download_stickers", True):
        return {
            **base,
            "status": "missing_file",
            "error": "local sticker cache not found; remote download disabled",
        }

    copied, fmt, width, height, via = download_sticker(info, target_base)
    if copied:
        return {
            **base,
            "source_path": via,
            "media_path": copied,
            "mime_type": mimetypes.guess_type(copied)[0] or "application/octet-stream",
            "width": width,
            "height": height,
            "status": "ready",
        }
    if source:
        return {
            **base,
            "source_path": str(source),
            "status": "encrypted_or_unknown",
            "error": "local sticker cache is not displayable and CDN fetch failed",
        }
    return {**base, "status": "missing_file", "error": "local sticker cache not found and CDN fetch failed"}


def sync_video(row: sqlite3.Row, args, resource_map: dict[tuple[str, int], str]) -> dict:
    chat_username = normalize_chat_username(row["chat_username"])
    local_id = int(row["local_id"])
    media_md5 = resource_map.get((chat_username, local_id))
    base = {
        "message_uid": row["message_uid"],
        "chat_username": chat_username,
        "local_id": local_id,
        "media_type": "video",
        "original_md5": media_md5,
    }
    if not media_md5:
        return {**base, "status": "missing_metadata", "error": "video md5 not found"}
    candidates = sorted((args.wechat_base_dir / "msg" / "video").glob(f"*/*{media_md5}*_thumb.jpg"))
    if not candidates:
        candidates = sorted((args.wechat_base_dir / "cache").glob(f"*/Message/*/Thumb/*thumb.jpg"))
    if not candidates:
        return {**base, "status": "missing_file", "error": "local video thumbnail not found"}
    source = candidates[-1]
    target = args.media_dir / "videos" / f"{media_md5}_thumb.jpg"
    if not target.exists() or target.stat().st_size != source.stat().st_size:
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(target.suffix + ".tmp")
        shutil.copyfile(source, tmp)
        tmp.replace(target)
    data = target.read_bytes()
    width, height = image_dimensions(data, "jpg")
    return {
        **base,
        "source_path": str(source),
        "thumb_path": str(target),
        "mime_type": "image/jpeg",
        "width": width,
        "height": height,
        "status": "ready",
    }


def safe_message_filename(raw: str | None) -> str:
    """Return a message-supplied filename only when it is safe as a path component.

    The title comes from WeChat message XML, which the remote party controls, so
    it may carry absolute paths, ``..``, separators or drive qualifiers.  Such a
    value is **refused** rather than silently reduced to its basename: a renamed
    file could otherwise match a different artifact that happens to share the
    basename.  Returns ``""`` when the value cannot be trusted.
    """

    name = str(raw or "")
    if not name or name != name.strip():
        return ""
    if name in (".", ".."):
        return ""
    if any(char in name for char in ("/", "\\", ":", "\x00")):
        return ""
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in name):
        return ""
    if len(name) > MAX_BASENAME_CHARS:
        return ""
    return name


def node_text(node: ET.Element | None, path: str) -> str:
    if node is None:
        return ""
    found = node.find(path)
    return str(found.text or "").strip() if found is not None else ""


def file_attachment_info(content: str | None) -> dict:
    """Return the file-transfer metadata of a message, or ``{}``.

    An empty result means the message is **not** a file transfer (url share,
    quote reply, merged chat records, ...) and therefore must not receive a
    media reference at all.
    """

    root = parse_xml_payload(content)
    if root is None:
        return {}
    appmsg = root.find(".//appmsg")
    if appmsg is None or node_text(appmsg, "type") != APPMSG_FILE_TYPE:
        return {}
    attach = appmsg.find("appattach")
    try:
        declared = int(node_text(attach, "totallen"))
    except (TypeError, ValueError):
        declared = 0
    return {
        "title": node_text(appmsg, "title"),
        "declared_size": declared if declared > 0 else None,
        "fileext": node_text(attach, "fileext"),
    }


def voice_message_info(content: str | None) -> dict:
    root = parse_xml_payload(content)
    if root is None:
        return {}
    voice = root.find(".//voicemsg")
    if voice is None:
        return {}
    try:
        declared = int(voice.get("length") or 0)
    except (TypeError, ValueError):
        declared = 0
    return {
        "voiceformat": str(voice.get("voiceformat") or ""),
        "declared_length": declared,
    }


def file_month_candidates(create_time: int | None) -> list[str]:
    try:
        base = datetime.fromtimestamp(int(create_time or 0), timezone.utc)
    except (TypeError, ValueError, OSError, OverflowError):
        return []
    months = [(base + timedelta(days=offset)).strftime("%Y-%m") for offset in (0, -FILE_MONTH_WINDOW_DAYS, FILE_MONTH_WINDOW_DAYS)]
    return list(dict.fromkeys(months))


def file_name_matches(candidate: str, title: str) -> bool:
    """Exact match, or the ``name (N).ext`` variant WeChat writes for duplicates."""

    if candidate == title:
        return True
    stem, ext = os.path.splitext(title)
    if not stem:
        return False
    return bool(re.fullmatch(re.escape(stem) + r" ?\(\d+\)" + re.escape(ext), candidate))


def list_cache_dir(directory: Path, cache: dict) -> list[Path]:
    key = str(directory)
    cached = cache.get(key)
    if cached is None:
        try:
            cached = [entry for entry in directory.iterdir() if entry.is_file()]
        except OSError:
            cached = []
        cache[key] = cached
    return cached


def find_file_candidates(wechat_base_dir: Path, title: str, create_time: int | None, cache: dict) -> list[Path]:
    """Locate local copies of ``title`` under ``msg/file``.

    The month directory is derived from the message timestamp first (the client
    files a download under the month it arrived); a full walk of ``msg/file``
    is the bounded fallback.  No other directory is ever searched, and a
    candidate is only accepted if its name really matches the message title.
    """

    root = wechat_base_dir.joinpath(*FILE_CACHE_DIR)
    if not root.is_dir():
        return []
    hits: list[Path] = []
    for month in file_month_candidates(create_time):
        month_dir = root / month
        if not month_dir.is_dir():
            continue
        for entry in sorted(list_cache_dir(month_dir, cache)):
            if file_name_matches(entry.name, title):
                hits.append(entry)
    if hits:
        return hits
    return [entry for entry in sorted(list_cache_dir(root, cache)) if file_name_matches(entry.name, title)]


def write_original_bytes(target: Path, data: bytes) -> Path | None:
    """Write original bytes atomically and verify the copy before publishing."""

    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists() or target.stat().st_size != len(data):
            safe_write(target, data)
        if target.stat().st_size != len(data):
            return None
    except OSError:
        return None
    return target


def copy_original_file(source: Path, target: Path) -> Path | None:
    """Copy an original file verbatim (streamed) and verify the byte count."""

    try:
        source_size = source.stat().st_size
        target.parent.mkdir(parents=True, exist_ok=True)
        if not target.exists() or target.stat().st_size != source_size:
            staging = target.parent / f"{target.name}.tmp"
            shutil.copyfile(source, staging)
            staging.replace(target)
        if target.stat().st_size != source_size:
            return None
    except OSError:
        return None
    return target


def load_voice_index(decrypted_dir: Path) -> dict[tuple[str, int, int], bytes]:
    """Index original voice payloads from the decrypted ``VoiceInfo`` table.

    Returns ``{(chat_username, create_time, local_id): voice_data}``.  The
    upstream client stores voice as a SILK blob inside the encrypted
    ``media_N.db`` shard rather than as a file, so this table is the only place
    an original voice artifact exists.
    """

    index: dict[tuple[str, int, int], bytes] = {}
    message_dir = decrypted_dir / VOICE_MEDIA_DB_DIR
    if not message_dir.is_dir():
        return index
    for db_path in sorted(message_dir.glob(VOICE_MEDIA_DB_GLOB)):
        try:
            with sqlite_ro(db_path) as conn:
                names = {
                    int(row["rowid"]): str(row["user_name"])
                    for row in conn.execute("SELECT rowid, user_name FROM Name2Id")
                }
                for row in conn.execute("SELECT chat_name_id, create_time, local_id, voice_data FROM VoiceInfo"):
                    data = row["voice_data"]
                    chat_username = names.get(int(row["chat_name_id"] or 0))
                    if not data or not chat_username:
                        continue
                    index[(chat_username, int(row["create_time"]), int(row["local_id"]))] = bytes(data)
        except (sqlite3.Error, KeyError, TypeError):
            continue
    return index


def sync_file(row: sqlite3.Row, args, cache: dict) -> dict | None:
    """Build the media reference for a genuine file transfer.

    Returns ``None`` when the message is not a file transfer at all, so that
    url shares, quote replies and merged chat records stay non-media messages.
    A file is only ever published as ``ready`` once the original bytes have
    been located, size-verified and materialised under ``runtime/media``.
    """

    info = file_attachment_info(row["message_content"])
    if not info:
        return None
    base = {
        "message_uid": row["message_uid"],
        "chat_username": row["chat_username"],
        "local_id": row["local_id"],
        "media_type": "file",
    }
    title = safe_message_filename(info.get("title"))
    if not title:
        return {**base, "status": "missing_metadata", "error": "file message carries no usable filename"}
    declared_size = info.get("declared_size")
    candidates = find_file_candidates(args.wechat_base_dir, title, row["create_time"], cache)
    source: Path | None = None
    for candidate in candidates:
        try:
            size = candidate.stat().st_size
        except OSError:
            continue
        if declared_size is not None and size != declared_size:
            continue
        # A zero-byte local copy cannot be distinguished from a truncated or
        # placeholder download, so it is never published as the original.
        if size <= 0:
            continue
        source = candidate
        break
    if source is None:
        return {
            **base,
            "status": "missing_file",
            "error": f"original {title!r} is not present in the local cache with the declared size",
        }
    target = args.media_dir / "files" / str(row["message_uid"]) / title
    copied = copy_original_file(source, target)
    if copied is None:
        return {**base, "source_path": str(source), "status": "missing_file", "error": "unable to materialise the original file bytes"}
    return {
        **base,
        "source_path": str(source),
        "media_path": str(copied),
        "mime_type": mimetypes.guess_type(title)[0] or "application/octet-stream",
        "status": "ready",
    }


def sync_voice(row: sqlite3.Row, args, voice_index: dict) -> dict:
    """Build the media reference for a voice message from the ``VoiceInfo`` blob."""

    base = {
        "message_uid": row["message_uid"],
        "chat_username": row["chat_username"],
        "local_id": row["local_id"],
        "media_type": "voice",
    }
    meta = voice_message_info(row["message_content"])
    if not meta:
        return {**base, "status": "missing_metadata", "error": "voice message carries no voicemsg payload"}
    key = (str(row["chat_username"]), int(row["create_time"]), int(row["local_id"]))
    data = voice_index.get(key)
    if not data:
        return {**base, "status": "missing_file", "error": "decrypted VoiceInfo holds no original for this message"}
    declared = int(meta.get("declared_length") or 0)
    if declared and len(data) != declared:
        return {
            **base,
            "status": "missing_file",
            "error": f"VoiceInfo payload is {len(data)} bytes but the message declares {declared}",
        }
    target = args.media_dir / "voice" / f"{row['message_uid']}{VOICE_FILE_SUFFIX}"
    written = write_original_bytes(target, data)
    if written is None:
        return {**base, "status": "missing_file", "error": "unable to materialise the original voice bytes"}
    return {
        **base,
        "source_path": f"{VOICE_MEDIA_DB_DIR}/VoiceInfo",
        "media_path": str(written),
        "mime_type": VOICE_MIME_TYPE,
        "status": "ready",
    }


def sync_media(args) -> dict:
    cfg = load_config(args.config_file)
    args.media_dir.mkdir(parents=True, exist_ok=True)
    resource_map = load_resource_map(args.decrypted_dir / "message" / "message_resource.db")
    started = time.time()

    with sqlite3.connect(args.memory_db, timeout=15) as conn:
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA busy_timeout=15000")
        ensure_media_schema(conn)
        rows = conn.execute(
            """
            SELECT message_uid, chat_username, local_id, type_label, message_content, create_time
            FROM messages
            WHERE type_label IN ('image', 'sticker', 'video', 'link_or_file', 'voice')
            ORDER BY create_time, local_id
            """
        ).fetchall()
        stats = {"ready": 0, "missing_metadata": 0, "missing_file": 0, "decode_failed": 0, "encrypted_or_unknown": 0, "unsupported_hevc": 0, "not_media": 0}
        scan_cache: dict = {}
        voice_index: dict | None = None
        for row in rows:
            label = row["type_label"]
            if label == "image":
                item = sync_image(row, args, resource_map, cfg)
            elif label == "sticker":
                item = sync_sticker(row, args)
            elif label == "video":
                item = sync_video(row, args, resource_map)
            elif label == "link_or_file":
                # ``link_or_file`` covers url shares, quote replies and merged
                # chat records as well as real file transfers; only the latter
                # receive a media reference.
                item = sync_file(row, args, scan_cache)
            else:
                if voice_index is None:
                    voice_index = load_voice_index(args.decrypted_dir)
                item = sync_voice(row, args, voice_index)
            if item is None:
                stats["not_media"] += 1
                continue
            stats[item["status"]] = stats.get(item["status"], 0) + 1
            upsert_media(conn, item, args.runtime_dir)

    try:
        os.chmod(args.memory_db, 0o600)
    except OSError:
        pass
    return {
        "media_dir": str(args.media_dir),
        "scanned": len(rows),
        "stats": stats,
        "elapsed_seconds": round(time.time() - started, 3),
        "finished_at": utc_now_iso(),
    }


def parse_args(argv: list[str] | None = None):
    parser = argparse.ArgumentParser(description="Sync local WeChat media into runtime/media")
    parser.add_argument("--memory-db", type=Path, default=Path("runtime/memory/wechat_memory.sqlite"))
    parser.add_argument("--decrypted-dir", type=Path, default=Path("runtime/wechat-decrypt/decrypted"))
    parser.add_argument("--wechat-base-dir", type=Path, default=Path("config/xwechat_files/PLEASE_SET_WECHAT_ACCOUNT_DIR"))
    parser.add_argument("--runtime-dir", type=Path, default=Path("runtime"))
    parser.add_argument("--media-dir", type=Path, default=Path("runtime/media"))
    parser.add_argument("--config-file", type=Path, default=Path("runtime/wechat-decrypt/config.json"))
    parser.add_argument("--prefer-full-images", action="store_true", help="Prefer full image .dat over thumbnails")
    parser.add_argument("--no-download-stickers", action="store_true", help="Do not fetch missing stickers from remote URLs")
    return parser.parse_args(argv)


def resolve_args(args):
    root = Path.cwd()
    for key in ("memory_db", "decrypted_dir", "wechat_base_dir", "runtime_dir", "media_dir", "config_file"):
        value = getattr(args, key)
        if not value.is_absolute():
            setattr(args, key, root / value)
    args.prefer_thumbnails = False
    args.download_stickers = not args.no_download_stickers
    return args


def main(argv: list[str] | None = None) -> int:
    args = resolve_args(parse_args(argv))
    print(json.dumps(sync_media(args), ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
