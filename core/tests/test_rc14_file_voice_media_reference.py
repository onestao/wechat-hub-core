"""RC.14 Core file / voice media-reference coverage (FMR-1 .. FMR-8).

The Core media pipeline only ever materialised references for
``image`` / ``sticker`` / ``video``.  A ``file`` or ``voice`` message therefore
had no ``message_media`` row, the projection attached no reference, and every
consumer that reads ``media_id`` / ``media_role`` / ``media_status`` at the top
level of the message failed closed -- permanently, not transiently.

Two independent defects are covered here:

* the extraction gap -- ``sync_media()`` had no ``file`` or ``voice`` branch;
* the classification gap -- upstream ``link_or_file`` is the staging label for
  *every* WeChat ``appmsg`` subtype, and the projection turned all of them into
  ``file`` (a media type) unless a url was present, so quote replies and merged
  chat records demanded a media reference that can never exist.
"""

from __future__ import annotations

import json
import shutil
import sqlite3
import sys
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace

CORE_ROOT = Path(__file__).resolve().parents[2]
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from core.normalize import import_account, _normalized_message  # noqa: E402
from core.registry import parse_account  # noqa: E402
from core.store import CoreStore  # noqa: E402
from memory import media_sync  # noqa: E402
from memory.message_parse import message_display_parts  # noqa: E402


# ---------------------------------------------------------------------------
# Sealed production evidence.
#
# The census row below is copied verbatim from the sealed RC.14 post-F3
# reprojection census (`work/rc14-efb-idempotency/tests/fixtures/
# rc14_post_f3_reprojection_stream.json`, sha256 697c207f1ceb073c40fa63743efe
# 53216f279a0974bd58af5078d3287373df3c).  It is the only ``file``-typed row in
# the whole 883-row window.  Nothing here writes to production.
#
# The message *body* is deliberately not reproduced: the census itself excludes
# message text and author identity.  The structural facts that decide the
# classification were read once, read-only, from the account staging store
# (which is not part of the protected Core database set) and are reproduced
# below as shape, not as content.
# ---------------------------------------------------------------------------
SEALED_274307 = {
    "cursor": 274307,
    "event_type": "message.created",
    "account_id": "f-live-a",
    "chat_id": "38808757431@chatroom",
    "message_id": "8745c1a931013b8941a0736d6fb890f1a4129129bd03b2b4842d09f7d5995d79",
    "type": "file",
    "direction": "incoming",
    "media_role": "",
    "media_status": "",
    "media_id": "",
}

#: Outer ``appmsg`` subtype observed for the sealed 274307 message, and the fact
#: that its ``appattach`` element carries no ``totallen`` (no attachment).
SEALED_274307_APPMSG_TYPE = "57"
SEALED_274307_HAS_ATTACHMENT_PAYLOAD = False

#: A structurally faithful rendering of the sealed payload: a quote reply whose
#: quoted content is itself an ``appmsg``.  Human text is replaced.
SEALED_274307_SHAPE = (
    '<msg><appmsg appid="" sdkver="0"><title>[redacted]</title><type>57</type>'
    "<appattach><cdnthumbaeskey /><aeskey /></appattach>"
    '<refermsg><type>49</type><fromusr>38808757431@chatroom</fromusr>'
    "<content>&lt;msg&gt;&lt;appmsg&gt;&lt;title&gt;[redacted]&lt;/title&gt;"
    "&lt;type&gt;19&lt;/type&gt;&lt;/appmsg&gt;&lt;/msg&gt;</content></refermsg>"
    "</appmsg></msg>"
)


def _file_payload(title: str, total: int) -> str:
    return (
        f'<msg><appmsg appid="wx0" sdkver="0"><title>{title}</title><des></des>'
        f"<type>{media_sync.APPMSG_FILE_TYPE}</type><url></url>"
        f"<appattach><totallen>{total}</totallen>"
        f"<fileext>{title.rpartition('.')[2]}</fileext>"
        "<attachid>@cdn_0</attachid></appattach></appmsg></msg>"
    )


def _voice_payload(declared: int) -> str:
    return (
        '<msg><voicemsg endflag="1" cancelflag="0" forwardflag="0" voiceformat="4"'
        f' voicelength="3388" length="{declared}" bufid="0" aeskey="0" /></msg>'
    )


def _link_payload(url: str) -> str:
    return (
        '<msg><appmsg appid="wx0" sdkver="0"><title>a link</title><des></des>'
        f"<type>5</type><url>{url}</url></appmsg></msg>"
    )


def _merged_records_payload() -> str:
    return (
        '<msg><appmsg appid="wx0" sdkver="0"><title>chat history</title>'
        "<type>19</type><appattach><cdnthumbaeskey /></appattach></appmsg></msg>"
    )


class FileVoiceMediaReferenceTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = CORE_ROOT / ".tmp" / f"rc14-filevoice-{uuid.uuid4().hex}"
        self.runtime_dir = self.root / "runtime"
        self.account_root = self.runtime_dir / "accounts" / "account-1"
        self.media_dir = self.account_root / "media"
        self.decrypted_dir = self.account_root / "wechat-decrypt" / "decrypted"
        self.memory_db = self.account_root / "memory" / "wechat_memory.sqlite"
        self.wechat_base_dir = self.root / "wechat"
        for directory in (self.media_dir, self.decrypted_dir / "message", self.memory_db.parent):
            directory.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    # ------------------------------------------------------------------
    # helpers
    # ------------------------------------------------------------------

    def _args(self) -> SimpleNamespace:
        return SimpleNamespace(
            memory_db=self.memory_db,
            decrypted_dir=self.decrypted_dir,
            wechat_base_dir=self.wechat_base_dir,
            runtime_dir=self.runtime_dir,
            media_dir=self.media_dir,
            config_file=self.root / "missing-config.json",
            prefer_thumbnails=False,
            download_stickers=False,
        )

    def _place_file(self, month: str, name: str, data: bytes) -> Path:
        directory = self.wechat_base_dir.joinpath(*media_sync.FILE_CACHE_DIR, month)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / name
        path.write_bytes(data)
        return path

    def _file_row(self, *, title: str, total: int, create_time: int = 1789605674, uid: str = "file-1") -> dict:
        return {
            "message_uid": uid,
            "chat_username": "chat-1",
            "local_id": 8,
            "type_label": "link_or_file",
            "message_content": _file_payload(title, total),
            "create_time": create_time,
        }

    def _normalized(self, row: dict, media: dict | None = None) -> dict:
        return _normalized_message("account-1", {**row, "compress_content": "", "source": "", "origin_source": 0}, {}, media=media)

    def _account(self):
        return parse_account(
            {
                "account_id": "account-1",
                "display_name": "Account One",
                "source_db_dir": str(self.root / "source" / "db_storage"),
                "wechat_base_dir": str(self.wechat_base_dir),
                "runtime_dir": str(self.account_root),
                "decrypted_dir": str(self.decrypted_dir),
                "memory_db": str(self.memory_db),
                "media_dir": str(self.media_dir),
            },
            root=self.root,
        )

    def _seed_staging(self, rows: list[dict]) -> None:
        conn = sqlite3.connect(self.memory_db)
        try:
            conn.execute("CREATE TABLE IF NOT EXISTS chats (username TEXT PRIMARY KEY, is_group INTEGER DEFAULT 0, display_name TEXT, updated_at TEXT)")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS messages (message_uid TEXT PRIMARY KEY, chat_username TEXT, local_id INTEGER,"
                " type_label TEXT, message_content TEXT, compress_content TEXT, source TEXT, origin_source INTEGER,"
                " create_time INTEGER, server_id TEXT, message_table TEXT)"
            )
            conn.execute("INSERT OR REPLACE INTO chats (username, is_group, display_name) VALUES ('chat-1', 0, 'Chat One')")
            for row in rows:
                conn.execute(
                    "INSERT OR REPLACE INTO messages (message_uid, chat_username, local_id, type_label, message_content,"
                    " compress_content, source, origin_source, create_time) VALUES (?,?,?,?,?,?,?,?,?)",
                    (
                        row["message_uid"],
                        row["chat_username"],
                        row["local_id"],
                        row["type_label"],
                        row["message_content"],
                        "",
                        "",
                        0,
                        row["create_time"],
                    ),
                )
            conn.commit()
        finally:
            conn.close()

    def _store(self) -> CoreStore:
        store = CoreStore(self.root / "core.sqlite")
        store.upsert_account("account-1", "Account One", state="ready")
        store.upsert_chat({"account_id": "account-1", "chat_id": "chat-1", "type": "private"})
        return store

    @staticmethod
    def _events(store: CoreStore, event_type: str) -> list[dict]:
        page = store.poll_events(after="0", limit=500)
        return [event["payload"] for event in page["events"] if event["event_type"] == event_type]

    def _media_rows(self) -> list[dict]:
        conn = sqlite3.connect(self.memory_db)
        conn.row_factory = sqlite3.Row
        try:
            return [dict(row) for row in conn.execute("SELECT * FROM message_media")]
        finally:
            conn.close()

    # ------------------------------------------------------------------
    # FMR-1 — file, original exists
    # ------------------------------------------------------------------

    def test_fmr_1_file_with_original_is_published_as_ready_original(self) -> None:
        payload = b"RC14 file payload\n"
        self._place_file("2026-09", "report.txt", payload)
        item = media_sync.sync_file(self._file_row(title="report.txt", total=len(payload)), self._args(), {})

        self.assertEqual(item["status"], "ready")
        self.assertEqual(item["media_type"], "file")
        self.assertEqual(item["mime_type"], "text/plain")
        copied = Path(item["media_path"])
        self.assertTrue(copied.is_file(), "the original must be materialised under runtime/media")
        self.assertEqual(copied.read_bytes(), payload, "bytes must be retrievable verbatim")
        self.assertTrue(copied.resolve().is_relative_to(self.runtime_dir.resolve()))

        normalized = self._normalized(self._file_row(title="report.txt", total=len(payload)), media={
            "media_id": item["message_uid"],
            "filename": copied.name,
            "mime_type": item["mime_type"],
            "role": "original",
            "status": "ready",
        })
        self.assertEqual(normalized["type"], "file")
        self.assertNotEqual(normalized["media_id"], "")
        self.assertEqual(normalized["media_role"], "original")
        self.assertEqual(normalized["media_status"], "ready")

    def test_fmr_1_file_media_row_is_served_with_original_role_and_ready_status(self) -> None:
        payload = b"0123456789"
        self._place_file("2026-09", "blob.bin", payload)
        store = self._store()
        item = media_sync.sync_file(self._file_row(title="blob.bin", total=len(payload)), self._args(), {})
        self.assertEqual(item["status"], "ready")
        store.upsert_media(
            {
                "account_id": "account-1",
                "media_id": item["message_uid"],
                "filename": Path(item["media_path"]).name,
                "mime_type": item["mime_type"],
                "local_path": item["media_path"],
                "disposition": "inline",
                "role": "original",
                "status": "ready",
            }
        )
        media = store.media("account-1", item["message_uid"])
        self.assertIsNotNone(media)
        self.assertEqual(media["role"], "original")
        self.assertEqual(media["status"], "ready")
        self.assertEqual(Path(media["local_path"]).read_bytes(), payload)

    # ------------------------------------------------------------------
    # FMR-2 — file, original unavailable
    # ------------------------------------------------------------------

    def test_fmr_2_missing_original_is_never_ready_and_never_uses_a_thumbnail(self) -> None:
        # Only a same-named thumbnail-ish decoy exists; no original download does.
        self._place_file("2026-09", "report.txt_thumb.jpg", b"decoy-thumbnail")
        item = media_sync.sync_file(self._file_row(title="report.txt", total=1024), self._args(), {})

        self.assertNotEqual(item["status"], "ready")
        self.assertEqual(item["status"], "missing_file")
        self.assertNotIn("media_path", item, "no artifact may be published when the original is absent")
        self.assertNotIn("thumb_path", item, "a thumbnail must never be substituted for the original")
        self.assertNotIn("source_path", item, "nothing was located, so nothing may be recorded as a source")

    def test_fmr_2_size_mismatch_is_not_ready(self) -> None:
        self._place_file("2026-09", "report.txt", b"short")
        item = media_sync.sync_file(self._file_row(title="report.txt", total=999999), self._args(), {})
        self.assertEqual(item["status"], "missing_file")
        self.assertIsNone(item.get("media_path"))

    def test_fmr_2_zero_byte_local_copy_is_not_ready(self) -> None:
        self._place_file("2026-09", "empty.txt", b"")
        item = media_sync.sync_file(self._file_row(title="empty.txt", total=0), self._args(), {})
        self.assertEqual(item["status"], "missing_file")
        self.assertIsNone(item.get("media_path"))

    def test_fmr_2_unsafe_filename_is_rejected_without_escaping_the_media_dir(self) -> None:
        for hostile in ("../../etc/passwd", "..\\..\\evil.txt", "C:evil.txt", "a/b.txt", "   "):
            item = media_sync.sync_file(self._file_row(title=hostile, total=8), self._args(), {})
            self.assertEqual(item["status"], "missing_metadata", f"{hostile!r} must be refused, not sanitised")
            self.assertNotIn("media_path", item)
        self.assertEqual(media_sync.safe_message_filename("../../etc/passwd"), "")
        self.assertEqual(media_sync.safe_message_filename("report final (1).pdf"), "report final (1).pdf")

    # ------------------------------------------------------------------
    # FMR-3 — file becomes available later
    # ------------------------------------------------------------------

    def test_fmr_3_pending_then_ready_emits_media_ready_exactly_once(self) -> None:
        row = self._file_row(title="later.pdf", total=11, uid="file-later")
        args = self._args()

        pending = media_sync.sync_file(row, args, {})
        self.assertEqual(pending["status"], "missing_file")

        # A not-ready row is never persisted into the Core media table, so the
        # message carries a truthful pending original and no bytes are served.
        store = self._store()
        pending_normalized = self._normalized(row, media={
            "media_id": row["message_uid"],
            "filename": row["message_uid"],
            "mime_type": "application/pdf",
            "role": "original",
            "status": pending["status"],
        })
        self.assertEqual(pending_normalized["media_role"], "original")
        self.assertEqual(pending_normalized["media_status"], "missing_file")
        store.upsert_message(pending_normalized)
        self.assertIsNone(store.media("account-1", row["message_uid"]))
        self.assertEqual(self._events(store, "media.ready"), [])

        # The original arrives.
        payload = b"hello world"
        self._place_file("2026-09", "later.pdf", payload)
        ready = media_sync.sync_file(row, args, {})
        self.assertEqual(ready["status"], "ready")

        media_payload = {
            "account_id": "account-1",
            "media_id": row["message_uid"],
            "filename": "later.pdf",
            "mime_type": ready["mime_type"],
            "local_path": ready["media_path"],
            "disposition": "inline",
            "role": "original",
            "status": "ready",
        }
        store.upsert_media(media_payload)
        self.assertEqual(len(self._events(store, "media.ready")), 1)

        # Re-running the sync cycle with identical bytes must not re-emit.
        self.assertEqual(store.upsert_media(media_payload), False)
        self.assertEqual(len(self._events(store, "media.ready")), 1)
        self.assertEqual(Path(ready["media_path"]).read_bytes(), payload)

    # ------------------------------------------------------------------
    # FMR-4 / FMR-5 — voice
    # ------------------------------------------------------------------

    def _seed_voice_db(self, entries: list[tuple[int, int, int, bytes]]) -> None:
        db_path = self.decrypted_dir / "message" / "media_0.db"
        conn = sqlite3.connect(db_path)
        try:
            conn.execute("CREATE TABLE IF NOT EXISTS Name2Id (user_name TEXT)")
            conn.execute(
                "CREATE TABLE IF NOT EXISTS VoiceInfo (chat_name_id INTEGER, create_time INTEGER, local_id INTEGER,"
                " svr_id INTEGER, voice_data BLOB, data_index TEXT DEFAULT '0')"
            )
            conn.execute("DELETE FROM Name2Id")
            conn.execute("DELETE FROM VoiceInfo")
            conn.execute("INSERT INTO Name2Id (rowid, user_name) VALUES (1, 'chat-1')")
            for create_time, local_id, svr_id, data in entries:
                conn.execute(
                    "INSERT INTO VoiceInfo (chat_name_id, create_time, local_id, svr_id, voice_data) VALUES (1,?,?,?,?)",
                    (create_time, local_id, svr_id, data),
                )
            conn.commit()
        finally:
            conn.close()

    @staticmethod
    def _silk_blob(length: int) -> bytes:
        return b"\x02#!SILK_V3" + b"\x00" * max(0, length - 10)

    def test_fmr_4_voice_original_is_published_as_ready_original(self) -> None:
        create_time, local_id = 1788681351, 324
        blob = self._silk_blob(6196)
        self._seed_voice_db([(create_time, local_id, 7, blob)])
        row = {
            "message_uid": "voice-1",
            "chat_username": "chat-1",
            "local_id": local_id,
            "type_label": "voice",
            "message_content": _voice_payload(len(blob)),
            "create_time": create_time,
        }
        index = media_sync.load_voice_index(self.decrypted_dir)
        item = media_sync.sync_voice(row, self._args(), index)

        self.assertEqual(item["status"], "ready")
        self.assertEqual(item["media_type"], "voice")
        self.assertEqual(item["mime_type"], media_sync.VOICE_MIME_TYPE)
        written = Path(item["media_path"])
        self.assertTrue(written.is_file())
        self.assertEqual(written.read_bytes(), blob, "the original SILK payload must be retrievable verbatim")
        self.assertTrue(written.name.endswith(media_sync.VOICE_FILE_SUFFIX))

        normalized = self._normalized(row, media={
            "media_id": "voice-1",
            "filename": written.name,
            "mime_type": item["mime_type"],
            "role": "original",
            "status": "ready",
        })
        self.assertEqual(normalized["type"], "voice")
        self.assertEqual(normalized["media_role"], "original")
        self.assertEqual(normalized["media_status"], "ready")

    def test_fmr_5_voice_without_original_is_fail_safe(self) -> None:
        row = {
            "message_uid": "voice-2",
            "chat_username": "chat-1",
            "local_id": 999,
            "type_label": "voice",
            "message_content": _voice_payload(5000),
            "create_time": 1788681351,
        }
        self._seed_voice_db([])
        index = media_sync.load_voice_index(self.decrypted_dir)
        item = media_sync.sync_voice(row, self._args(), index)
        self.assertEqual(item["status"], "missing_file")
        self.assertIsNone(item.get("media_path"))

    def test_fmr_5_voice_length_mismatch_is_fail_safe(self) -> None:
        blob = self._silk_blob(4000)
        self._seed_voice_db([(1788681351, 324, 7, blob)])
        row = {
            "message_uid": "voice-3",
            "chat_username": "chat-1",
            "local_id": 324,
            "type_label": "voice",
            "message_content": _voice_payload(6196),
            "create_time": 1788681351,
        }
        index = media_sync.load_voice_index(self.decrypted_dir)
        item = media_sync.sync_voice(row, self._args(), index)
        self.assertEqual(item["status"], "missing_file")
        self.assertIsNone(item.get("media_path"))

    def test_fmr_5_voice_without_decrypted_store_is_fail_safe(self) -> None:
        row = {
            "message_uid": "voice-4",
            "chat_username": "chat-1",
            "local_id": 1,
            "type_label": "voice",
            "message_content": _voice_payload(10),
            "create_time": 1,
        }
        item = media_sync.sync_voice(row, self._args(), media_sync.load_voice_index(self.decrypted_dir))
        self.assertEqual(item["status"], "missing_file")

    # ------------------------------------------------------------------
    # FMR-6 — image / sticker / video regression, and non-media appmsg types
    # ------------------------------------------------------------------

    def test_fmr_6_dispatch_preserves_image_sticker_video_and_adds_file_voice(self) -> None:
        payload = b"file-bytes"
        self._place_file("2026-09", "doc.txt", payload)
        self._seed_voice_db([(1788681351, 324, 7, self._silk_blob(6196))])
        self._seed_staging(
            [
                {"message_uid": "img-1", "chat_username": "chat-1", "local_id": 1, "type_label": "image", "message_content": "", "create_time": 1788681351},
                {"message_uid": "sti-1", "chat_username": "chat-1", "local_id": 2, "type_label": "sticker", "message_content": "<msg><emoji md5='%s'/></msg>" % ("b" * 32), "create_time": 1788681352},
                {"message_uid": "vid-1", "chat_username": "chat-1", "local_id": 3, "type_label": "video", "message_content": "", "create_time": 1788681353},
                {"message_uid": "file-1", "chat_username": "chat-1", "local_id": 8, "type_label": "link_or_file", "message_content": _file_payload("doc.txt", len(payload)), "create_time": 1788681354},
                {"message_uid": "voice-1", "chat_username": "chat-1", "local_id": 324, "type_label": "voice", "message_content": _voice_payload(6196), "create_time": 1788681351},
                {"message_uid": "link-1", "chat_username": "chat-1", "local_id": 9, "type_label": "link_or_file", "message_content": _link_payload("https://example.invalid/x"), "create_time": 1788681355},
                {"message_uid": "quote-1", "chat_username": "chat-1", "local_id": 10, "type_label": "link_or_file", "message_content": SEALED_274307_SHAPE, "create_time": 1788681356},
                {"message_uid": "merge-1", "chat_username": "chat-1", "local_id": 11, "type_label": "link_or_file", "message_content": _merged_records_payload(), "create_time": 1788681357},
            ]
        )
        result = media_sync.sync_media(self._args())
        self.assertEqual(result["scanned"], 8)

        by_uid = {row["message_uid"]: row for row in self._media_rows()}

        # image / sticker / video keep exactly their previous behaviour: they are
        # still dispatched to their original branches, they still produce the
        # same status vocabulary, and they never gain a `file`/`voice` type.
        self.assertIn("img-1", by_uid)
        self.assertEqual(by_uid["img-1"]["media_type"], "image")
        self.assertEqual(by_uid["img-1"]["status"], "missing_metadata")
        self.assertEqual(by_uid["sti-1"]["media_type"], "sticker")
        self.assertEqual(by_uid["sti-1"]["status"], "missing_file")
        self.assertEqual(by_uid["vid-1"]["media_type"], "video")
        self.assertEqual(by_uid["vid-1"]["status"], "missing_metadata")

        # The genuine file transfer and the voice message are now covered.
        self.assertEqual(by_uid["file-1"]["media_type"], "file")
        self.assertEqual(by_uid["file-1"]["status"], "ready")
        self.assertEqual(by_uid["voice-1"]["media_type"], "voice")
        self.assertEqual(by_uid["voice-1"]["status"], "ready")

        # Non-file appmsg subtypes must not receive a media reference at all.
        for uid in ("link-1", "quote-1", "merge-1"):
            self.assertNotIn(uid, by_uid, f"{uid} is not a media message and must have no media row")
        self.assertEqual(result["stats"]["not_media"], 3)

    def test_fmr_6_video_projection_still_reports_a_thumbnail(self) -> None:
        normalized = self._normalized(
            {"message_uid": "vid-1", "chat_username": "chat-1", "local_id": 3, "type_label": "video", "message_content": "", "create_time": 1},
            media={"media_id": "vid-1", "filename": "vid-1_thumb.jpg", "mime_type": "image/jpeg", "role": "thumbnail", "status": "ready"},
        )
        self.assertEqual(normalized["type"], "video")
        self.assertEqual(normalized["media_role"], "thumbnail")

    # ------------------------------------------------------------------
    # FMR-7 — F3 message projection regression
    # ------------------------------------------------------------------

    def test_fmr_7_file_message_keeps_top_level_media_contract_in_event_and_rest(self) -> None:
        store = self._store()
        normalized = self._normalized(
            self._file_row(title="report.txt", total=4),
            media={"media_id": "file-1", "filename": "report.txt", "mime_type": "text/plain", "role": "original", "status": "ready"},
        )
        store.upsert_message(normalized)
        created = self._events(store, "message.created")
        self.assertEqual(len(created), 1)
        self.assertEqual(created[0]["message"]["media_role"], "original")
        self.assertEqual(created[0]["message"]["media_status"], "ready")
        self.assertEqual(created[0]["message"]["media_id"], "file-1")
        listing = store.list_messages("account-1", "chat-1")
        self.assertEqual(listing["messages"][0]["media_role"], "original")
        self.assertEqual(listing["messages"][0]["media_status"], "ready")

    # ------------------------------------------------------------------
    # FMR-8 — thumbnail safety
    # ------------------------------------------------------------------

    def test_fmr_8_no_thumbnail_is_ever_reported_as_an_ready_original(self) -> None:
        payload = b"real-file"
        self._place_file("2026-09", "doc.txt", payload)
        # A decoy "thumbnail" of the same file, and a real video thumbnail.
        self._place_file("2026-09", "doc.txt_thumb.jpg", b"thumb")
        self._seed_staging(
            [
                {"message_uid": "file-1", "chat_username": "chat-1", "local_id": 8, "type_label": "link_or_file", "message_content": _file_payload("doc.txt", len(payload)), "create_time": 1788681354},
                {"message_uid": "file-2", "chat_username": "chat-1", "local_id": 9, "type_label": "link_or_file", "message_content": _file_payload("absent.txt", 10), "create_time": 1788681355},
            ]
        )
        media_sync.sync_media(self._args())

        rows = self._media_rows()
        self.assertTrue(rows)
        for row in rows:
            media_path = str(row["media_path"] or "")
            if not media_path:
                continue
            # message_media stores the artifact path relative to the runtime dir.
            artifact = Path(media_path)
            if not artifact.is_absolute():
                artifact = self.runtime_dir / artifact
            # Correlating pair: role vs the artifact's own filename suffix.
            is_thumb_artifact = "_thumb" in artifact.name or artifact.suffix == ".tmp"
            if is_thumb_artifact:
                self.assertFalse(
                    row["status"] == "ready" and row["media_type"] in {"file", "voice"},
                    f"thumbnail artifact {artifact} must never be published as a ready file/voice original",
                )
            if row["media_type"] in {"file", "voice"} and row["status"] == "ready":
                self.assertTrue(artifact.is_file())
                self.assertNotIn("_thumb", artifact.name)

        # No row anywhere claims ready while carrying no artifact.
        self.assertEqual([row["message_uid"] for row in rows if row["status"] == "ready" and not row["media_path"]], [])

    def test_fmr_8_role_derivation_never_marks_a_pending_file_as_a_thumbnail(self) -> None:
        # A not-ready file row must be a pending *original*, never a thumbnail:
        # there is no thumbnail form for a file, and a consumer that sees
        # role=thumbnail must treat the message as permanently undeliverable.
        normalized = self._normalized(
            self._file_row(title="absent.txt", total=10),
            media={"media_id": "file-1", "filename": "file-1", "mime_type": "application/octet-stream", "role": "original", "status": "missing_file"},
        )
        self.assertEqual(normalized["media_role"], "original")
        self.assertEqual(normalized["media_status"], "missing_file")
        self.assertEqual(normalized["vendor_specific"]["media"]["thumbnail_media_id"], "")
        self.assertEqual(normalized["vendor_specific"]["media"]["original_media_id"], "file-1")

    # ------------------------------------------------------------------
    # Sealed 274307 fixture
    # ------------------------------------------------------------------

    def test_sealed_274307_is_a_quote_reply_and_must_not_be_projected_as_a_file(self) -> None:
        row = {
            "message_uid": SEALED_274307["message_id"],
            "chat_username": SEALED_274307["chat_id"],
            "local_id": 1040,
            "type_label": "link_or_file",
            "message_content": SEALED_274307_SHAPE,
            "create_time": 1789605674,
        }
        parts = message_display_parts(SEALED_274307_SHAPE, "", "link_or_file", "")
        self.assertEqual(parts.get("app_type"), SEALED_274307_APPMSG_TYPE)
        self.assertFalse(parts.get("app_is_file_attachment"))
        self.assertFalse(SEALED_274307_HAS_ATTACHMENT_PAYLOAD)

        # No media reference may be created for it ...
        self.assertIsNone(media_sync.sync_file(row, self._args(), {}))

        # ... and it must not be projected as a media message.
        normalized = self._normalized(row)
        self.assertEqual(normalized["type"], "text")
        self.assertNotIn("media_id", normalized)
        self.assertNotIn("media_role", normalized)
        self.assertNotIn("media_status", normalized)
        self.assertEqual(SEALED_274307["type"], "file", "the sealed census recorded the pre-fix projection")

    def test_sealed_274307_media_backing_is_not_provable_offline(self) -> None:
        # The sealed payload declares no attachment payload and names no file, so
        # there is no frozen media artifact to resolve.  Recording this as a
        # fact (rather than as a missing capability) is the honest answer.
        info = media_sync.file_attachment_info(SEALED_274307_SHAPE)
        self.assertEqual(info, {})
        self.assertFalse(SEALED_274307_HAS_ATTACHMENT_PAYLOAD)

    def test_sealed_274307_fixture_matches_the_frozen_evidence(self) -> None:
        fixture = json.loads((Path(__file__).resolve().parent / "fixtures" / "rc14_sealed_274307.json").read_text(encoding="utf-8"))
        self.assertEqual(fixture["source_census"]["sha256"], "697c207f1ceb073c40fa63743efe53216f279a0974bd58af5078d3287373df3c")
        self.assertEqual(fixture["sealed_row"], SEALED_274307)
        self.assertEqual(fixture["structural_evidence"]["appmsg_type"], SEALED_274307_APPMSG_TYPE)
        self.assertFalse(fixture["structural_evidence"]["appattach_has_attachment_payload"])
        self.assertTrue(fixture["structural_evidence"]["has_refermsg"])
        self.assertEqual(fixture["census_invariants"]["file_typed_rows_in_window"], 1)
        self.assertEqual(fixture["verdicts"]["274307_MEDIA_ID_RESOLVABLE"], "NOT_PROVABLE_OFFLINE")

    def test_sealed_274307_is_a_single_first_appearance_in_the_census(self) -> None:
        # Guard the fixture against the sealed census itself when it is present
        # in this checkout: the invariant that makes 274307 new business rather
        # than a duplicate is that its message_id appears exactly once.
        fixture_path = self._census_path()
        if fixture_path is None:
            self.skipTest("sealed census fixture is not present in this checkout")
        payload = json.loads(fixture_path.read_text(encoding="utf-8"))
        columns = payload["columns"]
        rows = [dict(zip(columns, row)) for row in payload["rows"]]
        matching = [row for row in rows if row["message_id"] == SEALED_274307["message_id"]]
        self.assertEqual(len(matching), 1)
        self.assertEqual(matching[0]["cursor"], SEALED_274307["cursor"])
        self.assertEqual(matching[0]["event_type"], "message.created")
        self.assertEqual(matching[0]["media_id"], "")
        self.assertEqual(len([row for row in rows if row["type"] == "file" and row["event_type"].startswith("message.")]), 1)

    @staticmethod
    def _census_path() -> Path | None:
        relative = Path("work") / "rc14-efb-idempotency" / "tests" / "fixtures" / "rc14_post_f3_reprojection_stream.json"
        for base in (CORE_ROOT, *CORE_ROOT.parents):
            candidate = base / relative
            if candidate.is_file():
                return candidate
        return None


if __name__ == "__main__":
    unittest.main()
