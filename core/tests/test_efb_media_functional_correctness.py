"""RC.14 EFB media-role correctness tests."""

from __future__ import annotations

import hashlib
import shutil
import sys
import unittest
import uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

CORE_ROOT = Path(__file__).resolve().parents[2]
if str(CORE_ROOT) not in sys.path:
    sys.path.insert(0, str(CORE_ROOT))

from memory import media_sync  # noqa: E402
from core.normalize import _normalized_message  # noqa: E402
from core.store import CoreStore  # noqa: E402


class EFBMediaFunctionalCorrectnessTest(unittest.TestCase):
    def setUp(self) -> None:
        self.root = CORE_ROOT / ".tmp" / f"efb-media-correctness-{uuid.uuid4().hex}"
        self.root.mkdir(parents=True, exist_ok=True)

    def tearDown(self) -> None:
        shutil.rmtree(self.root, ignore_errors=True)

    def _image_paths(self, suffixes: list[str]) -> tuple[list[Path], str]:
        media_md5 = "a" * 32
        chat_hash = hashlib.md5(b"chat-1").hexdigest()
        image_dir = self.root / "wechat" / "msg" / "attach" / chat_hash / "01" / "Img"
        image_dir.mkdir(parents=True, exist_ok=True)
        paths = []
        for suffix in suffixes:
            path = image_dir / f"{media_md5}{suffix}.dat"
            path.write_bytes(suffix.encode("ascii") or b"original")
            paths.append(path)
        return paths, media_md5

    def test_f1_1_original_wins_when_thumbnail_also_exists(self) -> None:
        paths, _ = self._image_paths(["_t", "_h"])
        selected = media_sync.choose_dat(sorted(paths), prefer_thumb=False)
        self.assertIsNotNone(selected)
        self.assertTrue(str(selected).endswith("_h.dat"))

    def test_f1_2_thumbnail_only_is_not_a_final_original(self) -> None:
        paths, _ = self._image_paths(["_t"])
        self.assertIsNone(media_sync.choose_dat(paths, prefer_thumb=False))

    def test_f1_3_original_becomes_selectable_after_it_arrives(self) -> None:
        paths, media_md5 = self._image_paths(["_t"])
        self.assertIsNone(media_sync.choose_dat(paths, prefer_thumb=False))
        original = paths[0].with_name(f"{media_md5}.dat")
        original.write_bytes(b"original")
        self.assertEqual(
            media_sync.choose_dat(sorted([*paths, original]), prefer_thumb=False),
            original,
        )

    def test_f1_4_original_decode_failure_never_falls_back_to_thumbnail(self) -> None:
        paths, media_md5 = self._image_paths(["_t", "_h"])
        row = {
            "message_uid": "message-1",
            "chat_username": "chat-1",
            "local_id": 7,
        }
        args = SimpleNamespace(
            wechat_base_dir=self.root / "wechat",
            media_dir=self.root / "media",
            prefer_thumbnails=False,
        )
        with patch.object(media_sync, "decrypt_dat", return_value=(None, None)):
            result = media_sync.sync_image(
                row,
                args,
                {("chat-1", 7): media_md5},
                {},
            )
        self.assertEqual(result["status"], "decode_failed")
        self.assertTrue(str(result["source_path"]).endswith("_h.dat"))
        self.assertNotEqual(result["source_path"], str(paths[0]))

    def test_f2_pending_original_reference_is_preserved_in_message_contract(self) -> None:
        normalized = _normalized_message(
            "account-1",
            {
                "message_uid": "message-1",
                "chat_username": "chat-1",
                "type_label": "image",
                "message_content": "",
                "compress_content": "",
                "source": "",
                "origin_source": 0,
                "create_time": 1,
            },
            {},
            media={
                "media_id": "message-1",
                "filename": "message-1",
                "mime_type": "application/octet-stream",
                "role": "original",
                "status": "original_pending",
            },
        )
        self.assertEqual(normalized["media_id"], "message-1")
        self.assertEqual(normalized["media_role"], "original")
        self.assertEqual(normalized["media_status"], "original_pending")
        self.assertEqual(
            normalized["vendor_specific"]["media"]["original_media_id"],
            "message-1",
        )

    # ------------------------------------------------------------------
    # F3 — the media role/status contract must survive normalize -> store ->
    # event payload / REST projection.  F1 produced the fields, but the
    # explicit column whitelist in CoreStore.upsert_message() rebuilt the
    # persisted value from scratch and dropped them, so every consumer that
    # reads message["media_role"] at the top level (the frozen EFB candidate
    # among them) saw the pre-contract shape again.
    # ------------------------------------------------------------------

    def _store(self) -> CoreStore:
        store = CoreStore(self.root / "core.sqlite")
        # messages has a composite FK onto chats(account_id, chat_id), so the
        # account and the chat must exist before any message can be stored.
        store.upsert_account("account-1", "Account One", state="ready")
        store.upsert_chat({"account_id": "account-1", "chat_id": "chat-1", "type": "private"})
        return store

    @staticmethod
    def _media_message(message_id: str, *, role: str = "original", status: str = "ready") -> dict:
        return {
            "account_id": "account-1",
            "message_id": message_id,
            "chat_id": "chat-1",
            "type": "image",
            "direction": "incoming",
            "created_at": "2026-09-17T00:00:00Z",
            "author": {"member_id": "member-1", "display_name": "Member", "is_self": False},
            "media_id": message_id,
            "filename": f"{message_id}.jpg",
            "mime_type": "image/jpeg",
            "media_role": role,
            "media_status": status,
            "vendor_specific": {
                "source_local_id": 1,
                "source_message_table": "message_1",
                "media": {
                    "role": role,
                    "status": status,
                    "original_media_id": message_id if role == "original" else "",
                    "thumbnail_media_id": message_id if role == "thumbnail" else "",
                },
            },
        }

    @staticmethod
    def _events(store: CoreStore, event_type: str) -> list[dict]:
        page = store.poll_events(after="0", limit=200)
        return [event["payload"]["message"] for event in page["events"] if event["event_type"] == event_type]

    def test_f3_1_message_created_event_exposes_media_contract_fields(self) -> None:
        store = self._store()
        store.upsert_message(self._media_message("message-1"))
        created = self._events(store, "message.created")
        self.assertEqual(len(created), 1)
        self.assertEqual(created[0]["media_role"], "original")
        self.assertEqual(created[0]["media_status"], "ready")

    def test_f3_2_message_updated_event_exposes_media_contract_fields(self) -> None:
        store = self._store()
        store.upsert_message(self._media_message("message-1", status="original_pending"))
        store.upsert_message(self._media_message("message-1", status="ready"))
        updated = self._events(store, "message.updated")
        self.assertTrue(updated, "the pending -> ready transition must re-emit message.updated")
        self.assertEqual(updated[-1]["media_role"], "original")
        self.assertEqual(updated[-1]["media_status"], "ready")

    def test_f3_3_rest_message_projection_exposes_media_contract_fields(self) -> None:
        store = self._store()
        store.upsert_message(self._media_message("message-1"))
        listing = store.list_messages("account-1", "chat-1")
        self.assertEqual(len(listing["messages"]), 1)
        self.assertEqual(listing["messages"][0]["media_role"], "original")
        self.assertEqual(listing["messages"][0]["media_status"], "ready")

    def test_f3_4_thumbnail_role_is_exposed_and_never_reported_as_original(self) -> None:
        store = self._store()
        store.upsert_message(self._media_message("message-1", role="thumbnail"))
        created = self._events(store, "message.created")
        self.assertEqual(created[0]["media_role"], "thumbnail")
        listing = store.list_messages("account-1", "chat-1")
        self.assertEqual(listing["messages"][0]["media_role"], "thumbnail")

    def test_f3_5_projection_falls_back_to_vendor_specific_media(self) -> None:
        store = self._store()
        message = self._media_message("message-1")
        del message["media_role"]
        del message["media_status"]
        store.upsert_message(message)
        created = self._events(store, "message.created")
        self.assertEqual(created[0]["media_role"], "original")
        self.assertEqual(created[0]["media_status"], "ready")

    def test_f3_6_text_only_messages_are_not_perturbed(self) -> None:
        store = self._store()
        message = {
            "account_id": "account-1",
            "message_id": "text-1",
            "chat_id": "chat-1",
            "type": "text",
            "direction": "incoming",
            "created_at": "2026-09-17T00:00:00Z",
            "author": {"member_id": "member-1", "display_name": "Member", "is_self": False},
            "text": "hello",
        }
        self.assertEqual(store.upsert_message(message), "created")
        # The contract fields must not be materialized for media-less messages,
        # otherwise every historical text message would change its digest and
        # re-emit as message.updated.
        self.assertEqual(store.upsert_message(message), "unchanged")
        created = self._events(store, "message.created")
        self.assertEqual(len(created), 1)
        self.assertNotIn("media_role", created[0])
        self.assertNotIn("media_status", created[0])
        self.assertEqual(self._events(store, "message.updated"), [])

    def test_f3_7_normalizer_to_store_contract_is_preserved_end_to_end(self) -> None:
        store = self._store()
        normalized = _normalized_message(
            "account-1",
            {
                "message_uid": "message-1",
                "chat_username": "chat-1",
                "type_label": "image",
                "message_content": "",
                "compress_content": "",
                "source": "",
                "origin_source": 0,
                "create_time": 1,
            },
            {},
            media={
                "media_id": "message-1",
                "filename": "message-1.jpg",
                "mime_type": "image/jpeg",
                "role": "original",
                "status": "ready",
            },
        )
        store.upsert_message(normalized)
        created = self._events(store, "message.created")
        self.assertEqual(len(created), 1)
        self.assertEqual(created[0]["media_role"], "original")
        self.assertEqual(created[0]["media_status"], "ready")
        listing = store.list_messages("account-1", "chat-1")
        self.assertEqual(listing["messages"][0]["media_role"], "original")
        self.assertEqual(listing["messages"][0]["media_status"], "ready")


if __name__ == "__main__":
    unittest.main()
