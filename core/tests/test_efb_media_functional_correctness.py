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


if __name__ == "__main__":
    unittest.main()
