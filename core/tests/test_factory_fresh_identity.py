"""Factory Fresh identity-presentation tests (P0-2 / P0-3).

P0-3: the Console must never present the internal wxid as if it were the WeChat
nickname.  Core resolves the presentation identity in this order:

    nickname -> configured alias -> wechat_id -> wxid (internal account id)

and reports *why* it chose what it chose, plus whether self-profile hydration is
still pending, so the UI can say "正在读取微信资料…" instead of guessing.
"""

import importlib.util
import sys
import unittest
from pathlib import Path


def _load_module(name: str, path: Path):
    existing = sys.modules.get(name)
    if existing is not None and getattr(existing, "__file__", None) == str(path):
        return existing
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


CORE_DIR = Path(__file__).resolve().parents[1]
identity = _load_module("identity", CORE_DIR / "identity.py")


WXID = "wxid_7ugft7xlkf5a22_4117"


class ResolveDisplayIdentityTests(unittest.TestCase):
    def test_nickname_wins(self):
        profile = identity.resolve_display_identity(
            {"wechat_user_id": WXID, "nickname": "阿茶", "profile_json": '{"wechat_id":"cha_2024"}'},
            alias="arasial",
        )
        self.assertEqual(profile["display_name"], "阿茶")
        self.assertEqual(profile["display_name_source"], "nickname")
        self.assertEqual(profile["wechat_id"], "cha_2024")
        self.assertEqual(profile["profile_hydration"], "complete")

    def test_wxid_is_never_a_nickname(self):
        # The wxid echoed back as a nickname is not a nickname.
        profile = identity.resolve_display_identity(
            {"wechat_user_id": WXID, "nickname": WXID, "profile_json": "{}"},
            alias="arasial",
        )
        self.assertEqual(profile["nickname"], "")
        self.assertNotEqual(profile["display_name"], WXID)
        self.assertEqual(profile["display_name"], "arasial")
        self.assertEqual(profile["display_name_source"], "alias")

    def test_alias_then_wechat_id_then_internal_id(self):
        alias_profile = identity.resolve_display_identity(
            {"wechat_user_id": WXID, "nickname": "", "profile_json": "{}"}, alias="arasial"
        )
        self.assertEqual(alias_profile["display_name_source"], "alias")

        wechat_id_profile = identity.resolve_display_identity(
            {"wechat_user_id": WXID, "nickname": "", "profile_json": '{"wechat_id":"cha_2024"}'},
            alias="",
        )
        self.assertEqual(wechat_id_profile["display_name"], "cha_2024")
        self.assertEqual(wechat_id_profile["display_name_source"], "wechat_id")

        internal = identity.resolve_display_identity(
            {"wechat_user_id": WXID, "nickname": "", "profile_json": "{}"}, alias=""
        )
        self.assertEqual(internal["display_name"], WXID)
        self.assertEqual(internal["display_name_source"], "internal_account_id")

    def test_hydration_pending_is_bounded(self):
        pending = identity.resolve_display_identity(
            {
                "wechat_user_id": WXID,
                "nickname": "",
                "profile_json": f'{{"hydration_started_at":"{identity.utc_now()}"}}',
            },
            alias="arasial",
        )
        self.assertEqual(pending["profile_hydration"], "pending")

        stale = identity.resolve_display_identity(
            {
                "wechat_user_id": WXID,
                "nickname": "",
                "profile_json": '{"hydration_started_at":"2000-01-01T00:00:00Z"}',
            },
            alias="arasial",
        )
        self.assertEqual(stale["profile_hydration"], "unavailable")

    def test_avatar_url_only_when_safe(self):
        unsafe = identity.resolve_display_identity(
            {"wechat_user_id": WXID, "nickname": "x", "avatar_ref": "javascript:alert(1)", "profile_json": "{}"}
        )
        self.assertEqual(unsafe["avatar_url"], "")
        safe = identity.resolve_display_identity(
            {"wechat_user_id": WXID, "nickname": "x", "avatar_ref": "https://wx.qlogo.cn/a", "profile_json": "{}"}
        )
        self.assertEqual(safe["avatar_url"], "https://wx.qlogo.cn/a")


if __name__ == "__main__":
    unittest.main()
