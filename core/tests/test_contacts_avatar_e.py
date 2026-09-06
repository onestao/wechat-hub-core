"""Comprehensive regression tests for Agent E: Contacts / Groups / Avatar Pipeline.

Covers all 12 mandatory E9 requirements:
1. test_contact_names_priority (remark -> nickname -> alias -> member_id)
2. test_group_nickname_priority (group_nickname -> remark -> nickname -> alias -> member_id)
3. test_avatar_ref_persistence (write and reload keeps avatar_ref and avatar_cache)
4. test_self_profile_resolution (not logged in / no contact -> fallback to wxid, no guessing)
5. test_unknown_sender_fallback_to_member_id (prohibit showing "对方")
6. test_api_identity_isolation (API strictly filtered by wechat_identity_uuid)
7. test_two_identities_same_member_id_isolated (two WeChat identities same member_id do not cross)
8. test_avatar_bad_mime_rejected (415 rejects non-image data)
9. test_avatar_oversize_rejected (413 rejects > 2MB avatar)
10. test_avatar_fetch_timeout (504 timeout protection)
11. test_console_search_and_group_member_flow (Console endpoints & data flow test)
12. test_large_contact_list_pagination (cursor pagination test)
"""

from __future__ import annotations

import base64
import io
import os
import shutil
import sys
import tempfile
import time
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

CORE_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CORE_ROOT))

_TEST_GUI_LEASE_DIR = tempfile.TemporaryDirectory()
os.environ.setdefault("WECHAT_GUI_LEASE_DIR", _TEST_GUI_LEASE_DIR.name)

from core import identity as identity_v2
from core.avatar import (
    AvatarSecurityError,
    detect_image_mime,
    fallback_avatar_svg,
    fetch_remote_avatar,
    validate_avatar_url,
    ALLOWED_IMAGE_MIMES,
)
from core.store import (
    CoreStore,
    resolve_contact_display_name,
    resolve_group_member_display_name,
)
from memory.message_parse import parse_app_message


# 1x1 transparent PNG
SAMPLE_PNG = base64.b64decode("iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNk+A8AAQUBAScY42YAAAAASUVORK5CYII=")


class ContactsAndAvatarETest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path(tempfile.mkdtemp(prefix="core-test-e-"))
        self.db_path = self.temp_dir / "core.sqlite"
        self.store = CoreStore(self.db_path)

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def _seed_identity(self, account_id: str, wxid: str) -> str:
        self.store.upsert_account(account_id, f"Account {account_id}", state="online")
        self.store.observe_login(account_id, wxid, verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        state = self.store.binding_state(account_id)
        return str(state["identity"]["wechat_identity_uuid"])

    # -------------------------------------------------------------------------
    # 1. Contact names priority: remark -> nickname -> alias -> member_id
    # -------------------------------------------------------------------------
    def test_contact_names_priority(self) -> None:
        # Direct function priority check
        self.assertEqual(
            resolve_contact_display_name(remark="R", nickname="N", alias="A", member_id="M"),
            "R",
        )
        self.assertEqual(
            resolve_contact_display_name(remark="", nickname="N", alias="A", member_id="M"),
            "N",
        )
        self.assertEqual(
            resolve_contact_display_name(remark="", nickname="", alias="A", member_id="M"),
            "A",
        )
        self.assertEqual(
            resolve_contact_display_name(remark="", nickname="", alias="", member_id="M"),
            "M",
        )

        # Database upsert priority check
        ident_uuid = self._seed_identity("acc_p1", "wxid_self_p1")

        # Full fields -> display_name should be remark
        self.store.upsert_contact(
            "acc_p1",
            {"member_id": "c1", "remark": "Doctor Zhang", "nickname": "ZhangSan", "alias": "zhang_s"},
        )
        c = self.store.identity_contact(ident_uuid, "c1")
        self.assertIsNotNone(c)
        self.assertEqual(c["display_name"], "Doctor Zhang")

        # No remark -> display_name should be nickname
        self.store.upsert_contact(
            "acc_p1",
            {"member_id": "c2", "remark": "", "nickname": "LiSi", "alias": "li_s"},
        )
        c = self.store.identity_contact(ident_uuid, "c2")
        self.assertEqual(c["display_name"], "LiSi")

        # Only alias -> display_name should be alias
        self.store.upsert_contact(
            "acc_p1",
            {"member_id": "c3", "remark": "", "nickname": "", "alias": "wang_w"},
        )
        c = self.store.identity_contact(ident_uuid, "c3")
        self.assertEqual(c["display_name"], "wang_w")

        # Only member_id -> display_name should be member_id
        self.store.upsert_contact(
            "acc_p1",
            {"member_id": "c4", "remark": "", "nickname": "", "alias": ""},
        )
        c = self.store.identity_contact(ident_uuid, "c4")
        self.assertEqual(c["display_name"], "c4")

    # -------------------------------------------------------------------------
    # 2. Group nickname priority: group_nickname -> remark -> nickname -> alias -> member_id
    # -------------------------------------------------------------------------
    def test_group_nickname_priority(self) -> None:
        # Direct function priority check
        self.assertEqual(
            resolve_group_member_display_name(
                group_nickname="G", remark="R", nickname="N", alias="A", member_id="M"
            ),
            "G",
        )
        self.assertEqual(
            resolve_group_member_display_name(
                group_nickname="", remark="R", nickname="N", alias="A", member_id="M"
            ),
            "R",
        )
        self.assertEqual(
            resolve_group_member_display_name(
                group_nickname="", remark="", nickname="N", alias="A", member_id="M"
            ),
            "N",
        )
        self.assertEqual(
            resolve_group_member_display_name(
                group_nickname="", remark="", nickname="", alias="A", member_id="M"
            ),
            "A",
        )
        self.assertEqual(
            resolve_group_member_display_name(
                group_nickname="", remark="", nickname="", alias="", member_id="M"
            ),
            "M",
        )

        # Database upsert member check
        ident_uuid = self._seed_identity("acc_p2", "wxid_self_p2")
        chat_id = "group_lab@chatroom"
        self.store.upsert_chat({"account_id": "acc_p2", "chat_id": chat_id, "type": "group"})

        # Member with group_nickname
        self.store.upsert_member(
            "acc_p2",
            chat_id,
            {
                "member_id": "m1",
                "group_nickname": "Lab Admin",
                "remark": "Doctor Wu",
                "nickname": "WuWu",
                "alias": "wu_lab",
            },
        )
        m = self.store.identity_member(ident_uuid, chat_id, "m1")
        self.assertIsNotNone(m)
        self.assertEqual(m["display_name"], "Lab Admin")

        # Member without group_nickname, but has remark
        self.store.upsert_member(
            "acc_p2",
            chat_id,
            {
                "member_id": "m2",
                "group_nickname": "",
                "remark": "Doctor Wu",
                "nickname": "WuWu",
                "alias": "wu_lab",
            },
        )
        m = self.store.identity_member(ident_uuid, chat_id, "m2")
        self.assertEqual(m["display_name"], "Doctor Wu")

        # Member without group_nickname or remark, but has nickname
        self.store.upsert_member(
            "acc_p2",
            chat_id,
            {"member_id": "m3", "group_nickname": "", "remark": "", "nickname": "Student Chen"},
        )
        m = self.store.identity_member(ident_uuid, chat_id, "m3")
        self.assertEqual(m["display_name"], "Student Chen")

    # -------------------------------------------------------------------------
    # 3. Avatar ref persistence: write and reload keeps avatar_ref
    # -------------------------------------------------------------------------
    def test_avatar_ref_persistence(self) -> None:
        ident_uuid = self._seed_identity("acc_p3", "wxid_self_p3")
        avatar_url = "https://wx.qlogo.cn/mmopen/test_avatar_123"
        big_head = "https://wx.qlogo.cn/mmopen/big_123"
        small_head = "https://wx.qlogo.cn/mmopen/small_123"
        md5_val = "9f8e7d6c5b4a3f2e1d0c"

        self.store.upsert_contact(
            "acc_p3",
            {
                "member_id": "contact_avatar_1",
                "display_name": "Avatar Contact",
                "avatar_ref": avatar_url,
                "big_head_url": big_head,
                "small_head_url": small_head,
                "head_img_md5": md5_val,
            },
        )

        # Cache binary avatar
        cache_key = f"{ident_uuid}:contact_avatar_1"
        self.store.set_avatar_cache(cache_key, SAMPLE_PNG, "image/png")

        # Close store and reload from disk
        reloaded_store = CoreStore(self.db_path)
        c = reloaded_store.identity_contact(ident_uuid, "contact_avatar_1")
        self.assertIsNotNone(c)
        self.assertEqual(c["avatar_ref"], avatar_url)
        self.assertEqual(c["big_head_url"], big_head)
        self.assertEqual(c["small_head_url"], small_head)
        self.assertEqual(c["head_img_md5"], md5_val)

        # Assert avatar cache is also persistent
        cached = reloaded_store.get_avatar_cache(cache_key)
        self.assertIsNotNone(cached)
        body, mime = cached
        self.assertEqual(body, SAMPLE_PNG)
        self.assertEqual(mime, "image/png")

    # -------------------------------------------------------------------------
    # 4. Self profile resolution: fallback to wxid, no guessing
    # -------------------------------------------------------------------------
    def test_self_profile_resolution(self) -> None:
        self.store.upsert_account("acc_p4", "Account P4", state="online")
        self.store.observe_login("acc_p4", "wxid_self_nobound", verified_source=identity_v2.VERIFIED_SOURCE_AGENT_AUTH)
        b = self.store.binding_state("acc_p4")
        ident_uuid = str(b["identity"]["wechat_identity_uuid"])

        # When profile has no nickname, must fallback to wechat_user_id, not guess
        prof = self.store.identity_profile(ident_uuid)
        self.assertEqual(prof["wechat_user_id"], "wxid_self_nobound")
        self.assertEqual(prof["nickname"], "wxid_self_nobound")
        self.assertIn(ident_uuid, prof["avatar_url"])

        # Updating profile updates values properly
        updated = self.store.update_identity_profile(
            ident_uuid,
            nickname="Prof. Chemical",
            avatar_ref="https://wx.qlogo.cn/self_avatar",
            profile_json={"lab": "Organic Synthesis"},
        )
        self.assertEqual(updated["nickname"], "Prof. Chemical")
        self.assertEqual(updated["avatar_ref"], "https://wx.qlogo.cn/self_avatar")
        self.assertEqual(updated["profile_json"]["lab"], "Organic Synthesis")

    # -------------------------------------------------------------------------
    # 5. Unknown sender fallback to member_id: prohibit showing "对方"
    # -------------------------------------------------------------------------
    def test_unknown_sender_fallback_to_member_id(self) -> None:
        # Test quote message parsing without "对方" placeholder
        xml_with_sender = """<msg><appmsg type="57"><refermsg>
            <chatusr>wxid_unknown_999</chatusr>
            <content>Original message text</content>
        </refermsg></appmsg></msg>"""
        parsed = parse_app_message(xml_with_sender)
        self.assertNotIn("对方", parsed.get("semantic_text", ""))
        self.assertIn("wxid_unknown_999", parsed.get("semantic_text", ""))

        xml_without_sender = """<msg><appmsg type="57"><refermsg>
            <content>Quoted text without sender</content>
        </refermsg></appmsg></msg>"""
        parsed_anon = parse_app_message(xml_without_sender)
        self.assertNotIn("对方", parsed_anon.get("semantic_text", ""))
        self.assertIn("Quoted text without sender", parsed_anon.get("semantic_text", ""))

        # Verify contact display fallback returns member_id when nothing else is provided
        self.assertEqual(resolve_contact_display_name(member_id="wxid_sender_404"), "wxid_sender_404")
        self.assertNotEqual(resolve_contact_display_name(member_id="wxid_sender_404"), "对方")

    # -------------------------------------------------------------------------
    # 6. API identity isolation: strictly filtered by wechat_identity_uuid
    # -------------------------------------------------------------------------
    def test_api_identity_isolation(self) -> None:
        uuid1 = self._seed_identity("acc_iso1", "wxid_owner_1")
        uuid2 = self._seed_identity("acc_iso2", "wxid_owner_2")

        # Identity 1 contacts & groups
        self.store.upsert_contact("acc_iso1", {"member_id": "c_iso1", "display_name": "Contact 1"})
        self.store.upsert_chat({"account_id": "acc_iso1", "chat_id": "chat_iso1@chatroom", "type": "group"})
        self.store.upsert_member("acc_iso1", "chat_iso1@chatroom", {"member_id": "m_iso1", "display_name": "Member 1"})

        # Identity 2 contacts & groups
        self.store.upsert_contact("acc_iso2", {"member_id": "c_iso2", "display_name": "Contact 2"})
        self.store.upsert_chat({"account_id": "acc_iso2", "chat_id": "chat_iso2@chatroom", "type": "group"})
        self.store.upsert_member("acc_iso2", "chat_iso2@chatroom", {"member_id": "m_iso2", "display_name": "Member 2"})

        # Query Identity 1 contacts
        c_res1 = self.store.identity_list_contacts(uuid1)
        m_ids1 = {c["member_id"] for c in c_res1["contacts"]}
        self.assertIn("c_iso1", m_ids1)
        self.assertNotIn("c_iso2", m_ids1)

        # Query Identity 2 contacts
        c_res2 = self.store.identity_list_contacts(uuid2)
        m_ids2 = {c["member_id"] for c in c_res2["contacts"]}
        self.assertIn("c_iso2", m_ids2)
        self.assertNotIn("c_iso1", m_ids2)

        # Query chat members with wrong identity
        mem_wrong = self.store.identity_list_members(uuid2, "chat_iso1@chatroom")
        self.assertEqual(len(mem_wrong["members"]), 0)

        # Query chat members with correct identity
        mem_right = self.store.identity_list_members(uuid1, "chat_iso1@chatroom")
        self.assertEqual(len(mem_right["members"]), 1)
        self.assertEqual(mem_right["members"][0]["member_id"], "m_iso1")

    # -------------------------------------------------------------------------
    # 7. Two WeChat identities same member_id isolated
    # -------------------------------------------------------------------------
    def test_two_identities_same_member_id_isolated(self) -> None:
        uuid1 = self._seed_identity("acc_two1", "wxid_owner_a")
        uuid2 = self._seed_identity("acc_two2", "wxid_owner_b")
        shared_member_id = "wxid_shared_colleague"

        # Account 1 sees colleague as "Research Partner"
        self.store.upsert_contact(
            "acc_two1",
            {
                "member_id": shared_member_id,
                "remark": "Research Partner",
                "nickname": "Dr. Wang",
                "alias": "wang_chem",
            },
        )

        # Account 2 sees colleague as "Vendor Rep"
        self.store.upsert_contact(
            "acc_two2",
            {
                "member_id": shared_member_id,
                "remark": "Vendor Rep",
                "nickname": "Dr. Wang",
                "alias": "wang_reagents",
            },
        )

        # Querying Identity 1 returns "Research Partner"
        c1 = self.store.identity_contact(uuid1, shared_member_id)
        self.assertIsNotNone(c1)
        self.assertEqual(c1["display_name"], "Research Partner")
        self.assertEqual(c1["remark"], "Research Partner")
        self.assertEqual(c1["alias"], "wang_chem")

        # Querying Identity 2 returns "Vendor Rep"
        c2 = self.store.identity_contact(uuid2, shared_member_id)
        self.assertIsNotNone(c2)
        self.assertEqual(c2["display_name"], "Vendor Rep")
        self.assertEqual(c2["remark"], "Vendor Rep")
        self.assertEqual(c2["alias"], "wang_reagents")

        # Overwrite in Identity 1 does NOT touch Identity 2
        self.store.upsert_contact(
            "acc_two1",
            {
                "member_id": shared_member_id,
                "remark": "Keynote Speaker",
            },
        )
        c1_new = self.store.identity_contact(uuid1, shared_member_id)
        c2_stable = self.store.identity_contact(uuid2, shared_member_id)
        self.assertEqual(c1_new["display_name"], "Keynote Speaker")
        self.assertEqual(c2_stable["display_name"], "Vendor Rep")

    # -------------------------------------------------------------------------
    # 8. Avatar bad MIME rejected (415)
    # -------------------------------------------------------------------------
    def test_avatar_bad_mime_rejected(self) -> None:
        # Magic bytes detection
        self.assertEqual(detect_image_mime(SAMPLE_PNG), "image/png")
        self.assertIsNone(detect_image_mime(b"<html><body>Not an image</body></html>"))
        self.assertIsNone(detect_image_mime(b'{"error": "bad request"}'))
        self.assertIsNone(detect_image_mime(b""))

        # SSRF checks
        with self.assertRaises(AvatarSecurityError) as ctx:
            validate_avatar_url("http://127.0.0.1/evil.png")
        self.assertEqual(ctx.exception.status, 403)
        self.assertEqual(ctx.exception.code, "ssrf_blocked")

        with self.assertRaises(AvatarSecurityError) as ctx:
            validate_avatar_url("http://evil-attacker.com/avatar.jpg")
        self.assertEqual(ctx.exception.status, 403)
        self.assertEqual(ctx.exception.code, "ssrf_blocked")

        # Fetch returns 415 on bad mime
        mock_response = MagicMock()
        mock_response.read.side_effect = [b"<!DOCTYPE html><html><body>fake</body></html>", b""]
        mock_response.headers.get.return_value = "41"
        mock_response.__enter__.return_value = mock_response

        with patch("urllib.request.urlopen", return_value=mock_response):
            with self.assertRaises(AvatarSecurityError) as ctx:
                fetch_remote_avatar("https://wx.qlogo.cn/mmopen/fake_html_avatar")
            self.assertEqual(ctx.exception.status, 415)
            self.assertEqual(ctx.exception.code, "bad_mime")

    # -------------------------------------------------------------------------
    # 9. Avatar oversize rejected (413)
    # -------------------------------------------------------------------------
    def test_avatar_oversize_rejected(self) -> None:
        # Case A: Content-Length header claims > 2MB
        mock_hdr_response = MagicMock()
        mock_hdr_response.headers.get.return_value = str(3 * 1024 * 1024)
        mock_hdr_response.__enter__.return_value = mock_hdr_response

        with patch("urllib.request.urlopen", return_value=mock_hdr_response):
            with self.assertRaises(AvatarSecurityError) as ctx:
                fetch_remote_avatar("https://wx.qlogo.cn/mmopen/huge_header_avatar")
            self.assertEqual(ctx.exception.status, 413)
            self.assertEqual(ctx.exception.code, "oversize")

        # Case B: Downloaded chunks exceed 2MB
        mock_chunk_response = MagicMock()
        mock_chunk_response.headers.get.return_value = None
        # Stream 35 chunks of 64KB = 2.18 MB
        chunk = b"A" * (64 * 1024)
        mock_chunk_response.read.side_effect = [chunk] * 35 + [b""]
        mock_chunk_response.__enter__.return_value = mock_chunk_response

        with patch("urllib.request.urlopen", return_value=mock_chunk_response):
            with self.assertRaises(AvatarSecurityError) as ctx:
                fetch_remote_avatar("https://wx.qlogo.cn/mmopen/huge_chunk_avatar")
            self.assertEqual(ctx.exception.status, 413)
            self.assertEqual(ctx.exception.code, "oversize")

    # -------------------------------------------------------------------------
    # 10. Avatar fetch timeout (504)
    # -------------------------------------------------------------------------
    def test_avatar_fetch_timeout(self) -> None:
        with patch("urllib.request.urlopen", side_effect=TimeoutError("Request timed out")):
            with self.assertRaises(AvatarSecurityError) as ctx:
                fetch_remote_avatar("https://wx.qlogo.cn/mmopen/slow_avatar", timeout=0.1)
            self.assertEqual(ctx.exception.status, 504)
            self.assertEqual(ctx.exception.code, "fetch_timeout")

        # Fallback SVG works when remote fetch fails
        svg_bytes, svg_mime = fallback_avatar_svg("Alice")
        self.assertEqual(svg_mime, "image/svg+xml")
        self.assertIn(b"Alice"[:1], svg_bytes)

    # -------------------------------------------------------------------------
    # 11. Console search and group member flow
    # -------------------------------------------------------------------------
    def test_console_search_and_group_member_flow(self) -> None:
        ident_uuid = self._seed_identity("acc_console", "wxid_self_console")
        # Populate contacts
        self.store.upsert_contact(
            "acc_console",
            {"member_id": "albert", "display_name": "Albert Einstein", "remark": "Physicist"},
        )
        self.store.upsert_contact(
            "acc_console",
            {"member_id": "marie", "display_name": "Marie Curie", "remark": "Chemist"},
        )
        self.store.upsert_chat({"account_id": "acc_console", "chat_id": "science@chatroom", "type": "group"})
        self.store.upsert_member(
            "acc_console",
            "science@chatroom",
            {"member_id": "albert", "group_nickname": "Albert (Relativity)", "remark": "Physicist"},
        )

        # 1. Search contacts with query "Curie"
        res_search = self.store.identity_list_contacts(ident_uuid, query="Curie")
        self.assertEqual(len(res_search["contacts"]), 1)
        self.assertEqual(res_search["contacts"][0]["member_id"], "marie")

        # 2. Search group members with query "Relativity"
        res_member_search = self.store.identity_list_members(ident_uuid, "science@chatroom", query="Relativity")
        self.assertEqual(len(res_member_search["members"]), 1)
        self.assertEqual(res_member_search["members"][0]["member_id"], "albert")
        self.assertEqual(res_member_search["members"][0]["group_nickname"], "Albert (Relativity)")

        # 3. Profile check
        prof = self.store.identity_profile(ident_uuid)
        self.assertEqual(prof["wechat_identity_uuid"], ident_uuid)

    # -------------------------------------------------------------------------
    # 12. Large contact list pagination
    # -------------------------------------------------------------------------
    def test_large_contact_list_pagination(self) -> None:
        ident_uuid = self._seed_identity("acc_p12", "wxid_self_p12")
        total_contacts = 150

        # Seed 150 contacts
        for i in range(total_contacts):
            member_id = f"wxid_user_{i:04d}"
            name = f"User {i:04d}"
            self.store.upsert_contact(
                "acc_p12",
                {
                    "member_id": member_id,
                    "display_name": name,
                    "nickname": name,
                    "remark": f"Remark {i:04d}",
                },
            )

        page_size = 40
        collected_ids: list[str] = []
        cursor = ""
        page_count = 0

        while True:
            res = self.store.identity_list_contacts(ident_uuid, limit=page_size, cursor=cursor)
            contacts = res.get("contacts", [])
            collected_ids.extend([c["member_id"] for c in contacts])
            page_count += 1

            if not res.get("has_more"):
                break
            cursor = res.get("next_cursor")
            self.assertTrue(cursor, "next_cursor must be non-empty when has_more is True")

        # Verify all contacts collected without duplicates
        self.assertEqual(len(collected_ids), total_contacts)
        self.assertEqual(len(set(collected_ids)), total_contacts)
        self.assertEqual(page_count, 4)  # 40 + 40 + 40 + 30 = 150 in 4 pages


if __name__ == "__main__":
    unittest.main()
