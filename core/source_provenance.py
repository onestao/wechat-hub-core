"""Small source-provenance helpers for the RC.5 release lineage.

RB-003 confirmed that ``discover_source_db`` selected an upstream WeChat
``db_storage`` directory purely by directory mtime, so a stale historical
account directory could win over the freshly authenticated account and its
history would be decrypted and ingested under the wrong identity.

These fail-closed helpers bind source selection to the runtime-verified
``logged_in_user``.  The release lineage does not carry the Identity-v2
helper set, so the small error type and the two code-verifiable metadata
helpers are implemented locally with semantics identical to the audited fix
in docs/RC5_RB003_CORE_SOURCE_PROVENANCE_FIX.md.  This module is
intentionally tiny and dependency-free; it must not grow into the
Identity-v2 feature stream.
"""

from __future__ import annotations

import re
from typing import Any

WXID_RE = re.compile(r"[A-Za-z][A-Za-z0-9_-]{4,79}\Z")

DATA_DIR_PLACEHOLDERS = {
    "__runtime_unresolved__",
    "unconfigured-source-db",
    "db_storage",
    "xwechat_files",
}


class SourceIdentityError(RuntimeError):
    """Fail-closed source-provenance gate error carrying an HTTP status and code."""

    def __init__(self, code: str, status: int, message: str, *, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.status = status
        self.message = message
        self.details = details or {}


def valid_wxid(value: object) -> bool:
    text = str(value or "").strip()
    return bool(text) and bool(WXID_RE.fullmatch(text))


def wechat_data_dir_name(path_value: object) -> str:
    """Return the WeChat account directory name encoded in an xwechat_files path.

    WeChat itself names ``xwechat_files/<account>/`` after the logged-in
    account id, so this directory name is code-verifiable credential metadata.
    Returns "" for placeholder/unresolved paths or anything that does not sit
    beneath an ``xwechat_files`` directory.
    """
    text = str(path_value or "").strip().replace("\\", "/")
    if not text:
        return ""
    parts = [part for part in text.split("/") if part]
    for index, part in enumerate(parts):
        if part == "xwechat_files":
            if index + 1 >= len(parts):
                return ""
            candidate = parts[index + 1]
            if not candidate or candidate in DATA_DIR_PLACEHOLDERS:
                return ""
            return candidate
    return ""
