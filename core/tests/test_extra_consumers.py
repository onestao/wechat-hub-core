"""WECHAT_CORE_EXTRA_CONSUMERS registers additional consumers (e.g. a 2nd EFB)."""

from __future__ import annotations

import os
import sys
import unittest
from pathlib import Path
from unittest import mock

CORE_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(CORE_ROOT))

from core import store  # noqa: E402


class ExtraConsumersTest(unittest.TestCase):
    def test_unset_keeps_builtin_registry(self) -> None:
        with mock.patch.dict(os.environ, {}, clear=False):
            os.environ.pop(store.EXTRA_CONSUMERS_ENV, None)
            self.assertFalse(store.is_registered_consumer("efb-linux-wechat-2:wechat.linux"))
            self.assertTrue(store.is_registered_consumer("efb-linux-wechat:wechat.linux"))

    def test_extra_consumers_are_registered(self) -> None:
        env = {store.EXTRA_CONSUMERS_ENV: "efb-linux-wechat-2:wechat.linux, quote-parser\nother#1"}
        with mock.patch.dict(os.environ, env):
            self.assertTrue(store.is_registered_consumer("efb-linux-wechat-2:wechat.linux"))
            self.assertTrue(store.is_registered_consumer("quote-parser"))
            self.assertTrue(store.is_registered_consumer("other#1"))
            self.assertTrue(store.is_registered_consumer("wechat-console"))
            self.assertFalse(store.is_registered_consumer("not-listed"))

    def test_invalid_ids_are_ignored(self) -> None:
        with mock.patch.dict(os.environ, {store.EXTRA_CONSUMERS_ENV: " , ok-1, -x, id!, a b"}):
            self.assertEqual({"ok-1", "a", "b"}, store.get_extra_consumers())

if __name__ == "__main__":
    unittest.main()
