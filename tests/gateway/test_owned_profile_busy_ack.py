"""Busy-session acks must use the owning multiplex profile's adapter/bot."""

from __future__ import annotations

import dataclasses
from types import SimpleNamespace
from unittest.mock import MagicMock

from gateway.config import Platform
from gateway.platforms.base import BasePlatformAdapter
from gateway.session import SessionSource, build_session_key


class _TinyAdapter(BasePlatformAdapter):
    """Minimal concrete adapter for ownership stamping tests."""

    async def connect(self):  # pragma: no cover
        return True

    async def disconnect(self):  # pragma: no cover
        return None

    async def send(self, *a, **k):  # pragma: no cover
        return MagicMock(success=True)

    async def get_chat_info(self, chat_id: str):  # pragma: no cover
        return {}


def _source(*, profile=None):
    return SessionSource(
        platform=Platform("zulip") if hasattr(Platform, "_missing_") else Platform.TELEGRAM,
        chat_id="dm:user@example.com",
        chat_type="dm",
        user_id="user@example.com",
        profile=profile,
    )


class TestOwnedProfileStamp:
    def test_stamp_owned_profile_sets_source_profile(self):
        cfg = SimpleNamespace(extra={})
        # Use telegram enum member which always exists
        adapter = object.__new__(_TinyAdapter)
        adapter._owned_profile = "amc12"
        event = SimpleNamespace(source=_source(profile=None))
        adapter._stamp_owned_profile(event)
        assert event.source.profile == "amc12"

    def test_session_key_includes_owned_profile(self):
        source = _source(profile="amc12")
        key = build_session_key(source, profile="amc12")
        assert key.startswith("agent:amc12:")

    def test_default_profile_keeps_main_namespace(self):
        source = _source(profile=None)
        key = build_session_key(source, profile=None)
        assert key.startswith("agent:main:")

    def test_set_owned_profile_normalizes_default(self):
        adapter = object.__new__(_TinyAdapter)
        adapter.set_owned_profile("default")
        assert adapter._owned_profile is None
        adapter.set_owned_profile("amc12")
        assert adapter._owned_profile == "amc12"
