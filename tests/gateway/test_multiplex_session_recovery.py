"""Multiplex session recovery must not cross profile namespaces.

Under gateway.multiplex_profiles, many profiles share one SessionStore /
state.db. Peer-tuple recovery (platform + user + chat_id) would otherwise
attach a secondary profile's inbound to the default profile's open DM
transcript — the bot answers from the wrong conversation and system
traffic can surface on the wrong bot.
"""

from __future__ import annotations

import threading
from pathlib import Path
from types import SimpleNamespace


from gateway.session import SessionStore


def _store(*, multiplex: bool) -> SessionStore:
    store = SessionStore.__new__(SessionStore)
    store.sessions_dir = Path("/tmp/unused-sessions")
    store._entries = {}
    store._loaded = True
    store._lock = threading.RLock()
    store.config = SimpleNamespace(multiplex_profiles=multiplex)
    store._has_active_processes_fn = None
    store._db = None
    return store


class TestMultiplexSessionRecoveryGuard:
    def test_multiplex_rejects_cross_profile_recovery(self):
        store = _store(multiplex=True)
        allowed = store._recovered_row_allowed_for_active_profile(
            requested_session_key="agent:amc12:zulip:dm:user@example.com",
            recovered={"session_key": "agent:main:zulip:dm:user@example.com"},
        )
        assert allowed is False

    def test_multiplex_allows_same_profile_recovery(self):
        store = _store(multiplex=True)
        allowed = store._recovered_row_allowed_for_active_profile(
            requested_session_key="agent:amc12:zulip:dm:user@example.com",
            recovered={"session_key": "agent:amc12:zulip:dm:user@example.com"},
        )
        assert allowed is True

    def test_multiplex_allows_exact_key_match(self):
        store = _store(multiplex=True)
        key = "agent:fintechnews:zulip:dm:user@example.com"
        allowed = store._recovered_row_allowed_for_active_profile(
            requested_session_key=key,
            recovered={"session_key": key},
        )
        assert allowed is True

    def test_non_multiplex_still_rejects_other_profile_rows(self, monkeypatch):
        store = _store(multiplex=False)
        monkeypatch.setattr(
            SessionStore,
            "_active_profile_name",
            staticmethod(lambda: "default"),
        )
        allowed = store._recovered_row_allowed_for_active_profile(
            requested_session_key="agent:main:zulip:dm:user@example.com",
            recovered={"session_key": "agent:amc12:zulip:dm:user@example.com"},
        )
        assert allowed is False
