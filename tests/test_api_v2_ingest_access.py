"""Who may open the ingest socket, and -- more importantly -- when it exists.

The receiver is a write path for a collector that is not deployed yet, so the
invariant that matters most is that production cannot reach it at all until
someone deliberately turns it on.
"""

from __future__ import annotations

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer

from juice.api.access import Access, access_of
from juice.api.v2 import ROUTES, SERVICE_ROUTES
from juice.server import RecorderState, create_app
from juice.store import Store

TOKEN = "s3cret-ingest-token"  # noqa: S105


@pytest.fixture
def store():
    with Store(":memory:") as s:
        yield s


def _app(store: Store, token: str | None = TOKEN) -> web.Application:
    return create_app(RecorderState(), store, dev_auth=True, ingest_token=token)


class TestTheRouteOnlyExistsWhenConfigured:
    """`JUICE_INGEST_TOKEN` unset means the endpoint is not registered at all,
    exactly as `/api/backup` behaves. This is what keeps the receiver inert in
    production until cutover, so it is the first thing to pin."""

    def test_no_token_means_no_route(self, store: Store) -> None:
        app = _app(store, token=None)
        assert "/api/v2/ingest" not in {r.resource.canonical for r in app.router.routes()}

    def test_a_token_registers_the_route(self, store: Store) -> None:
        app = _app(store)
        assert "/api/v2/ingest" in {r.resource.canonical for r in app.router.routes()}


class TestServiceAccessIsDeclared:
    def test_the_ingest_route_declares_service(self) -> None:
        assert [access_of(r.handler) for r in SERVICE_ROUTES] == [Access.SERVICE]

    def test_no_browser_facing_route_declares_service(self) -> None:
        """SERVICE means "a machine with a shared secret". A browser route that
        claimed it would be gated by a token no browser has, which would look
        like a broken page rather than like a security decision."""
        assert Access.SERVICE not in {access_of(r.handler) for r in ROUTES}


class TestTheTokenIsEnforced:
    async def _connect(self, store: Store, headers: dict | None = None, token=TOKEN):
        client = TestClient(TestServer(_app(store, token=token)))
        await client.start_server()
        return client

    async def test_no_token_is_refused(self, store: Store) -> None:
        client = await self._connect(store)
        try:
            with pytest.raises(aiohttp.WSServerHandshakeError) as exc:
                await client.ws_connect("/api/v2/ingest")
            assert exc.value.status == 401
        finally:
            await client.close()

    async def test_a_wrong_token_is_refused(self, store: Store) -> None:
        client = await self._connect(store)
        try:
            with pytest.raises(aiohttp.WSServerHandshakeError) as exc:
                await client.ws_connect("/api/v2/ingest", headers={"Authorization": "Bearer wrong"})
            assert exc.value.status == 401
        finally:
            await client.close()

    async def test_a_non_ascii_token_is_refused_rather_than_crashing(self, store: Store) -> None:
        """`hmac.compare_digest` raises TypeError on non-ASCII `str`, so the
        obvious implementation answers a garbage credential with a 500 -- which
        both leaks that the endpoint exists and pages somebody at 3am."""
        client = await self._connect(store)
        try:
            with pytest.raises(aiohttp.WSServerHandshakeError) as exc:
                await client.ws_connect(
                    "/api/v2/ingest", headers={"Authorization": "Bearer paßwort"}
                )
            assert exc.value.status == 401
        finally:
            await client.close()

    async def test_the_right_token_gets_in(self, store: Store) -> None:
        client = await self._connect(store)
        try:
            ws = await client.ws_connect(
                "/api/v2/ingest", headers={"Authorization": f"Bearer {TOKEN}"}
            )
            await ws.close()
        finally:
            await client.close()

    async def test_a_logged_in_operator_cannot_open_it(self, store: Store) -> None:
        """The branch-ordering test. If the SERVICE check ran after the session
        check, any logged-in browser would be admitted to the ingest socket --
        a write path with no capability gate on it."""
        client = await self._connect(store)
        try:
            await client.post("/login")  # dev-auth shim: mints an operator session
            with pytest.raises(aiohttp.WSServerHandshakeError) as exc:
                await client.ws_connect("/api/v2/ingest")
            assert exc.value.status == 401
        finally:
            await client.close()
