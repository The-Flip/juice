"""Tests for juice.auth — OAuth SSO via FlipFix."""

from __future__ import annotations

import pytest
from aiohttp import web
from aiohttp.test_utils import TestClient, TestServer
from aioresponses import aioresponses

from juice.auth import (
    setup_auth,
)

PROVIDER_URL = "https://flipfix.example.com"
CLIENT_ID = "test-client-id"
CLIENT_SECRET = "test-client-secret-that-is-long-enough"
REDIRECT_URI = "http://localhost:8000/callback"

OAUTH_CONFIG = {
    "client_id": CLIENT_ID,
    "client_secret": CLIENT_SECRET,
    "provider_url": PROVIDER_URL,
    "redirect_uri": REDIRECT_URI,
}

FAKE_USER = {
    "sub": "42",
    "name": "Alice Smith",
    "email": "alice@theflip.museum",
    "preferred_username": "alice",
}


def make_app(oauth_config=OAUTH_CONFIG):
    """Create a minimal app with auth middleware for testing.

    Routes here are deliberately split into public-readable (matching
    PUBLIC_READABLE_PATTERNS in juice.auth) and private to cover both
    branches of the middleware.
    """
    app = web.Application()
    setup_auth(app, oauth_config)

    async def public_page(request):
        from juice.auth import is_authenticated

        return web.json_response({"authed": is_authenticated(request)})

    async def admin_page(request):
        return web.Response(text="OK")

    async def public_api(request):
        from juice.auth import is_authenticated

        return web.json_response({"authed": is_authenticated(request)})

    async def admin_api(request):
        return web.json_response({"data": "secret"})

    async def mock_power(request):
        from juice.auth import require_capability

        error = require_capability(request, "control_power")
        if error:
            return error
        return web.json_response({"ok": True})

    # `/` and `/api/machines` match PUBLIC_READABLE_PATTERNS in juice.auth.
    app.router.add_get("/", public_page)
    app.router.add_get("/api/machines", public_api)
    # `/admin` and `/api/admin` do not — they require authentication.
    app.router.add_get("/admin", admin_page)
    app.router.add_get("/api/admin", admin_api)
    app.router.add_post("/api/machines/1/power", mock_power)
    return app


@pytest.fixture
def mock_api():
    with aioresponses(passthrough=["http://127.0.0.1", "http://localhost"]) as m:
        yield m


def _stub_token(mock_api, access_token="fake-access-token"):
    mock_api.post(
        f"{PROVIDER_URL}/oauth/token/",
        payload={
            "access_token": access_token,
            "token_type": "Bearer",
            "expires_in": 3600,
            "refresh_token": "fake-refresh-token",
            "scope": "openid profile email capabilities",
        },
    )


def _stub_userinfo(mock_api, capabilities=None):
    payload = {
        **FAKE_USER,
        "email_verified": True,
        "https://flipfix.theflip.museum/capabilities": capabilities or [],
    }
    mock_api.get(f"{PROVIDER_URL}/oauth/userinfo/", payload=payload)


async def _login_session(client: TestClient, mock_api) -> None:
    """Drive the login flow to establish an authenticated session."""
    # Start login to get state + code_verifier into session
    resp = await client.get("/login", allow_redirects=False)
    assert resp.status == 302

    # Stub token + userinfo endpoints
    _stub_token(mock_api)
    _stub_userinfo(mock_api, capabilities=["control_power"])

    # Extract state from the redirect URL
    location = resp.headers["Location"]
    from urllib.parse import parse_qs, urlparse

    params = parse_qs(urlparse(location).query)
    state = params["state"][0]

    # Hit callback with the code + state
    resp = await client.get(f"/callback?code=fake-code&state={state}", allow_redirects=False)
    assert resp.status == 302
    assert resp.headers["Location"] == "/"


class TestAuthMiddleware:
    @pytest.mark.asyncio
    async def test_unauthenticated_private_page_redirects_to_login(self) -> None:
        app = make_app()
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/admin", allow_redirects=False)
            assert resp.status == 302
            assert resp.headers["Location"] == "/login"

    @pytest.mark.asyncio
    async def test_unauthenticated_private_api_returns_401(self) -> None:
        app = make_app()
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/api/admin")
            assert resp.status == 401
            data = await resp.json()
            assert data["error"] == "Not authenticated"

    @pytest.mark.asyncio
    async def test_unauthenticated_public_page_passes_through(self) -> None:
        app = make_app()
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/")
            assert resp.status == 200
            assert await resp.json() == {"authed": False}

    @pytest.mark.asyncio
    async def test_unauthenticated_public_api_passes_through(self) -> None:
        app = make_app()
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/api/machines")
            assert resp.status == 200
            assert await resp.json() == {"authed": False}

    @pytest.mark.asyncio
    async def test_unauthenticated_post_returns_401(self) -> None:
        # POST is never in the public-readable list — even on a "public" path
        # like /api/machines/1/power.
        app = make_app()
        async with TestClient(TestServer(app)) as client:
            resp = await client.post("/api/machines/1/power")
            assert resp.status == 401

    @pytest.mark.asyncio
    async def test_login_path_accessible(self) -> None:
        app = make_app()
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/login", allow_redirects=False)
            assert resp.status == 302
            # Should redirect to FlipFix, not to /login.
            assert PROVIDER_URL in resp.headers["Location"]

    @pytest.mark.asyncio
    async def test_authenticated_request_sees_authed_true(self, mock_api) -> None:
        app = make_app()
        async with TestClient(TestServer(app)) as client:
            await _login_session(client, mock_api)
            resp = await client.get("/")
            assert resp.status == 200
            assert await resp.json() == {"authed": True}
            resp = await client.get("/admin")
            assert resp.status == 200
            assert await resp.text() == "OK"


class TestLogin:
    @pytest.mark.asyncio
    async def test_redirects_to_provider(self) -> None:
        app = make_app()
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/login", allow_redirects=False)
            assert resp.status == 302
            location = resp.headers["Location"]
            assert location.startswith(f"{PROVIDER_URL}/oauth/authorize/")
            assert f"client_id={CLIENT_ID}" in location
            assert "code_challenge=" in location
            assert "code_challenge_method=S256" in location
            assert "scope=openid+profile+email+capabilities" in location


class TestCallback:
    @pytest.mark.asyncio
    async def test_exchanges_code_and_sets_session(self, mock_api) -> None:
        app = make_app()
        async with TestClient(TestServer(app)) as client:
            await _login_session(client, mock_api)

            # Verify session is set by hitting /api/me
            resp = await client.get("/api/me")
            assert resp.status == 200
            data = await resp.json()
            assert data["name"] == "Alice Smith"
            assert "control_power" in data["capabilities"]

    @pytest.mark.asyncio
    async def test_invalid_state_returns_error(self, mock_api) -> None:
        app = make_app()
        async with TestClient(TestServer(app)) as client:
            # First visit /login to set up a session
            await client.get("/login", allow_redirects=False)

            # Hit callback with wrong state
            resp = await client.get("/callback?code=fake-code&state=wrong-state")
            assert resp.status == 400


class TestLogout:
    @pytest.mark.asyncio
    async def test_clears_session(self, mock_api) -> None:
        app = make_app()
        async with TestClient(TestServer(app)) as client:
            await _login_session(client, mock_api)

            # Verify logged in
            resp = await client.get("/")
            assert resp.status == 200

            # Logout
            resp = await client.get("/logout", allow_redirects=False)
            assert resp.status == 302
            assert resp.headers["Location"] == "/"

            # `/` is public-readable; after logout the request still succeeds
            # but the handler sees authed=False.
            resp = await client.get("/")
            assert resp.status == 200
            assert (await resp.json()) == {"authed": False}

            # A private path should now redirect to /login.
            resp = await client.get("/admin", allow_redirects=False)
            assert resp.status == 302
            assert resp.headers["Location"] == "/login"


class TestMe:
    @pytest.mark.asyncio
    async def test_returns_user_info_when_authed(self, mock_api) -> None:
        app = make_app()
        async with TestClient(TestServer(app)) as client:
            await _login_session(client, mock_api)
            resp = await client.get("/api/me")
            assert resp.status == 200
            data = await resp.json()
            assert data["authenticated"] is True
            assert data["name"] == "Alice Smith"
            assert data["email"] == "alice@theflip.museum"
            assert "control_power" in data["capabilities"]

    @pytest.mark.asyncio
    async def test_unauthenticated_returns_authed_false(self) -> None:
        # /api/me is public-readable so the dashboard JS can probe auth
        # state without an extra round-trip for anonymous viewers.
        app = make_app()
        async with TestClient(TestServer(app)) as client:
            resp = await client.get("/api/me")
            assert resp.status == 200
            assert (await resp.json()) == {"authenticated": False}


class TestCapabilities:
    @pytest.mark.asyncio
    async def test_power_allowed_with_capability(self, mock_api) -> None:
        app = make_app()
        async with TestClient(TestServer(app)) as client:
            await _login_session(client, mock_api)
            resp = await client.post("/api/machines/1/power", json={"on": True})
            assert resp.status == 200
            data = await resp.json()
            assert data["ok"] is True

    @pytest.mark.asyncio
    async def test_power_denied_without_capability(self, mock_api) -> None:
        app = make_app()
        async with TestClient(TestServer(app)) as client:
            # Login with no capabilities
            resp = await client.get("/login", allow_redirects=False)
            location = resp.headers["Location"]
            from urllib.parse import parse_qs, urlparse

            params = parse_qs(urlparse(location).query)
            state = params["state"][0]

            _stub_token(mock_api)
            _stub_userinfo(mock_api, capabilities=[])  # No control_power

            resp = await client.get(
                f"/callback?code=fake-code&state={state}", allow_redirects=False
            )
            assert resp.status == 302

            # Try to control power
            resp = await client.post("/api/machines/1/power", json={"on": True})
            assert resp.status == 403
            data = await resp.json()
            assert "permission" in data["error"].lower()


class TestNoAuth:
    @pytest.mark.asyncio
    async def test_power_allowed_without_oauth(self) -> None:
        """When OAuth is not configured, power endpoint works without auth."""
        from juice.auth import require_capability

        app = web.Application()

        async def mock_power(request):
            error = require_capability(request, "control_power")
            if error:
                return error
            return web.json_response({"ok": True})

        app.router.add_post("/api/machines/1/power", mock_power)

        async with TestClient(TestServer(app)) as client:
            resp = await client.post("/api/machines/1/power", json={"on": True})
            assert resp.status == 200
            data = await resp.json()
            assert data["ok"] is True
