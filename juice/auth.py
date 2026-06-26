"""OAuth SSO authentication via FlipFix OIDC provider."""

from __future__ import annotations

import hashlib
import logging
import re
from urllib.parse import urlencode

import aiohttp
from aiohttp import ClientTimeout, web
from aiohttp_session import get_session
from aiohttp_session import setup as setup_session_middleware
from aiohttp_session.cookie_storage import EncryptedCookieStorage
from authlib.common.security import generate_token
from authlib.oauth2.rfc7636 import create_s256_code_challenge

log = logging.getLogger(__name__)

# Paths that bypass auth completely (the OAuth flow itself).
# Exact paths the auth middleware passes straight through, before any session
# check. /api/backup self-authorizes with its own bearer token (see
# handle_backup), so it must bypass the OAuth gate rather than 401/redirect.
PUBLIC_PATHS = {
    "/login",
    "/callback",
    "/logout",
    "/api/backup",
    "/favicon.svg",
    "/favicon.ico",
}

# GET paths that unauthenticated requests are allowed to read. Handlers
# matching these paths can use is_authenticated(request) to decide which
# fields/UI elements to surface to public vs. logged-in viewers. Every
# other path requires authentication.
PUBLIC_READABLE_PATTERNS: tuple[re.Pattern[str], ...] = (
    re.compile(r"^/$"),
    re.compile(r"^/machine/[^/]+$"),
    re.compile(r"^/usage$"),
    re.compile(r"^/air$"),
    re.compile(r"^/api/machines$"),
    # SSE live stream — pushes the same operational status the public dashboard
    # already shows (power/state, machine names, plug ids), so it's public too.
    re.compile(r"^/api/events$"),
    re.compile(r"^/api/usage$"),
    re.compile(r"^/api/play-hours$"),
    re.compile(r"^/api/busy-grid$"),
    re.compile(r"^/api/air$"),
    re.compile(r"^/api/air/[^/]+/history$"),
    re.compile(r"^/api/machines/[^/]+/readings$"),
    re.compile(r"^/api/machines/[^/]+/peak$"),
    re.compile(r"^/api/me$"),
)


def _is_public_readable(request: web.Request) -> bool:
    if request.method != "GET":
        return False
    return any(p.match(request.path) for p in PUBLIC_READABLE_PATTERNS)


def is_authenticated(request: web.Request) -> bool:
    """True when the requester should see operator-level info.

    When no auth is wired into the app — ``create_app`` with neither OAuth nor
    the dev shim, e.g. handler-level unit tests that build requests directly —
    there's no gate to pass, so treat everyone as authed. Once auth is active —
    real OAuth (``setup_auth``) or the local dev shim (``setup_dev_auth``) —
    only a request with a session-bound user counts.
    """
    app = request.app
    if oauth_config_key not in app and dev_auth_key not in app:
        return True
    return request.get("user") is not None


oauth_config_key: web.AppKey[dict] = web.AppKey("oauth_config")
# Marker that the dev login shim is installed (no real OAuth). Lets
# is_authenticated / require_capability gate dev the same way as production.
dev_auth_key: web.AppKey[bool] = web.AppKey("dev_auth")
OAUTH_TIMEOUT = ClientTimeout(total=30)


def setup_auth(app: web.Application, oauth_config: dict) -> None:
    """Configure session storage, auth middleware, and OAuth routes."""
    # Derive Fernet key from client secret (EncryptedCookieStorage base64-encodes bytes)
    secret_bytes = hashlib.sha256(oauth_config["client_secret"].encode()).digest()
    storage = EncryptedCookieStorage(secret_bytes)
    setup_session_middleware(app, storage)

    app[oauth_config_key] = oauth_config
    app.middlewares.append(auth_middleware)

    app.router.add_get("/login", handle_login)
    app.router.add_get("/callback", handle_callback)
    app.router.add_get("/logout", handle_logout)
    app.router.add_get("/api/me", handle_me)


# Stable (non-secret) key so dev sessions survive a server reload. This shim is
# only installed when OAuth is NOT configured, so it never guards anything real.
_DEV_SESSION_KEY = hashlib.sha256(b"juice-dev-session").digest()


def setup_dev_auth(app: web.Application) -> None:
    """Local stand-in for OAuth so a no-OAuth ``juice serve`` still shows the
    production logged-out → login → logout flow.

    It reuses the real gating middleware and session handling; only "logging
    in" differs — a one-click ``/login`` that mints a local operator session
    instead of bouncing through FlipFix. Never installed when OAuth is
    configured, so production is untouched.
    """
    storage = EncryptedCookieStorage(_DEV_SESSION_KEY)
    setup_session_middleware(app, storage)

    app[dev_auth_key] = True
    app.middlewares.append(auth_middleware)

    app.router.add_get("/login", handle_dev_login)
    app.router.add_get("/logout", handle_logout)
    app.router.add_get("/api/me", handle_me)


async def handle_dev_login(request: web.Request) -> web.Response:
    """Dev-only one-click login: mint a local operator session (with the
    control_power capability), no OAuth provider involved."""
    session = await get_session(request)
    session["user"] = {"sub": "dev", "name": "Developer", "email": "dev@localhost"}
    session["capabilities"] = ["control_power"]
    raise web.HTTPFound("/")


@web.middleware
async def auth_middleware(request: web.Request, handler):
    if request.path in PUBLIC_PATHS:
        return await handler(request)

    session = await get_session(request)
    user = session.get("user")
    if user:
        request["user"] = user
        request["capabilities"] = session.get("capabilities", [])
        return await handler(request)

    # Unauthenticated. Public-readable GETs proceed without a user in the
    # request bag — handlers branch on is_authenticated() to decide what
    # to surface. Everything else: 401 for API, redirect-to-login for HTML.
    if _is_public_readable(request):
        return await handler(request)

    if request.path.startswith("/api/"):
        return web.json_response({"error": "Not authenticated"}, status=401)
    raise web.HTTPFound("/login")


def require_capability(request: web.Request, capability: str) -> web.Response | None:
    """Gate a write action behind a capability.

    Returns None (proceed) when:
      - no auth is wired into the app (create_app with neither OAuth nor the
        dev shim — e.g. handler-level unit tests), OR
      - the requester is authenticated AND has the capability.

    Returns a 401 when auth is active (real OAuth or the dev shim) but the
    requester is unauthenticated (e.g. arriving via a public-readable
    GET handler that also handles writes — defence in depth).
    Returns a 403 when authenticated but lacking the capability.
    """
    if oauth_config_key not in request.app and dev_auth_key not in request.app:
        return None
    if not is_authenticated(request):
        return web.json_response({"error": "Not authenticated"}, status=401)
    capabilities = request.get("capabilities", [])
    if capability not in capabilities:
        return web.json_response(
            {"error": f"Permission denied: requires {capability}"},
            status=403,
        )
    return None


async def handle_login(request: web.Request) -> web.Response:
    """Redirect to FlipFix authorize endpoint with PKCE."""
    config = request.app[oauth_config_key]
    session = await get_session(request)

    state = generate_token(32)
    code_verifier = generate_token(48)
    code_challenge = create_s256_code_challenge(code_verifier)

    session["oauth_state"] = state
    session["code_verifier"] = code_verifier

    params = {
        "response_type": "code",
        "client_id": config["client_id"],
        "redirect_uri": config["redirect_uri"],
        "scope": "openid profile email capabilities",
        "state": state,
        "code_challenge": code_challenge,
        "code_challenge_method": "S256",
    }
    authorize_url = f"{config['provider_url']}/oauth/authorize/?{urlencode(params)}"
    raise web.HTTPFound(authorize_url)


async def handle_callback(request: web.Request) -> web.Response:
    """Exchange authorization code for tokens and fetch user info."""
    config = request.app[oauth_config_key]
    session = await get_session(request)

    # Verify state
    state = request.query.get("state")
    expected_state = session.get("oauth_state")
    if not state or state != expected_state:
        return web.json_response({"error": "Invalid state parameter"}, status=400)

    code = request.query.get("code")
    code_verifier = session.get("code_verifier")

    # Exchange code for tokens
    async with aiohttp.ClientSession(timeout=OAUTH_TIMEOUT) as http:
        token_resp = await http.post(
            f"{config['provider_url']}/oauth/token/",
            data={
                "grant_type": "authorization_code",
                "code": code,
                "redirect_uri": config["redirect_uri"],
                "client_id": config["client_id"],
                "client_secret": config["client_secret"],
                "code_verifier": code_verifier,
            },
        )
        if token_resp.status != 200:
            log.warning("Token exchange failed: %s", await token_resp.text())
            return web.json_response({"error": "Token exchange failed"}, status=502)
        token_data = await token_resp.json()

        # Fetch user info
        userinfo_resp = await http.get(
            f"{config['provider_url']}/oauth/userinfo/",
            headers={"Authorization": f"Bearer {token_data['access_token']}"},
        )
        if userinfo_resp.status != 200:
            log.warning("UserInfo request failed: %s", await userinfo_resp.text())
            return web.json_response({"error": "UserInfo request failed"}, status=502)
        userinfo = await userinfo_resp.json()

    # Store in session
    session["user"] = {
        "sub": userinfo.get("sub"),
        "name": userinfo.get("name", ""),
        "email": userinfo.get("email", ""),
    }
    session["capabilities"] = userinfo.get("https://flipfix.theflip.museum/capabilities", [])
    session["access_token"] = token_data["access_token"]

    # Clean up OAuth state
    session.pop("oauth_state", None)
    session.pop("code_verifier", None)

    raise web.HTTPFound("/")


async def handle_logout(request: web.Request) -> web.Response:
    """Clear session and redirect to home."""
    session = await get_session(request)
    session.clear()
    raise web.HTTPFound("/")


async def handle_me(request: web.Request) -> web.Response:
    """Return the requester's auth state + user info (when logged in).

    Public-readable: unauthenticated callers get {"authenticated": false}.
    """
    if not is_authenticated(request):
        return web.json_response({"authenticated": False})
    user = request.get("user", {})
    return web.json_response(
        {
            "authenticated": True,
            "name": user.get("name", ""),
            "email": user.get("email", ""),
            "capabilities": request.get("capabilities", []),
        }
    )
