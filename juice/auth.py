"""OAuth SSO authentication via FlipFix OIDC provider."""

from __future__ import annotations

import hashlib
import logging
from urllib.parse import urlencode

import aiohttp
from aiohttp import ClientTimeout, web
from aiohttp_session import get_session
from aiohttp_session import setup as setup_session_middleware
from aiohttp_session.cookie_storage import EncryptedCookieStorage
from authlib.common.security import generate_token
from authlib.oauth2.rfc7636 import create_s256_code_challenge

log = logging.getLogger(__name__)

PUBLIC_PATHS = {"/login", "/callback", "/logout"}

oauth_config_key: web.AppKey[dict] = web.AppKey("oauth_config")
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


@web.middleware
async def auth_middleware(request: web.Request, handler):
    if request.path in PUBLIC_PATHS:
        return await handler(request)

    session = await get_session(request)
    user = session.get("user")
    if not user:
        if request.path.startswith("/api/"):
            return web.json_response({"error": "Not authenticated"}, status=401)
        raise web.HTTPFound("/login")

    request["user"] = user
    request["capabilities"] = session.get("capabilities", [])
    return await handler(request)


def require_capability(request: web.Request, capability: str) -> web.Response | None:
    """Return a 403 response if the user lacks the capability, or None if OK.

    When OAuth is not configured (no auth middleware), allow access.
    """
    if oauth_config_key not in request.app:
        return None
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
    """Return current user info and capabilities."""
    user = request.get("user", {})
    capabilities = request.get("capabilities", [])
    return web.json_response(
        {
            "name": user.get("name", ""),
            "email": user.get("email", ""),
            "capabilities": capabilities,
        }
    )
