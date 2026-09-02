"""An `/api/v2` client, written from `api_v2.md` and nothing else.

Deliberately imports no `juice.*` server module. The point of the TUI is to
find out whether the documented contract is enough to build a client from; a
client that reached into `juice.server` for a shape would answer that question
by cheating.

Three pieces, separable so the tricky parts are testable without a server:

  * `SseParser`  — bytes → complete SSE blocks. The transport.
  * `StreamTracker` — §5's client rules: the epoch lives on `hello` only,
    `seq` is dense, a gap or a `resync_required` means refetch the floor.
  * `JuiceClient` — the requests, plus `stream()` which wires the two together
    and reconnects with backoff.

There is no polling anywhere, by design: §1.3 says the stream replaces it.
"""

from __future__ import annotations

import asyncio
import json
import random
import re
from collections.abc import AsyncIterator, Iterable
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlsplit

import aiohttp
from yarl import URL

# The documented idle heartbeat is a comment frame every 15s, so four missed in
# a row means the connection is dead even though the socket still looks open.
# Reading ticks are NOT a liveness signal: their cadence is the recorder's poll
# period, which on production runs 6-9s and grows with the number of outlets.
STREAM_READ_TIMEOUT = 60.0
REQUEST_TIMEOUT = 15.0

# Reconnect backoff. api_v2.md §5 warns that a client resyncing every second is
# what overflowed the server's queue in the first place, so this is not linear.
BACKOFF_INITIAL = 0.5
BACKOFF_MAX = 30.0

# aiohttp_session's default cookie name, which juice does not override.
SESSION_COOKIE = "AIOHTTP_SESSION"

# RFC 6265 token characters, minus the ones no cookie name uses in practice.
_COOKIE_NAME = re.compile(r"^[!#$%&\'*+\-.^_`|~0-9A-Za-z]{1,64}$")


def parse_cookies(specs: Iterable[str]) -> dict[str, str]:
    """`NAME=VALUE` pairs, a whole `Cookie:` header, or a bare session value.

    The bare-value case needs care: a session cookie is a Fernet token, which is
    base64 with `=` padding, so a naive split on the first `=` turns the token
    itself into the cookie name and sends nothing usable. Anything whose
    left-hand side isn't a plausible cookie name is treated as a value for
    `SESSION_COOKIE` instead.
    """
    cookies: dict[str, str] = {}
    for spec in specs:
        for part in spec.split(";"):
            part = part.strip()
            if not part:
                continue
            name, sep, value = part.partition("=")
            if sep and _COOKIE_NAME.match(name) and value:
                cookies[name] = value
            elif sep and _COOKIE_NAME.match(name) and not value:
                raise ValueError(f"cookie {name!r} has no value")
            else:
                cookies[SESSION_COOKIE] = part
    if not cookies:
        raise ValueError("no cookie given")
    return cookies


def looks_external(base_url: str, landed_url: object) -> bool:
    """Did `/login` send us to somebody else's host?

    That is the whole difference between the dev-auth shim (same origin, one
    request, done) and real OAuth (off to FlipFix, which no non-browser client
    can complete).
    """
    landed = urlsplit(str(landed_url))
    if not landed.netloc:
        return False
    return landed.netloc != urlsplit(base_url).netloc


class ApiError(Exception):
    """A request that did not return what was asked for.

    `code` is always populated, so callers branch on it and never on `message`:
    the documented envelope's code where there is one, `http_<status>` for a
    response that isn't the envelope at all (a proxy's HTML 502), and
    `unreachable` with `status == 0` when the request never got an answer.

    That last case matters more than it looks: a client that lets transport
    errors through has one error path for "the server said no" and another for
    "the server is gone", and the second one is exactly what happens during the
    resync that a server restart triggers.
    """

    def __init__(self, status: int, code: str, message: str, detail: Any = None) -> None:
        super().__init__(f"{status} {code}: {message}")
        self.status = status
        self.code = code
        self.message = message
        self.detail = detail


@dataclass(frozen=True)
class SseBlock:
    """One complete SSE block: either a comment or a `data:` payload."""

    data: str | None = None
    comment: str | None = None


class SseParser:
    """Incremental SSE framing.

    Feeding raw chunks rather than iterating lines matters: aiohttp hands over
    whatever arrived, which routinely splits a frame mid-JSON, and a client that
    decodes per-chunk drops those frames — which then looks exactly like a
    server-side sequence gap.
    """

    def __init__(self) -> None:
        self._buffer = ""

    def feed(self, chunk: bytes | str) -> list[SseBlock]:
        text = chunk.decode("utf-8", "replace") if isinstance(chunk, bytes) else chunk
        self._buffer += text.replace("\r\n", "\n").replace("\r", "\n")

        blocks: list[SseBlock] = []
        while "\n\n" in self._buffer:
            raw, self._buffer = self._buffer.split("\n\n", 1)
            blocks.extend(_parse_block(raw))
        return blocks


def _parse_block(raw: str) -> list[SseBlock]:
    """One dispatched block → zero or more `SseBlock`s.

    A block carrying neither a comment nor data (a stray blank) yields nothing
    rather than an empty frame the UI would have to filter out again.
    """
    data_lines: list[str] = []
    comments: list[str] = []
    for line in raw.split("\n"):
        if not line:
            continue
        if line.startswith(":"):
            comments.append(line[1:].strip())
        elif line.startswith("data:"):
            data_lines.append(line[5:].removeprefix(" "))
        # Other SSE fields (event:, id:, retry:) are unused by /api/v2.

    blocks = [SseBlock(comment=c) for c in comments]
    if data_lines:
        blocks.append(SseBlock(data="\n".join(data_lines)))
    return blocks


@dataclass(frozen=True)
class Frame:
    """Something to show in the stream pane.

    `kind` is `event` for anything the server sent, and `comment`, `resync`,
    `connected` or `disconnected` for the things the client itself observed.
    Keeping the synthetic ones in the same stream is the whole point: a gap is
    exactly as interesting as the frames around it.
    """

    kind: str
    seq: int | None = None
    type: str | None = None
    data: dict[str, Any] = field(default_factory=dict)
    raw: str = ""
    reason: str | None = None
    detail: str = ""


class StreamTracker:
    """The client contract from api_v2.md §5, in one place.

    Emits a `resync` frame — meaning *refetch /api/v2/floor* — for each of the
    three documented causes, and nothing else. In particular the epoch is read
    from `hello` only; comparing it on every frame tests `None` against the
    known value on each reading tick and resyncs forever.
    """

    def __init__(self) -> None:
        self.epoch: str | None = None
        self.last_seq: int | None = None

    def observe(self, event: dict[str, Any], raw: str = "") -> list[Frame]:
        kind = event.get("type")
        seq = event.get("seq")
        frame = Frame(kind="event", seq=seq, type=kind, data=event, raw=raw)

        if kind == "hello":
            # `hello` restarts the sequence, so it is never itself a gap.
            epoch = event.get("epoch")
            changed = self.epoch is not None and epoch != self.epoch
            self.epoch = epoch
            self.last_seq = seq
            if changed:
                return [
                    Frame(
                        kind="resync",
                        seq=seq,
                        reason="epoch",
                        detail="server restarted — sequence numbers restarted too",
                    ),
                    frame,
                ]
            return [frame]

        out: list[Frame] = []
        if self.last_seq is not None and seq != self.last_seq + 1:
            out.append(
                Frame(
                    kind="resync",
                    seq=seq,
                    reason="gap",
                    detail=f"seq {self.last_seq} → {seq}: events were dropped",
                )
            )
        if seq is not None:
            self.last_seq = seq

        out.append(frame)

        if kind == "resync_required":
            out.append(
                Frame(
                    kind="resync",
                    seq=seq,
                    reason="server",
                    detail="server asked for a resync",
                )
            )
        return out


class JuiceClient:
    """Reads and a live stream against one juice server.

    The session is created on first use and reused, so there is no connect step
    to forget; `close()` (or `async with`) releases it.

    Cookies are the whole of juice's auth story for a non-browser client. Under
    the dev shim `login()` mints an operator session in one request; against
    real OAuth there is no non-browser path at all, so `cookies=` takes a
    session lifted from a logged-in browser instead.
    """

    def __init__(self, base_url: str, cookies: dict[str, str] | None = None) -> None:
        self.base_url = base_url.rstrip("/")
        self.identity: dict[str, Any] | None = None
        self._cookies = dict(cookies or {})
        self._session: aiohttp.ClientSession | None = None

    @property
    def authenticated(self) -> bool:
        return bool(self.identity and self.identity.get("authenticated"))

    @property
    def who(self) -> str:
        if not self.authenticated:
            return "anon"
        assert self.identity is not None
        name = self.identity.get("email") or self.identity.get("name") or "?"
        caps = self.identity.get("capabilities") or []
        return f"{name}{' +control_power' if 'control_power' in caps else ''}"

    async def __aenter__(self) -> JuiceClient:
        return self

    async def __aexit__(self, *_exc: object) -> None:
        await self.close()

    async def close(self) -> None:
        if self._session is not None:
            await self._session.close()
            self._session = None

    def _http(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT),
                # aiohttp's default jar silently discards cookies set by an
                # IP-address host, so a client pointed at 127.0.0.1 logs in,
                # gets a session cookie, drops it, and stays anonymous with no
                # error anywhere. Every juice dev server is on an IP.
                cookie_jar=aiohttp.CookieJar(unsafe=True),
            )
            if self._cookies:
                # With a response_url, so the jar scopes them to this server
                # rather than to every host the client might ever touch.
                self._session.cookie_jar.update_cookies(
                    self._cookies, response_url=URL(self.base_url)
                )
        return self._session

    def session_cookies(self) -> dict[str, str]:
        """This client's cookies for the server, ready to hand to another one."""
        jar = self._http().cookie_jar
        return {c.key: c.value for c in jar.filter_cookies(URL(self.base_url)).values()}

    async def _get(self, path: str, **params: Any) -> Any:
        query = {k: v for k, v in params.items() if v is not None}
        with _as_api_error():
            async with self._http().get(self.base_url + path, params=query) as response:
                body = await response.text()
                if response.status >= 400:
                    raise _error_from(response.status, body)
                return json.loads(body) if body else None

    # --- reads -----------------------------------------------------------

    async def floor(self) -> dict[str, Any]:
        return await self._get("/api/v2/floor")

    async def machines(self) -> list[dict[str, Any]]:
        return (await self._get("/api/v2/machines"))["machines"]

    async def machine(self, asset_id: str) -> dict[str, Any]:
        return await self._get(f"/api/v2/machines/{asset_id}")

    # --- session ---------------------------------------------------------

    async def me(self) -> dict[str, Any]:
        """Who the server thinks we are. `/api/me` predates v2 and is v1's.

        v2 has no equivalent, so a v2-only client cannot answer "am I logged
        in?" without inferring it from whether an operator-only key came back.
        """
        self.identity = await self._get("/api/me")
        return self.identity

    async def login(self) -> dict[str, Any]:
        """One-click login against the dev-auth shim.

        Only works when the server runs without OAuth. Against a server with
        real OAuth, `/login` redirects to FlipFix's authorization endpoint,
        which no non-browser client can complete — that raises `oauth_required`
        rather than reporting a vague failure, because the fix is completely
        different: bring a session cookie in from a browser instead.
        """
        with _as_api_error():
            async with self._http().get(f"{self.base_url}/login") as response:
                await response.read()
                landed = response.url
        identity = await self.me()
        if not self.authenticated and looks_external(self.base_url, landed):
            raise ApiError(
                0,
                "oauth_required",
                f"this server authenticates through {urlsplit(str(landed)).netloc}; "
                "a non-browser client cannot complete that flow — pass --cookie "
                "with a session lifted from a logged-in browser",
            )
        return identity

    async def logout(self) -> dict[str, Any]:
        with _as_api_error():
            async with self._http().get(f"{self.base_url}/logout") as response:
                await response.read()
        return await self.me()

    # --- the stream ------------------------------------------------------

    async def stream(self) -> AsyncIterator[Frame]:
        """`GET /api/v2/stream`, forever, reconnecting on failure.

        Yields server events, heartbeat comments, and the client's own
        observations. Every `Frame` with `kind == "resync"` means the same
        thing: refetch `/api/v2/floor`.

        The tracker is kept across reconnects on purpose — that is what lets a
        restarted server be recognised by its new epoch instead of silently
        continuing with a sequence that started over.
        """
        tracker = StreamTracker()
        backoff = BACKOFF_INITIAL
        first = True

        while True:
            if not first:
                # A reconnect always missed whatever happened while we were
                # away, whether or not the epoch changed.
                yield Frame(
                    kind="resync",
                    reason="reconnect",
                    detail=f"reconnecting in {backoff:.1f}s",
                )
                await asyncio.sleep(backoff + random.uniform(0, backoff / 2))  # noqa: S311
                backoff = min(backoff * 2, BACKOFF_MAX)
            first = False

            try:
                async for frame in self._read_stream(tracker):
                    if frame.kind == "event":
                        backoff = BACKOFF_INITIAL  # a live connection resets it
                    yield frame
            except (TimeoutError, aiohttp.ClientError, OSError) as exc:
                yield Frame(kind="disconnected", detail=f"{type(exc).__name__}: {exc}")
            else:
                yield Frame(kind="disconnected", detail="server closed the stream")

    async def _read_stream(self, tracker: StreamTracker) -> AsyncIterator[Frame]:
        timeout = aiohttp.ClientTimeout(total=None, sock_read=STREAM_READ_TIMEOUT)
        headers = {"Accept": "text/event-stream"}
        async with self._http().get(
            f"{self.base_url}/api/v2/stream", headers=headers, timeout=timeout
        ) as response:
            if response.status >= 400:
                raise _error_from(response.status, await response.text())
            yield Frame(kind="connected", detail=f"{self.base_url}/api/v2/stream")

            parser = SseParser()
            async for chunk in response.content.iter_any():
                for block in parser.feed(chunk):
                    if block.comment is not None:
                        yield Frame(kind="comment", raw=f": {block.comment}")
                        continue
                    assert block.data is not None
                    try:
                        event = json.loads(block.data)
                    except json.JSONDecodeError:
                        yield Frame(
                            kind="disconnected", detail=f"undecodable frame: {block.data[:120]}"
                        )
                        continue
                    for frame in tracker.observe(event, raw=block.data):
                        yield frame


@contextmanager
def _as_api_error():
    """Present a transport failure as an `ApiError`, like any other refusal."""
    try:
        yield
    except ApiError:
        raise
    except (aiohttp.ClientError, TimeoutError, OSError) as exc:
        raise ApiError(0, "unreachable", f"{type(exc).__name__}: {exc}") from exc


def _error_from(status: int, body: str) -> ApiError:
    try:
        envelope = json.loads(body)["error"]
        return ApiError(
            status, envelope["code"], envelope.get("message", ""), envelope.get("detail")
        )
    except json.JSONDecodeError, KeyError, TypeError:
        # v1 returns a flat string for 401 and a proxy returns HTML; neither is
        # the envelope, and a client that assumes it gets a KeyError instead of
        # a usable error.
        return ApiError(status, f"http_{status}", body.strip()[:200] or f"HTTP {status}")
