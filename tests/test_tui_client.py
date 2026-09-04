"""Tests for juice.tui.client — the /api/v2 client the TUI is built on.

This client is deliberately written against `api_v2.md` alone and imports
nothing from `juice.server`; these tests hold it to the wire contract rather
than to the server's internals. The SSE parsing and the seq/epoch rules of §5
are where a real client goes wrong, so they get the most attention.
"""

from __future__ import annotations

import asyncio
import json

import pytest
from aiohttp import web
from aioresponses import aioresponses

from juice.tui.client import (
    SESSION_COOKIE,
    ApiError,
    JuiceClient,
    SseParser,
    StreamTracker,
    looks_external,
    parse_cookies,
)

# --------------------------------------------------------------------------
# SSE framing
# --------------------------------------------------------------------------


def _fed(chunks: list[bytes]) -> list:
    parser = SseParser()
    out = []
    for chunk in chunks:
        out.extend(parser.feed(chunk))
    return out


def test_parser_emits_one_block_per_frame():
    blocks = _fed([b'data: {"seq":1}\n\ndata: {"seq":2}\n\n'])
    assert [b.data for b in blocks] == ['{"seq":1}', '{"seq":2}']


def test_parser_reassembles_a_frame_split_mid_json():
    """The wire does not respect frame boundaries; a naive line split loses data."""
    blocks = _fed([b'data: {"seq":1,"ty', b'pe":"hello"}\n', b"\n"])
    assert [b.data for b in blocks] == ['{"seq":1,"type":"hello"}']


def test_parser_holds_an_incomplete_frame_until_its_blank_line():
    parser = SseParser()
    assert parser.feed(b'data: {"seq":1}\n') == []
    assert [b.data for b in parser.feed(b"\n")] == ['{"seq":1}']


def test_parser_joins_multi_line_data_with_newlines():
    blocks = _fed([b"data: line one\ndata: line two\n\n"])
    assert [b.data for b in blocks] == ["line one\nline two"]


def test_parser_surfaces_comments_without_decoding_them():
    """`: ping` is the heartbeat. It is not JSON and must not be parsed as any."""
    blocks = _fed([b": ping\n\n"])
    assert len(blocks) == 1
    assert blocks[0].data is None
    assert blocks[0].comment == "ping"


def test_parser_survives_a_frame_split_across_the_blank_line():
    blocks = _fed([b"data: a\n\ndata: b\n", b"\ndata: c\n\n"])
    assert [b.data for b in blocks] == ["a", "b", "c"]


# --------------------------------------------------------------------------
# The §5 client contract: epoch on hello only, dense seq, explicit resync
# --------------------------------------------------------------------------


def _types(frames) -> list[str]:
    return [f.type or f.reason for f in frames]


def test_hello_sets_the_epoch_without_reporting_a_gap():
    tracker = StreamTracker()
    frames = tracker.observe({"seq": 1, "type": "hello", "epoch": "abc"})
    assert _types(frames) == ["hello"]
    assert tracker.epoch == "abc"
    assert tracker.last_seq == 1


def test_dense_sequence_passes_through_untouched():
    tracker = StreamTracker()
    tracker.observe({"seq": 1, "type": "hello", "epoch": "abc"})
    frames = tracker.observe({"seq": 2, "type": "reading_tick", "machines": []})
    assert _types(frames) == ["reading_tick"]


def test_a_sequence_gap_asks_for_a_resync_once():
    tracker = StreamTracker()
    tracker.observe({"seq": 5, "type": "hello", "epoch": "abc"})
    frames = tracker.observe({"seq": 7, "type": "reading_tick", "machines": []})
    assert [f.kind for f in frames] == ["resync", "event"]
    assert frames[0].reason == "gap"
    assert "5" in frames[0].detail and "7" in frames[0].detail
    # The gap is not re-reported on the next in-order frame.
    assert _types(tracker.observe({"seq": 8, "type": "reading_tick"})) == ["reading_tick"]


def test_reading_ticks_never_carry_an_epoch_so_they_cannot_trip_one():
    """Checking `epoch` on every frame compares undefined against the known
    value and resyncs forever — api_v2.md §5 calls this out by name."""
    tracker = StreamTracker()
    tracker.observe({"seq": 1, "type": "hello", "epoch": "abc"})
    frames = tracker.observe({"seq": 2, "type": "reading_tick", "machines": []})
    assert all(f.kind == "event" for f in frames)


def test_a_new_epoch_on_reconnect_asks_for_a_resync():
    tracker = StreamTracker()
    tracker.observe({"seq": 1, "type": "hello", "epoch": "abc"})
    frames = tracker.observe({"seq": 1, "type": "hello", "epoch": "xyz"})
    assert [f.kind for f in frames] == ["resync", "event"]
    assert frames[0].reason == "epoch"
    assert tracker.epoch == "xyz"


def test_the_same_epoch_on_reconnect_does_not():
    tracker = StreamTracker()
    tracker.observe({"seq": 1, "type": "hello", "epoch": "abc"})
    frames = tracker.observe({"seq": 1, "type": "hello", "epoch": "abc"})
    assert [f.kind for f in frames] == ["event"]


def test_resync_required_is_honoured_as_the_server_sending_it():
    tracker = StreamTracker()
    tracker.observe({"seq": 1, "type": "hello", "epoch": "abc"})
    frames = tracker.observe({"seq": 2, "type": "resync_required"})
    assert [f.kind for f in frames] == ["event", "resync"]
    assert frames[1].reason == "server"


def test_a_gapped_resync_required_reports_both_causes():
    tracker = StreamTracker()
    tracker.observe({"seq": 1, "type": "hello", "epoch": "abc"})
    frames = tracker.observe({"seq": 9, "type": "resync_required"})
    assert [f.reason for f in frames if f.kind == "resync"] == ["gap", "server"]


# --------------------------------------------------------------------------
# Errors: the {"error": {"code", ...}} envelope
# --------------------------------------------------------------------------


async def test_an_error_response_raises_with_the_code_branchable():
    body = {
        "error": {
            "code": "ambiguous_assignment",
            "message": "M0021 is claimed by more than one online outlet",
            "detail": {"candidates": [3, 9]},
        }
    }
    with aioresponses() as mocked:
        mocked.get("http://juice.test/api/v2/machines/M0021", status=409, payload=body)
        async with JuiceClient("http://juice.test") as client:
            with pytest.raises(ApiError) as excinfo:
                await client.machine("M0021")
    error = excinfo.value
    assert error.status == 409
    assert error.code == "ambiguous_assignment"
    assert error.detail == {"candidates": [3, 9]}


async def test_a_non_json_error_still_raises_an_apierror():
    """A proxy 502 is HTML. A client that assumes the envelope crashes on it."""
    with aioresponses() as mocked:
        mocked.get("http://juice.test/api/v2/floor", status=502, body="<html>bad gateway</html>")
        async with JuiceClient("http://juice.test") as client:
            with pytest.raises(ApiError) as excinfo:
                await client.floor()
    assert excinfo.value.status == 502
    assert excinfo.value.code == "http_502"


async def test_machines_returns_the_list_not_the_envelope():
    with aioresponses() as mocked:
        mocked.get(
            "http://juice.test/api/v2/machines",
            payload={"machines": [{"asset_id": "M0001", "name": "Blackout"}]},
        )
        async with JuiceClient("http://juice.test") as client:
            machines = await client.machines()
    assert [m["asset_id"] for m in machines] == ["M0001"]


# --------------------------------------------------------------------------
# Against the real thing
# --------------------------------------------------------------------------


@pytest.fixture(scope="module")
def fixture_server():
    """The cloud-free fixture server, with problems, on an ephemeral port.

    Parser tests can all pass against a client that never speaks to juice; this
    is the one that would notice.
    """
    import socket
    import subprocess
    import sys
    import time
    import urllib.error
    import urllib.request

    with socket.socket() as probe:
        probe.bind(("127.0.0.1", 0))
        port = probe.getsockname()[1]

    # sys.executable, not "uv run": the test already runs inside the venv, and
    # a nested resolve adds seconds to every run of this module.
    proc = subprocess.Popen(  # noqa: S603
        [
            sys.executable,
            "-m",
            "tests.e2e.serve",
            "--port",
            str(port),
            "--interactive",
            "--with-problems",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 90
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                pytest.skip("fixture server exited during startup")
            try:
                urllib.request.urlopen(f"{url}/api/v2/floor", timeout=1).read()  # noqa: S310
                break
            except urllib.error.URLError, OSError, TimeoutError:
                time.sleep(0.5)
        else:
            pytest.skip("fixture server did not come up in time")
        yield url
    finally:
        proc.terminate()
        proc.wait(timeout=20)


async def test_floor_against_the_fixture_has_real_problems_in_it(fixture_server):
    async with JuiceClient(fixture_server) as client:
        await client.login()
        floor = await client.floor()

    assert floor["counts"]["total"] > 20
    assert floor["counts"]["problems"] == len(floor["problems"])
    statuses = {p["status"] for p in floor["problems"]}
    assert "no_draw" in statuses
    assert "abandoned" in statuses
    assert [i["kind"] for i in floor["infrastructure"]] == ["unreachable_device"]
    assert floor["infrastructure"][0]["affects"]
    # Every problem carries a duration; a null one renders as a blank age.
    assert all(p["since"] for p in floor["problems"])


async def test_login_is_what_reveals_the_operator_only_keys(fixture_server):
    async with JuiceClient(fixture_server) as client:
        anon = await client.machines()
        assert not client.authenticated
        assert "plug_id" not in anon[0]
        assert "strip" not in anon[0]

        me = await client.login()
        assert client.authenticated
        assert "control_power" in me["capabilities"]

        operator = await client.machines()
        assert operator[0]["plug_id"] is not None
        assert operator[0]["strip"]


async def test_the_stream_delivers_hello_then_dense_reading_ticks(fixture_server):
    frames = []
    async with JuiceClient(fixture_server) as client:
        await client.login()

        async def collect():
            async for frame in client.stream():
                frames.append(frame)
                if len([f for f in frames if f.type == "reading_tick"]) >= 3:
                    return

        await asyncio.wait_for(collect(), timeout=30)

    events = [f for f in frames if f.kind == "event"]
    assert events[0].type == "hello"
    assert events[0].data["epoch"]
    assert [f.seq for f in events] == list(range(1, len(events) + 1))
    assert not [f for f in frames if f.kind == "resync"]

    tick = next(f for f in events if f.type == "reading_tick")
    assert tick.data["machines"]
    machine = tick.data["machines"][0]
    assert set(machine) == {
        "plug_id",
        "asset_id",
        "status",
        "activity",
        "activity_unknown_because",
        "status_since",
        "relay",
        "draw_watts",
    }
    # The raw text is the wire, not a re-serialization of the parsed dict.
    assert json.loads(tick.raw)["seq"] == tick.seq


async def test_an_unreachable_server_is_an_apierror_not_a_transport_exception():
    """A resync fires exactly when the server may have gone; the caller of
    `floor()` there is the stream worker, and an escaping ClientConnectorError
    kills it silently."""
    async with JuiceClient("http://127.0.0.1:1") as client:
        with pytest.raises(ApiError) as excinfo:
            await client.floor()
    assert excinfo.value.status == 0
    assert excinfo.value.code == "unreachable"


async def test_login_against_an_unreachable_server_raises_the_same_way():
    async with JuiceClient("http://127.0.0.1:1") as client:
        with pytest.raises(ApiError) as excinfo:
            await client.login()
    assert excinfo.value.code == "unreachable"


# --------------------------------------------------------------------------
# Bringing a session in from a browser
# --------------------------------------------------------------------------


def test_cookie_specs_parse_as_name_equals_value():
    assert parse_cookies(["AIOHTTP_SESSION=abc123"]) == {"AIOHTTP_SESSION": "abc123"}
    assert parse_cookies(["a=1", "b=2"]) == {"a": "1", "b": "2"}
    # A whole Cookie header pasted in one go.
    assert parse_cookies(["a=1; b=2"]) == {"a": "1", "b": "2"}


def test_a_bare_value_is_taken_as_the_session_cookie():
    assert parse_cookies(["abc123"]) == {SESSION_COOKIE: "abc123"}


def test_a_pasted_fernet_token_is_not_mistaken_for_a_name():
    """Session values are base64 with `=` padding; splitting on the first `=`
    would turn the token itself into a cookie name."""
    token = "gAAAAABo" + "x" * 200 + "=="
    assert parse_cookies([token]) == {SESSION_COOKIE: token}


def test_an_empty_or_blank_spec_is_rejected_rather_than_silently_ignored():
    with pytest.raises(ValueError):
        parse_cookies(["   "])
    with pytest.raises(ValueError):
        parse_cookies(["name="])


def test_an_external_login_landing_is_recognised_as_oauth():
    """A same-origin landing means the dev shim; another host means a provider."""
    assert looks_external("https://juice.theflip.museum", "https://flipfix.theflip.museum/login/")
    assert not looks_external("http://127.0.0.1:8150", "http://127.0.0.1:8150/")
    # A relative or unparseable landing must not be called external.
    assert not looks_external("http://127.0.0.1:8150", "/")


async def test_a_session_cookie_lifted_from_another_client_grants_the_operator_view(
    fixture_server,
):
    async with JuiceClient(fixture_server) as browser:
        await browser.login()
        assert browser.authenticated
        cookies = browser.session_cookies()
    assert SESSION_COOKIE in cookies

    async with JuiceClient(fixture_server, cookies=cookies) as pasted:
        me = await pasted.me()
        assert me["authenticated"]
        assert pasted.authenticated
        assert (await pasted.machines())[0]["plug_id"] is not None


async def test_login_against_a_dev_auth_server_does_not_report_oauth(fixture_server):
    async with JuiceClient(fixture_server) as client:
        await client.login()  # must not raise oauth_required
        assert client.authenticated


# --------------------------------------------------------------------------
# The reconnect loop
# --------------------------------------------------------------------------


class _StubStream:
    """A local SSE endpoint whose behaviour a test can dictate per connection."""

    def __init__(self, handler):
        self.handler = handler
        self.connections = 0

    async def _serve(self, request):
        self.connections += 1
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        await self.handler(response, self.connections)
        return response

    async def __aenter__(self):
        app = web.Application()
        app.router.add_get("/api/v2/stream", self._serve)
        self._runner = web.AppRunner(app)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        self.url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
        return self

    async def __aexit__(self, *_exc):
        await self._runner.cleanup()


async def test_an_http_error_on_the_stream_is_retried_not_raised():
    """A proxy's 502 mid-deploy must go through the same backoff as a dropped
    socket. Letting ApiError escape here kills the consuming worker instead."""

    async def handler(_request):
        raise web.HTTPBadGateway

    app = web.Application()
    app.router.add_get("/api/v2/stream", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    url = f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"

    frames = []
    try:
        async with JuiceClient(url) as client:

            async def collect():
                async for frame in client.stream():
                    frames.append(frame)
                    if len([f for f in frames if f.kind == "disconnected"]) >= 2:
                        return

            await asyncio.wait_for(collect(), timeout=20)
    finally:
        await runner.cleanup()

    # It kept going rather than raising out of the generator.
    assert [f.kind for f in frames].count("disconnected") == 2
    assert any(f.kind == "resync" and f.reason == "reconnect" for f in frames)
    assert all("502" in f.detail or f.kind != "disconnected" for f in frames if f.detail)


async def test_backoff_grows_when_a_server_accepts_then_immediately_drops():
    """juice sends `hello` the moment the stream opens, so resetting the
    backoff on the first frame would reconnect twice a second forever."""

    async def handler(response, n):
        await response.write(b'data: {"seq":1,"type":"hello","epoch":"e"}\n\n')
        await response.write_eof()

    waits = []
    async with _StubStream(handler) as server, JuiceClient(server.url) as client:

        async def collect():
            async for frame in client.stream():
                if frame.kind == "resync" and frame.reason == "reconnect":
                    waits.append(frame.detail)
                    if len(waits) >= 3:
                        return

        await asyncio.wait_for(collect(), timeout=20)

    seconds = [float(w.split("in ")[1].rstrip("s")) for w in waits]
    assert seconds == sorted(seconds)
    assert seconds[-1] > seconds[0], f"backoff never grew: {seconds}"


async def test_a_long_lived_connection_resets_the_backoff():
    from juice.tui.client import BACKOFF_RESET_AFTER

    assert BACKOFF_RESET_AFTER > 0  # a connection must last to count as healthy


# --------------------------------------------------------------------------
# Parser edge cases the wire can produce
# --------------------------------------------------------------------------


def test_a_codepoint_split_across_chunks_is_not_corrupted():
    """Decoding each chunk alone turns one character into two U+FFFD, and the
    frame then fails to parse as JSON."""
    blocks = _fed([b'data: {"n":"Caf\xc3', b'\xa9"}\n\n'])
    assert [b.data for b in blocks] == ['{"n":"Café"}']
    assert json.loads(blocks[0].data)["n"] == "Café"


def test_a_crlf_split_across_chunks_does_not_invent_a_frame_boundary():
    blocks = _fed([b"data: one\r", b"\ndata: two\r\n\r\n"])
    assert [b.data for b in blocks] == ["one\ntwo"]


def test_an_unterminated_frame_does_not_grow_without_limit():
    from juice.tui.client import MAX_FRAME_BYTES

    parser = SseParser()
    with pytest.raises(ApiError) as excinfo:
        for _ in range((MAX_FRAME_BYTES // 10_000) + 2):
            parser.feed(b"x" * 10_000)
    assert excinfo.value.code == "stream_overflow"
