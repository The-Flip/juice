# What building a client against `/api/v2` found

`api_v2.md` claims a client can be built without reading the server. This is the
report from actually trying it: `juice/tui/`, a read-only Textual TUI written
from that document alone, run against the seeded fixture server.

The headline is that the claim mostly holds. The read model, the status
vocabulary and the stream's sequencing were all buildable from the document, and
the parts it warns about (§1's five things) are exactly the parts that would
otherwise have been got wrong. What follows is the friction.

Scope: reads and the stream only. Writes, bulk operations and metrics were not
exercised, so nothing here is evidence about them.

---

## 1. An anonymous client cannot use the stream at all — FIXED

**The blocker.** `reading_tick` identifies machines by `plug_id` and nothing
else (§5):

```json
{"plug_id": 3, "status": "playing", "activity": "playing", …}
```

But `plug_id` is one of the operator-only keys stripped from the machine view
for anonymous callers (§8). So an anonymous client gets a machine list with no
`plug_id`, then a 1 Hz stream keyed by `plug_id`, and has no way to join them.
Verified against the fixture: logged out, 31 rows and 0 joinable ticks; logged
in, 31 of 31 join.

This is a real hole rather than a nit, because §8 says the public dashboard is
deliberate and long-standing, and §5 says the stream "replaces polling entirely —
do not add a refresh timer". Together those tell an anonymous client to build a
live view it cannot actually keep live. Its only options are to poll `/floor`
against the document's explicit instruction, or to show a frozen page.

Options, in preference order:

1. Add `asset_id` to each `reading_tick` entry. It is already the public
   identity everywhere else in the API, it is not sensitive (the floor view
   hands anonymous callers `asset_id` and `name` in `problems` and `groups`
   today), and it makes the tick self-describing for every audience.
2. Keep `plug_id` for operators and add `asset_id` alongside it.
3. If neither, say plainly in §5 and §8 that the stream is operator-only in
   practice, so nobody builds the public live view twice.

**Fixed in this branch (option 1).** `reading_tick` entries now carry `asset_id`
alongside `plug_id`, added to `_readings_snapshot` so there is still one
derivation, and surfaced in the v2 projection. The TUI joins on `asset_id` and
falls back to `plug_id`; its anonymous banner is replaced by a count of ticks it
could not attribute, which is zero on a current server and non-zero against one
that predates the fix.

Tick entries now also pass through the same `redact` as every other v2 payload,
so `plug_id` leaves an anonymous tick rather than being the one operator-only
key that §8's boundary let through.

Joining on `asset_id` exposed a second problem that `plug_id` had been hiding.
`_readings_snapshot` emits one entry **per plug**, and a machine that has moved
outlets has two open assignments — the case `juice/identity.py` exists for, and
which `/floor` and `/machines` both dedupe through `resolve_asset`. Keyed by
`plug_id` those two entries were merely redundant; keyed by `asset_id` they are
two rows claiming one machine, and the stale one on the dead outlet wins the
client's merge — nulling a live, drawing machine, and undetectably so for an
anonymous client that cannot see `plug_id` to tell them apart. The snapshot now
carries `asset_id` only on the resolved-live plug, and the v2 projection drops
the rest; v1 still receives both entries, because it keys tiles by `plug_id` and
renders one per outlet.

## 2. `unreachable` machines still report `relay` and `draw_watts` — FIXED

§3 defines `unreachable` as "device not answering — **we know nothing
current**". The payload disagrees:

```json
{"asset_id": "M0007", "status": "unreachable", "relay": "on", "draw_watts": 127.4}
```

Those are last-known values from before the device went quiet, presented in the
same fields as live ones and with nothing marking them stale. A client that
renders a watts column — as any client will — shows six machines on a dead strip
drawing 127 W, 116 W, 97 W. `status_since` is the only hint, and only if you
notice it is eight minutes old.

The document already has the right instinct elsewhere: it insists `draw_watts:
null` means unmeasurable and "`null` is not zero". The same reasoning applies
here. Either null both fields when `status` is `unreachable`, or document that
they are last-known-good and say since when.

This one is quiet and dangerous: it is not an error, it just looks like data.

**Fixed in this branch.** `views.blank_when_unreachable` nulls both fields when
the status is `unreachable`, applied to machines, outlets and reading ticks —
one rule, next to the redaction boundary. `juice.status` is untouched: the
cascade is pure and shared with v1, and the relay really was "on" when we last
looked; what changed is what the v2 wire is willing to assert. A knock-on worth
having: an unreachable strip now reports `draw_watts: null` and counts every
outlet in `unmeasured_outlets` instead of totalling stale readings.
`api_v2.md` §3 documents `relay` as nullable accordingly.

## 3. `reading_tick` is not 1 Hz, and a client will act on that number

§5 says reading ticks arrive "roughly 1 Hz". Measured against production, twice,
about 45 seconds each:

```
gaps: 7.2  7.1  6.3  6.2  5.8  18.0
n=6  min=5.8s  median=6.7s  max=18.0s
```

The cause is in `record()`: the loop polls, publishes one tick, then sleeps
`max(0, 1.0 - elapsed)` (`juice/recorder.py:969`). The 1.0s is a floor that never
binds, because `poll_once` walks devices **sequentially** — one WAN round trip to
`wap.tplinkcloud.com` per strip (`recorder.py:485-492`). The observed cadence is
simply how long a full poll takes. The 18s outlier is the same loop doing its
60-poll housekeeping inline: the FlipFix fetch, `refresh_metadata`, and four
rollup refreshes (`recorder.py:941-965`).

**It gets worse when the museum is open.** Those measurements were taken with
every machine off, and an off outlet skips the emeter read entirely
(`recorder.py:502`). Each *on* metered outlet adds another sequential
`read_emeter` call (`recorder.py:594`), so a full floor is tens of extra round
trips per cycle.

Why this is a client-facing finding and not just a performance note: a client
told "roughly 1 Hz" will reasonably treat six seconds of silence as a stalled
connection and show a stale indicator, or animate on an assumed cadence. This
TUI avoids that only because it keys staleness off `seq` gaps rather than
elapsed time — which was luck, not judgement.

§5 should say the cadence *is* the recorder's poll period, that it scales with
device count and with how many outlets are drawing, and that a client must infer
nothing from inter-tick timing. The 15s `: ping` heartbeat is the liveness
signal, and the document already provides it — it just doesn't say it is the
only one.

The fix on the server side is to poll devices concurrently
(`asyncio.gather` over `devices` in `poll_once`) rather than in series, which
would plausibly get production close to the documented 1 Hz. That is recorder
surgery, not a client change.

**The fixture does not reproduce any of this.** `tests/e2e/serve.py`'s
`_readings_ticker` is a bare `asyncio.sleep(1.0)` with no cloud, so it is exactly
1 Hz by construction — it matches the documented cadence rather than the real
one. Anything tuned against the fixture (a staleness indicator, a "live" dot, an
animation) will behave differently in the museum.

## 4. What a client should do when the `activity` invariant is violated

§3 lists `activity_unknown_because` as `not_drawing | uncalibrated | unmetered | no_measurement |
unreachable | null`. That matched the fixture, and pairing it with a null
`activity` to render *why* instead of a blank cell worked well — it is a good
design and the TUI leans on it. But the invariant in §3 ("a non-null `activity`
always implies `status` is one of attract/playing/abandoned") is the kind of
thing a client will assert on, and the document does not say what to do when it
is violated. Saying "treat a contradicting payload as a bug and prefer `status`"
would be one sentence and would settle it.

## 5. There is no v2 way to ask "am I logged in?"

The redaction model in §8 is good and worked exactly as described. But a client
needs to know which audience it is in — to label the UI, to decide whether the
stream is usable (see finding 1 above), and to offer a login. `/api/v2` has no endpoint
for that. The only option is v1's `/api/me` — and the document's opening says
v1 is frozen and not to be built against.

So the very first thing this client does is call a v1 endpoint. Either add
`GET /api/v2/me`, or state in §8 that `/api/me` is exempt from the v1 freeze.

The alternative — inferring the audience from whether `plug_id` came back — is
what a client will otherwise do, and it is exactly the kind of implicit coupling
the redaction section is trying to avoid.

## 6. Session auth has no non-browser path, and §8 doesn't say so

§8 says "sessions are 30-day cookies" and leaves it there. What that means in
practice, established by reading `juice/auth.py` rather than the document:

- Against **dev-auth**, `GET /login` mints an operator session and any cookie
  jar picks it up. Fine.
- Against **real OAuth**, `/login` redirects into FlipFix with PKCE. There is no
  token, no client-credentials path, nothing a script or a TUI can drive. A
  non-browser client cannot authenticate against production at all.

That is a legitimate design decision, but it belongs in §8 as one sentence,
because it decides whether a non-browser client is even possible. `/api/backup`
shows the pattern that would work if one is ever wanted: a bearer token, checked
before the OAuth gate.

The workaround the TUI ships with is `--cookie`: paste the `AIOHTTP_SESSION`
cookie from a logged-in browser. It works — verified against the fixture — but
it hands a script a full 30-day operator session in an argv string, which is a
worse credential story than a scoped token would be.

**Practical trap, for whoever writes the next Python client:** aiohttp's default
cookie jar silently discards cookies set by an IP-address host. Pointed at
`http://127.0.0.1:8150`, the client logs in, receives the session cookie, drops
it, and stays anonymous with no error anywhere — it just looks like login did
nothing. `aiohttp.CookieJar(unsafe=True)` fixes it. Every juice dev server is on
an IP, so every Python client will hit this.

## 7. Small things

- **`counts.powered` includes `abandoned`.** Defensible — the machine is drawing
  — but a floor summary reading "23 powered · 3 problems" counts the abandoned
  machine in both. Worth one clarifying line in §4.
- **`problems[].since` duplicates `status_since`** under a different name. Not
  wrong, but a client holding both has to remember they are the same field.
- **`groups` for anonymous callers is `[{"device_id": null, "name": null, …}]`.**
  §4 documents this and the reasoning is sound, but a client must handle a null
  group name in the same code path that renders a strip label. Worth a word in
  §4's example payload, which only shows the named form.
- **The `: ping` heartbeat is invisible to `EventSource` but not to everyone.**
  §5 mentions this from the browser's point of view. A hand-rolled SSE parser
  has to skip comment lines explicitly, and one that feeds every line to
  `JSON.parse`/`json.loads` will throw every 15 seconds while idle.
- **Nothing in the document says frames can split mid-JSON.** Obvious to anyone
  who has written an SSE parser, but §5's "the client contract" snippet is
  written as if whole messages arrive, and it is the only implementation guidance
  given. One line — "buffer until a blank line; a frame can arrive in pieces" —
  would stop the next client dropping frames and mistaking it for a server gap.

---

## What the document got right, and should keep

Worth recording, because these are the parts that would have caused bugs and
didn't:

- **`seq` being dense per connection** is the single most useful thing in the
  API. Gap detection is four lines and it removes polling entirely.
- **`epoch` on `hello` only**, with the explicit warning about checking it on
  every frame. Without that warning the natural implementation is a check on
  every message, which resyncs forever; the warning is the only reason this
  client does not do that.
- **`status` derived server-side, exactly once.** Every client that re-derives
  "is it on" from watts gets it wrong differently; this makes it impossible.
- **`activity_unknown_because`** turns blank cells into explanations for free.
- **`problems` as a filter on the same data as the tiles** means the summary and
  the list cannot disagree. The fixture's `--with-problems` mode proving it is
  the right call: the panel was testable on the first run.
- **The five things in §1** are all real, all non-obvious, and all cost nothing
  to obey once stated.

## Ranked

| # | finding | severity |
|---|---|---|
| 1 | anonymous stream cannot be joined to machines | ~~blocker~~ **fixed** |
| 2 | `unreachable` reports stale relay/watts as live | ~~wrong data~~ **fixed** |
| 3 | ticks are ~6-9s, not the documented 1 Hz | **doc is wrong; clients will act on it** |
| 5 | no v2 "who am I" — forces a v1 call | should fix |
| 6 | §8 silent on non-browser auth | doc gap, decides feasibility |
| 4 | `activity` invariant violation behaviour unspecified | doc gap |
| 7 | assorted | polish |
