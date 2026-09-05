# tap

A standalone collector that polls Kasa/Tapo smart plugs **over the LAN**,
buffers every reading to disk, and streams them to a server over a WebSocket.

`tap` imports nothing from `juice`. It knows about hosts, devices, outlets,
watts, volts and amps. It does not know what a machine, an asset tag or a
pinball table is — an outlet alias is an opaque string it copies verbatim.
`tests/tap/test_isolation.py` enforces that.

## Why it exists

juice reads power through the TP-Link **cloud**, which has three problems: it
needs the WAN for data that never leaves the building; it cannot read newer
SMART/KLAP hardware at all (the P316M, EP25, KP125M); and its poll loop walks
devices sequentially over a session with no timeout, so one hung device stalls
every other device for minutes.

## Running it

```bash
uv sync --extra tap
cp tap.toml.example tap.toml          # edit; or run with no config at all
uv run tap run --buffer-dir ./data/buffer
```

Then open <http://127.0.0.1:8010/>. With no `[uplink].url` configured it runs
standalone: polls, buffers, and shows you what it has. That is a complete,
useful mode — the uplink is additive.

Other commands:

```bash
uv run tap probe 192.168.4.38     # one sweep, with timings
uv run tap devices                # LAN discovery + the resulting roster
uv run tap bench                  # how much headroom the buffer has on this disk
uv run tap relay 192.168.4.38:<child_id> --off   # the server-is-down escape hatch
uv run tap status                 # fetch a running tap's status as JSON
```

Credentials come from `KASA_USERNAME` / `KASA_PASSWORD`. The same TP-Link
account works for both device families on the LAN; no separate Tapo login.

## The measurements this is built on

Taken against a real P316M. They are why the code looks the way it does:

| | |
|---|---|
| `Device.update()` (python-kasa's normal path) | **812 ms** — never used |
| `get_child_device_list` | 76 ms — relay state and aliases for all outlets |
| `control_child` → `get_emeter_data` | **14 ms** per outlet |
| `control_child` → `get_energy_usage` | 67 ms — cumulative counters we don't need |
| A six-outlet sweep | ~160–350 ms depending on the network |
| Buffer | ~30 bytes/row, ~3.7 GB for 30 days at 48 metered outlets |

The buffer figure is measured, not estimated, and it was wrong here until it
was. An eight-hour run against the P316M above wrote 110 bytes/row — 456 MB/day
and 13.7 GB over a 30-day window at 48 outlets, three times what this table
claimed. The row layout was right about varint-encoded integers and wrong about
everything else: `(device_id, child_id)` is 82 characters of hex, and it was
written on every row. Interning plug identity per day file (the `plugs` table)
brought it to 29.6 bytes/row measured the same way.

That same run is the source of the cadence numbers this design is really for:
27,116 sweeps at a p50 interval of **1001 ms** (p95 1002, p99 1003), 99.2%
coverage, against the cloud recorder's 6–9 s.

Two hardware facts shape the design:

- **The firmware rejects `control_child` inside `multipleRequest`** (every
  sub-request returns `error_code: -1001`), so outlets are read one at a time.
- **The energy meter refreshes about once a second.** Polling faster returns the
  identical value eight or ten times. 1 Hz is the hardware's rate, not a
  compromise.

## Design notes

**One task per device.** This is the whole answer to "one device must not slow
the others": there is no shared loop. Each device has its own task, its own
connection, and an `asyncio.timeout` budget of 0.8 s — deliberately under the
1 s interval, so a hung sweep is cancelled before its successor is due.

**Day-partitioned SQLite.** The hard part of a 30-day rolling buffer is not
writing it, it is expiring it, and neither SQLite nor DuckDB gives back disk
from a `DELETE` without rewriting the file. One file per UTC day makes retention
`os.unlink` — constant time, complete reclamation, and it still works on a file
too corrupt to open, because the decision is made from the filename. Writes go
through a single-worker thread pool, so a slow disk never stalls a poller.

**The uplink is a cursor tailing the buffer.** There is no separate backfill
path: caught up, the walk produces one small batch a second; after an outage it
produces large batches of old rows. Same code, same ordering, same acks. The
server is the authority on durability — it replies to `hello` with the cursor it
has actually stored, and tap rewinds to it.

The cursor is a **global sequence assigned at commit time**, not a timestamp and
not a per-file rowid. That matters more than it sounds: ordering by day would
lose a sweep that starts at 23:59:59.9 and commits after a faster device has
already pushed the cursor into the next day, and a single future-dated row from
a bad clock would strand everything written after it, permanently. A sequence
handed out by the one thread that writes is monotonic in insertion order, so
nothing can ever land behind the cursor whatever its timestamp says.

The durable cursor advances only over a **contiguous** run of acked batches.
With four batches in flight, acking whichever arrives last would skip the ones
still outstanding — and since the cursor is persisted, no later connection would
ever send those rows again.

`readings` and `live` are separate message types for a reason. Feeding backfill
into a server's live state would replay days of history through overload
detection at wire speed and fire shutdowns for events that ended on Tuesday.

**Small errors recover in place; major flaws exit.** A sweep timeout, an offline
device, a dropped connection, a full write queue — all recover, all counted, all
visible on the status page. An unwritable buffer, a corrupt database, a wedged
write path or a stalled event loop exits non-zero and lets the supervisor
restart a clean process, which rebuilds its state from the buffer on disk.

**Exactly two log lines per outage**, one entering and one leaving. At 1 Hz
across a dozen devices, a per-tick log line is a million lines a day.

**A failure below the offline threshold is reported when the device recovers**,
rate-limited to one line a minute with the suppressed count riding along. That
gap was found the same way as the buffer sizing: eight hours of real polling
dropped 132 sweeps and logged exactly one of them, because the other 131 never
crossed the threshold and the DEBUG line for them is invisible at the default
level. Reporting on recovery rather than on the failure is what keeps an outage
at two lines — a device on its way offline never recovers to trigger it.

**A cancelled sweep records which round trip it was on.** The budget cancels
from outside, so the `TimeoutError` that reaches the poller carries no message
at all — `last_error` read `"TimeoutError: "` for all 132. The adapters now
leave the phase (`get_child_device_list`, `emeter[3/6]`) on the device for the
poller to attribute the failure to, because "the connect is slow" and "outlet 5
hangs" want different fixes. Failed attempts are also timed into their own
percentiles: folding them into the success latency would flatter a fleet that
fails fast, and dropping them — the old behaviour — censored the slow tail out
of the very p95 you would use to size the budget.

## Deployment

`Dockerfile.tap` and `compose.tap.yml` in the repo root. Pick a profile — there
is no default, because silently choosing a networking mode for you is how you
end up debugging why discovery finds nothing:

```bash
mkdir -p ./data/tap && sudo chown -R 10001:10001 ./data/tap   # tap runs as UID 10001
docker compose -f compose.tap.yml --profile host   up -d      # discovery works
docker compose -f compose.tap.yml --profile bridge up -d      # pinned devices only
```

**LAN discovery is a UDP broadcast and does not cross Docker's default bridge.**
Host networking makes discovery work but has no port mapping and does not exist
on Docker Desktop for macOS. Bridge networking works anywhere but needs every
device pinned in `tap.toml` — a supported configuration, not a degraded one.

Flash wear is worth a thought: ~125 MB/day of sustained small writes at 48
metered outlets will destroy a microSD card in months. Use an SSD.

## Known gaps

- **The IOT adapter is unverified against real hardware.** The HS300s live on
  the museum LAN; only a P316M was reachable while this was written. The call
  shapes are the ones juice has used in production for months and the fixture
  tests pin them, but first contact with a real strip is the real check. In
  particular, confirm that a local `get_sysinfo`'s `deviceId` matches the cloud
  `deviceId` — if it does not, local and cloud readings fork into duplicate
  plugs.
- **There is no server yet.** The uplink is implemented and tested against a
  fake, but the `/api/v2/ingest` endpoint does not exist in juice. Until it
  does, run standalone.
- `energy_wh` means different things on different families (lifetime on an
  HS300, likely a period counter on the P316M). tap ships the raw integer and
  builds nothing on it.
