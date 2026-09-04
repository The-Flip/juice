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
| Buffer | ~36 bytes/row, ~4.5 GB for 30 days at 48 metered outlets |

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

## Deployment

`Dockerfile.tap` and `compose.tap.yml` in the repo root. Read the comment at the
top of the compose file before choosing a network mode: **LAN discovery is a UDP
broadcast and does not cross Docker's default bridge**. Host networking makes
discovery work; bridge networking works everywhere but needs every device pinned
in `tap.toml`. Both are supported.

Flash wear is worth a thought: ~150 MB/day of sustained small writes will destroy
a microSD card in months. Use an SSD.

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
