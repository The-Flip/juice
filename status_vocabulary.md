# Juice — Status Vocabulary

A proposal resolving `domain_model.md` §7.1–7.3: the overlapping "on-ness" concepts
that have produced recurring bugs. **This is a naming and modelling decision, not a
UI design.** It should be settled before any of the new interface is drawn, because
every screen depends on it.

Status: **accepted** — the open naming decisions were settled on 2026-08-30; see §8.

---

## 1. The problem, concretely

Juice has at least five ways to say "on": `reading.is_on` (relay), `watts >= OFF_WATTS`
(drawing), `power_status`, `State.OFF`, and `offline`. They overlap but are not
interchangeable, and the code flattens them into a display token **at each render site,
independently**. There are six such sites (`tiles.js` ×2, `detail.js` ×2, `strip.js` ×2),
and they disagree.

Three symptoms, all real, all in `main` today:

**a. The same nested ternary, rewritten per site.**
```js
// tiles.js:72
const st = offline ? 'OFFLINE'
  : noDraw ? 'NO_DRAW'
  : (m.power_status === 'off' ? 'OFF' : (m.state || 'null'));
```
It mixes a boolean facet (`offline`), a string enum (`power_status`), and a different
string enum (`m.state`) into one CSS class token — with a literal `'null'` fallback that
renders as an unexplained gray dashed dot.

**b. "Relay is on" is rendered as `PLAYING`.**
```js
// tiles.js:43  — the no-emeter path
const dotState = offline ? 'OFFLINE' : (isOn ? 'PLAYING' : 'OFF');
```
A claw machine with an energized relay gets the same green dot as a pinball machine
mid-game. We cannot measure that outlet at all, so we know strictly less about it, and
we render the strongest possible claim.

**c. The colour and the label disagree, by construction.**
```js
// detail.js:22-25
const badgeState = ... (noEmeter ? (relayOn ? 'PLAYING' : 'OFF') : (m.state || 'OFF'));
const badgeLabel = ... (noEmeter ? (relayOn ? 'ON'      : 'OFF') : (m.state || 'OFF'));
```
Two parallel cascades over the same inputs, deliberately diverging — the dot says
PLAYING while the text says ON. That divergence is the tell: these are **two different
concepts** being forced through one channel.

The root cause isn't sloppiness. It's that the domain is a **cascade of four
independent questions**, and the code keeps trying to flatten it into one enum.

---

## 2. The proposal: four axes, one derived status

### The four axes (independent inputs)

Each answers a different question, each can independently be unknown, and **none of them
is called "on"** except the relay.

| Axis | Type | Question | Replaces |
|---|---|---|---|
| `reachable` | `bool` | Did we hear from the device recently? | `offline` (inverted) |
| `relay` | `'on' \| 'off'` | Is the outlet energized? A hardware fact. | `is_on` |
| `draw` | `float \| null` | How many watts? `null` = unmeasurable | `watts` + `has_emeter` |
| `activity` | `Activity \| null` | What is the machine *doing*? | `State` |

`activity` is `null` whenever we can't say — and **the reason is carried alongside**:

```
activity_unknown_because: 'not_drawing' | 'uncalibrated' | 'unmetered' | 'unreachable'
```

That field is what lets the UI say *"uncalibrated — play time not measurable"* instead of
a confident gray box (user_needs §5.5).

### The derived status (the single output)

One value, derived **once**, server-side, that every surface renders. Nothing re-derives
it; nothing renders a raw axis.

| Status | Derived when | Label | Colour (existing) |
|---|---|---|---|
| `unreachable` | `not reachable` | Unreachable | `#c7c7cc` light gray |
| `off` | relay off | Off | `#1d1d1f` near-black |
| `no_draw` | relay on, metered, `draw < 2W` | No draw | `#ff9500` orange |
| `powered` | relay on, drawing or unmeasurable, activity unknown | Powered | `#007aff` blue |
| `attract` | drawing, classified ATTRACT | Attract | `#007aff` blue |
| `playing` | drawing, classified PLAYING | In play | `#34c759` green |
| `abandoned` | drawing, classified IDLE | Abandoned | `#f5c41a` yellow |

Every existing colour keeps its meaning. **`state-null` disappears entirely** — there is
no longer any way to reach an unexplained gray dash, because every state has a name and
every unknown has a reason.

### Two properties worth noticing

**The problem list falls out of the enum.** The new interface will open with a
**Problems section at the top** — machines that may need operator intervention — and its
contents are exactly a filter on status: `no_draw` + `abandoned`. No separate query, no
second cascade, no hand-maintained list of what counts as a problem.

`unreachable` is a judgement call for that section: it means a device we can't talk to,
which is a real problem but a different kind (infrastructure, not machine). Suggest
grouping it separately within the section rather than mixing it in with machines that
are physically present and misbehaving.

Note this is a **future-interface** commitment. The current UI keeps its existing
treatment of `no_draw` (a coloured dot plus the `outlet on · no draw` note) — see §8.4.

**Unassigned outlets fit the same model.** An outlet has no machine, so its `activity` is
always `null`, but `unreachable` / `off` / `no_draw` / `powered` describe it perfectly. Today
outlets have their own rendering path; under this model they don't need one.

---

## 3. Renames

**`State` → `Activity`.** "State" is the single most overloaded word in the codebase
(`RecorderState`, machine state, power state, operation state). `Activity` says what it
is: what the machine is *doing*.

**`Activity.IDLE` → `Activity.ABANDONED`.** This one is a genuine trap. `IDLE`
colloquially means "not in use" — which is what `ATTRACT` actually is. The current `IDLE`
means "a game is in progress and the player walked away", i.e. **the opposite of idle**,
and it's the signal behind J4. Anyone reading `IDLE` in this codebase without the
docstring will get it backwards.

**`Activity.OFF` is removed.** Off is not an activity; it's the absence of one. That case
becomes `activity = null, because = 'not_drawing'`.

**`Activity.OFFLINE` is removed** (it was never a classifier output — the server injected
it at the presentation layer). Reachability is its own axis.

So `Activity` has exactly three members: `ATTRACT`, `PLAYING`, `ABANDONED`. All three
mean the machine is drawing power and we know what it's doing. Clean.

---

## 4. Naming rules

1. **"On" and `is_on` mean the relay. Nothing else, ever.**
2. **"Drawing" is watts-derived.** (Already the established rule for this codebase.)
3. **"Activity" is the classification.** Never "state".
4. **"Status" is the single derived display value.** Never "power status", never "state".
5. **Never render a raw axis.** A UI surface reads `status` and `activity_unknown_because`.
   If a surface needs to re-derive, the derivation is missing from the server.

---

## 5. What this fixes

| `domain_model.md` §7 | How |
|---|---|
| 7.1 Overlapping "on-ness" | Four named axes; exactly one derived status; one derivation site |
| 7.2 `OFFLINE` in the `State` enum | Removed — reachability is its own axis |
| 7.3 Uncalibrated reads as failure | `powered` is a first-class status with a stated reason, not a gray fallback |

It also preserves the fix from #74 (uncalibrated machines read blue, not gray) while
making it *honest*: `powered` and `attract` share a colour but carry different labels, so
we stop asserting "attract" about a machine we can't classify. Both read as the good,
normal case to an operator — which is correct, because they are.

---

## 6. Code changes

- **`juice/state.py`** — `State` → `Activity`; drop `OFF` and `OFFLINE`; rename
  `IDLE` → `ABANDONED`; `classify()` returns `Activity | None` per sample.
- **`juice/server.py`** — `_power_status` → `machine_status`, returning the 7-value enum
  plus `activity_unknown_because`. This becomes **the only** place the cascade exists.
- **`juice/web/*.js`** — delete all six ad-hoc cascades; render `status` directly.
  `tiles.js`, `detail.js`, and `strip.js` each lose a nested ternary.
- **CSS** — rename `.state-*` → `.status-*`, drop `.state-null`, add `.status-powered`.
  `.state-NO_DRAW` keeps its colour and becomes `.status-no_draw`.
- **`hourly_play_seconds`** is unaffected: it keys on PLAYING, which keeps its name and
  meaning.

`UNCALIBRATED_CALIBRATION` can stay exactly as it is — with `play_min_rsd = inf` the
classifier returns no PLAYING, and the server maps "drawing but unclassifiable" to
`powered` rather than forcing it through ATTRACT.

## 7. Migration

The old UI is being kept for now, so `/api/machines` has two consumers during the
transition. Suggested approach: **add** `status` and `activity` alongside the existing
`power_status` / `state` / `is_on` fields, mark the old ones deprecated in the handler
docstring, and delete them when the old UI goes. The new fields are pure derivations of
data already in the payload, so this costs a few bytes per machine and no new queries.

The rename inside `juice/state.py` is not wire-visible and can land first, on its own.

---

## 8. Decisions

Settled 2026-08-30. Recorded with rationale so they don't get relitigated.

1. **`no_draw`, not `dark`.** The audience for this interface is comfortable with the
   jargon, and precision beats evocativeness. It also happens to be the term already used
   by `power_status`, so this half of the vocabulary needs no migration at all.

2. **`powered` and `attract` stay separate** — same colour, different labels. The
   deciding argument is that this is not a rare edge case: the machines we can't classify
   (Lightning, Centipede, Tempest) are a standing part of the collection, not a
   calibration backlog to be worked off. From the operator's point of view `powered` and
   `attract` are *both* the good, normal case, which the shared colour says correctly;
   the labels then avoid claiming knowledge we don't have.

3. **`unreachable`, not `unknown`.** Same reasoning as (1) — a savvy audience is better
   served by naming the actual condition than by hedging about our knowledge of it.

4. **The current interface is left alone.** `no_draw` keeps its existing dot-plus-note
   treatment there. Making it louder is a **new-interface** job, and the answer there is
   the Problems section at the top of the floor view (§2), carrying both `no_draw` and
   `abandoned` machines.

### Still open

- **`IDLE` → `ABANDONED`** (§3) was not among the decisions above and hasn't been
  explicitly confirmed. Keeping it as proposed; easy to drop if the old name is preferred.
- **The migration approach in §7** (add new fields alongside the old, delete when the old
  UI goes) is a suggestion, not a decision.
