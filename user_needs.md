# Juice — User Needs

Working reference for the new web interface. What people actually come to juice to do,
ranked by how often they do it — which is the ordering the interface should reflect and
the current one does not.

**Evidence basis.** Items marked ✅ are grounded in the repo (README feature list,
`todo.md`, CLAUDE.md operations runbooks, existing endpoints, and the shape of the
data model) or confirmed directly by the operator. Items marked ❓ are still inferred and
**should be confirmed before they drive design**.

---

## 1. Who uses it

### A. Floor operator / opening–closing volunteer ✅
The highest-volume user. Opens and closes the museum, walks the floor, deals with a
machine that's misbehaving in front of a guest. Checks regularly for abandoned games
and reboots the machine if it's truly not being used. Often on a **phone**, standing up,
possibly with one hand full. Not technical. May be a different person every day.

Needs: bulk power, at-a-glance floor status, one-tap reboot. Nothing else.

### B. Technician / maintainer ✅
Fixes machines, moves them between outlets, responds to overload shutdowns, keeps the
plug↔machine mapping honest. Comfortable with the Kasa app and a terminal, but
shouldn't need either.

Needs: per-machine history, power event audit, mapping health, locks, calibration.

### C. Manager / owner ✅
Wants to know what the collection costs to run, which machines earn their floor space,
and when the museum is actually busy. Reads on a laptop, occasionally, in depth.

Needs: cost, utilization, trends over weeks and months.

### D. Public visitor ✅
`/`, `/usage`, and `/air` are public-readable with operational detail redacted.

This audience is **rarely actually used**. Its purpose is to show off that the system
exists. That makes the design brief easy: the public view is legitimately **the operator
view with actions and sensitive information removed** — no separate information
architecture required, and no effort should be spent inventing one.

### E. Juice itself (autonomous) ✅
Overload protection acts without a human: it shuts a machine down, files a FlipFix
report, marks the machine broken. The interface's job is to make those actions
**visible after the fact**, since nobody was watching when it happened.

---

## 2. Form factors and concurrency ✅

**Devices, in order of importance:**
1. **Phones** — operators and technicians walking the floor. The primary target.
2. **A tablet at the front desk** — semi-permanent, likely left on a page all day.
   Effectively a glanceable status display that also happens to be interactive.
3. **Laptops** — occasional, mostly for the Tier 3 analytical jobs.

Design mobile-first, with the tablet as a "readable from three feet away" case. Desktop
is the least important surface, which is roughly the inverse of how the current UI is
built.

**Concurrency:** expect **2–3 simultaneous operators** — a front desk person plus one or
two technicians. They frequently converge on the same problem (the canonical example: a
machine smelling of smoke). When that happens they need to be **aware of each other**:
who is already on it, what has been tried, and whether someone else has already cut the
power.

This is a hard constraint, not a nicety. The current model fights it: `current_operation`
is a single global slot that 409s a second operator with no explanation of who holds it.

---

## 3. Jobs to be done, by frequency

### Tier 1 — daily, time-pressured

**J1. Open the museum ✅**
> "Turn everything on, and tell me when it's actually up."

- One action. Then a **progress view** — machines come up staggered (inrush limiting),
  so this takes real wall-clock time and the operator needs to see it advancing.
- Must surface, at the end: what failed, what was skipped and *why* (locked-off,
  offline, unsupported plug). "3 machines didn't come up" is the answer people need,
  and it must not be buried.
- Must be cancellable mid-flight.
- Must show **who started it** if it wasn't you — a second operator hitting All On needs
  to see "Dana started this 40 seconds ago", not a bare 409.
- Currently exists (`/api/operations/all-on` + SSE progress) but is a button on a
  dense dashboard rather than the primary affordance.

**J2. Close the museum ✅**
> "Turn everything off — except what shouldn't be."

- Same shape as J1, plus the domain rules: skip locked-on, and **never interrupt a
  machine that's currently `PLAYING`**. The interface should *say* it skipped a machine
  because someone was still playing it, so the operator can come back to it.
- Needs a clear "did everything actually go off?" confirmation. A machine left on
  overnight is real money and real wear.

**J3. Read the floor at a glance ✅**
> "Is everything OK right now?"

- The default view. Should answer, without interaction: how many machines are on, how
  many are being played right now, and **what's wrong** — abandoned games, offline devices, `no_draw`
  outlets (relay on but nothing drawing = someone switched it off at the machine, or
  it's faulted), locked-off machines.
- The `no_draw` state is the most operationally useful signal juice has and is
  currently near-invisible.
- **Decided:** the new view opens with a **Problems section at the top** — machines that
  may need operator intervention. Its contents are a filter on the status vocabulary:
  `no_draw` + `abandoned`, with `unreachable` devices grouped separately within it
  (infrastructure trouble, not a machine physically present and misbehaving). Because it
  is a filter rather than a hand-maintained list, it cannot drift out of sync with what
  the tiles show. The *current* interface keeps its existing treatment — this is a
  new-interface commitment. See `status_vocabulary.md` §2 and §8.4.
- Grouping should follow physical reality (strip / circuit / floor area), because the
  operator is walking a building, not reading a list.
- This is the view that lives on the front-desk tablet all day. It has to stay correct
  and readable without anyone touching it.

**J4. Deal with abandoned games ✅**
> "What machines need restarting?"

- A very common problem is non-expert users walking away from a game in progress.
  Juice can detect things that look to be abandoned games, but it takes a human operator
  to decide if it's real. It should be easy for an operator to spot that Juice has
  noticed the issue, to check, and to reboot the machine if it's truly not being used.
- Backed by `State.IDLE` (ultra-stable draw = started but abandoned). Note this only
  works for **calibrated** machines with `idle_max_rsd` set, so the list is inherently
  partial and should say so.

**J5. Fix one misbehaving machine ✅**
> "Machine X is wedged. Reboot it."

- Find the machine **by name** (fast — the operator knows "Godzilla", not `M0021` and
  certainly not `plug_id=331`), then one-tap reboot with a confirm if it's mid-game.
- Search-by-name is the single most important navigation primitive and doesn't
  meaningfully exist today.
- A reboot takes 4–30 s over the TP-Link cloud. It **must** show pending → confirmed /
  failed, or operators will tap it repeatedly believing it did nothing.

**J6. Work the same problem as another operator ✅**
> "Someone says Godzilla smells like smoke. Has anyone dealt with it?"

- When 2–3 people converge on one machine, each needs to see what the others have
  already done — **without asking over the radio**.
- Minimum viable: on the machine view, show recent power events with actor and source
  ("Dana turned this off 2 minutes ago"), plus any in-flight bulk operation and who owns
  it. `power_events` already records all of this; nothing new needs collecting.
- Nice to have: a lightweight "I'm on it" marker or a note on the machine.
- ❓ Whether that shared marker is worth building, or whether the audit trail alone is
  enough, is a design question — but the *awareness* need is confirmed.

### Tier 2 — weekly

**J7. Recover after moving a machine ✅**
> "I moved Star Trip to a different outlet and now juice is confused."

- Documented today as a CLI runbook (`juice doctor`) plus relabelling in the Kasa app.
  Keeping the **admin action** in the CLI is fine (see §6). What belongs in the web UI is
  the **diagnosis**: untagged online outlets, stale assignments, offline devices, and
  unsupported models should surface as problems on the status view rather than requiring
  someone to remember to run a command.
- Note the deliberate constraint: **assignment is driven by the Kasa alias**, so the
  UI should *guide and verify* the relabel, not offer a competing manual mapping.

**J8. Take a machine out of service / put it back ✅**
> "This one's broken — keep it off until I say otherwise."

- `lock_mode = 'off'`, and it must be obvious on the floor view that a machine is
  locked and why, or the next volunteer will just power it on again.
- Locked-on is the rarer mirror case (something that must never be switched off).

**J9. Understand an overload shutdown ✅**
> "Juice turned Godzilla off by itself last night. What happened?"

- Needs: when, what the trailing draw looked like against the baseline and threshold,
  what FlipFix report it filed, and whether it's recurred.
- This is juice's most consequential behaviour and has essentially no dedicated
  surface. High value, low current coverage.

**J10. Audit who did what ✅**
> "Who turned this off?"

- `power_events` has actor, source, result (including `refused`) and operation grouping.
  Currently reachable only via a link buried on the dashboard.
- Overlaps heavily with J6 — the same data serves both "what happened last night" and
  "what did my colleague just do". Build it once.

### Tier 3 — monthly / occasional

**J11. What is this costing? ✅**
Evidenced directly by `todo.md`: cost-per-day chart, per-machine cost table, a normal
day and a monthly total, at $0.31/kWh. Explicitly noted as wanting **per-bill bars for
cross-comparison against the actual utility bill** once there's enough history — that
is the real underlying job: *reconcile juice against the electric bill.*

**J12. Which machines get played, and when? ✅**
Play hours per machine, the busy grid (day-of-week × hour). Feeds decisions about
what to keep on the floor and when to staff. Must distinguish "not played" from
"not available" (the `on_seconds` denominator) and from "not measurable"
(uncalibrated).

**J13. Am I going to trip a breaker? ✅**
Circuit peaks vs. breaker amps. Comes up when adding a machine or rearranging the
floor. Rare, but the cost of getting it wrong is the whole row going dark mid-day.

**J14. Environmental conditions ✅**
Temperature, humidity, CO₂, PM2.5/PM10, TVOC, noise. Room-scoped, ~15 min cadence.
Realistically a comfort/HVAC check rather than a machine concern. ❓ *Unclear whether
anyone checks this proactively or only when the room feels stuffy — worth confirming,
because it changes whether air deserves top-level navigation or a corner of a status page.*

**J15. Calibrate a machine ✅**
Rare, technical, and only meaningful for machines with a distinguishable play
signature. Should be discoverable from the machine page, not a top-level concept.

---

## 4. What the current interface gets wrong

Named plainly, so the redesign has explicit targets.

1. **Navigation is three links.** `Home / Usage / Air`, with events, strips, circuits,
   and machine detail reachable only by clicking through or knowing the URL. There is
   no sense of place and no way back up the hierarchy.
2. **Tuned for browsing, not for the two things people actually do.** Opening and
   closing the museum are ~90% of usage and are not the shape of the home page.
3. **No search.** With 33 machines and growing, finding one by name is a scan.
4. **URLs are keyed on `plug_id`.** A machine's page changes address when it moves
   outlets — links and bookmarks rot exactly when things are most confusing.
5. **Status vocabulary is inconsistent.** Relay state, drawing, `power_status`, the
   `State` enum, offline, and calibration status overlap and are rendered
   inconsistently across pages. This has produced recurring bugs, not just confusion.
6. **Problems don't surface themselves.** Offline devices, `no_draw` outlets, untagged
   outlets, stale assignments, and overload shutdowns all require you to go looking.
   The system knows about every one of them.
7. **Unreliable — three specific symptoms** ✅, in the operator's own terms:
   - **a. User actions don't always work.**
   - **b. Reboots are slow.**
   - **c. You frequently end up logged out.**

   (a) and (c) were **the same bug**, now fixed in #77. Session storage set no `max_age`, so
   the cookie was a *browser-session* cookie that died whenever the phone or tablet reaped
   the browser session. The failure was silent: public-readable pages kept rendering
   perfectly, the controls simply stopped working and writes returned 401. It looked
   like "the button did nothing", not like "you're logged out". `SESSION_MAX_AGE` is now
   30 days. **Surfacing auth state explicitly is still outstanding** — the new UI should
   make a lost session loud and one-tap recoverable rather than an invisible downgrade to
   read-only.

   (b) is structural, not a bug. Every command is a WAN round-trip to the TP-Link cloud;
   a reboot is off → 3 s hold → on, each half retrying up to 6 times with backoff, and
   confirmation waits on the recorder's poll. That's 4–30 s. The fix is honest
   *pending / confirmed / failed* states with visible progress, not more speed.

   Contributing factors also visible in the code: fixed-poll vs. SSE races on power
   buttons, relay state lagging a command by a poll cycle, stale-duplicate machines
   during a move, and a 7,400-line `server.py` with eight hand-maintained inline HTML
   templates that drift apart.
8. **Mobile is an afterthought** ✅ — and mobile is the primary surface (§2).
9. **The public view is fine in principle, wrong in emphasis.** ✅ Operator-view-minus is
   the right model. The problem is that the *operator* view it derives from is the weak
   one, and that redaction is scattered through handlers rather than being one clear
   boundary.

---

## 5. Design principles that follow

1. **One status vocabulary**, defined once and rendered identically everywhere. Settled
   in `status_vocabulary.md`, which resolves `domain_model.md` §7.1–7.3: four named axes,
   one derived status (`unreachable · off · no_draw · powered · attract · playing ·
   abandoned`), derived server-side exactly once. No surface re-derives it.
2. **Name-first navigation.** Search by machine name, everywhere, always. Address
   machines by `asset_id`, not `plug_id`.
3. **Push, don't poll.** SSE already exists; the UI should never disagree with the
   server about relay state.
4. **Surface problems, don't file them.** Anything juice knows is wrong belongs on the
   home view, not behind a diagnostic page.
5. **Honest about uncertainty.** "Offline — last seen 14 min ago" and "uncalibrated —
   play time not measurable" are better than a confident gray box.
6. **Every action has three states**: pending, confirmed, failed — with the retry count
   visible. Actuation takes seconds, sometimes tens of seconds, and pretending otherwise
   is the root of "the button did nothing".
7. **Never fail silently, especially on auth.** Losing a session must be loud and
   recoverable in one tap, not an invisible downgrade to read-only.
8. **Sessions should outlive the browser.** A front-desk tablet should stay logged in for
   weeks.
9. **Assume you are not alone.** Show who else acted, and when. Recent actor and source
   belong on the machine view by default, not behind an audit link.
10. **Thumb-sized targets, high contrast.** Museum floor, dim lighting, one hand.
11. **Explain refusals.** Every skip in a bulk operation has a reason the domain model
    already knows. Say it. This includes "someone else is already running this".
12. **Public is operator-minus.** One information architecture, one redaction boundary,
    applied at the edge — not a second design.

---

## 6. Scope decisions ✅

- **Rarely-used admin stays in the CLI** for now (`doctor`, `discover`, calibration
  plumbing, backup). The web UI should surface what the CLI *diagnoses* as problems, but
  needn't reimplement the fixes.
- **FlipFix inlining is worth a modest amount, at low priority.** Showing open problem
  reports on a machine page makes sense eventually; it should not shape the redesign.

---

## 7. Open questions

- ❓ Does anyone check air quality proactively, or only when the room feels stuffy?
  Determines whether it earns top-level navigation. (J14)
- ❓ Is a shared "I'm on it" marker worth building, or is the power-event audit trail
  enough for multi-operator awareness? (J6)
- ❓ Does the front-desk tablet want a dedicated always-on layout, or is the normal
  status view at a larger size enough?
- ❓ Are there floor areas / zones that operators think in, beyond strips and circuits?
  (J3 grouping.)
