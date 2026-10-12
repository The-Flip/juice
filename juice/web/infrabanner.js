import { escapeHtml } from './format.js';

// Pure descriptor for the dashboard's infrastructure banner. Given the
// `infrastructure` array from GET /api/v2/floor, returns what the banner should
// show — the glue in the template applies it to the DOM. See web/README.md.
//
// This exists because a strip juice cannot reach used to be invisible: its
// machines rendered as ordinary `off` tiles and nothing said otherwise, so the
// museum opened twice with twenty machines dark and no signal anywhere.
//
// The server has already done the collapsing and the judgement — one entry per
// unreachable *device* rather than per machine, or a single collector entry when
// the whole uplink is down, because nine unreachable strips would be nine wrong
// diagnoses. This function must not re-derive any of that from machine statuses:
// a UI surface reads what the server decided (status_vocabulary.md rule 5).
//
// Shape:
//   { hidden: true }                        // nothing wrong → banner off
//   { hidden: false, kind, html }           // `kind` drives the class toggle
export function buildInfraBanner(infrastructure) {
  const entries = infrastructure || [];
  if (!entries.length) return { hidden: true };

  // A collector entry is always alone and always outranks the rest: if juice has
  // lost its collector the strips are probably fine, and saying "3 strips
  // unreachable" would send someone to the wrong floor.
  const collector = entries.find((e) => e.kind === 'collector_offline'
                                     || e.kind === 'collector_silent');
  if (collector) {
    const affected = (collector.affects || []).length;
    const cost = affected ? ` — ${affected} machine${affected === 1 ? '' : 's'} affected` : '';
    return collector.kind === 'collector_offline'
      ? {
          hidden: false, kind: 'collector_offline',
          html: '<strong>No collector connected.</strong> Nothing can be switched on or off'
                + escapeHtml(cost) + '.',
        }
      : {
          hidden: false, kind: 'collector_silent',
          html: '<strong>The collector is catching up.</strong> Power control still works, '
                + 'but the tiles are not moving' + escapeHtml(cost) + '.',
        };
  }

  const dark = entries.reduce((n, e) => n + (e.affects || []).length, 0);
  const machines = `${dark} machine${dark === 1 ? '' : 's'}`;
  // One strip gets named; several get counted, because the banner is one line
  // and the names are in the tiles either way.
  const what = entries.length === 1
    ? `<strong>${escapeHtml(entries[0].name || 'A strip')}</strong> is unreachable`
    : `<strong>${entries.length} strips</strong> are unreachable`;
  return {
    hidden: false, kind: 'unreachable_device',
    // Deliberately no "since": the entry's timestamp is when juice gave up on
    // the device, not when it died, and a precise-looking duration would be a
    // claim we cannot make.
    html: `${what} — ${machines} cannot be seen or switched.`,
  };
}
