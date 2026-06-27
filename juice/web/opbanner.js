import { escapeHtml } from './format.js';

// Pure descriptor for the dashboard's all-on/all-off operation banner. Given the
// current operation (or null), returns what the banner should show — the glue in
// the template applies it to the DOM (hidden, class toggles, text/innerHTML, the
// Cancel button). Page state threaded in, no globals. See web/README.md.
//
// Shape:
//   { hidden: true }                              // no operation → banner off
//   { hidden: false, cancelled, complete, retrying,   // class toggles
//     text } | { html },                          // textContent vs innerHTML
//   plus { cancelHidden, cancelDisabled }         // Cancel button state
// Exactly one of `text` / `html` is set when visible; `html` (retrying) carries
// pre-escaped interpolations, so the glue assigns it via innerHTML.
export function buildOpBanner(op) {
  if (!op) return { hidden: true };

  const cancelled = op.state === 'cancelled';
  const complete = op.state === 'complete';
  const isRetrying = op.state === 'running' && !!op.retrying;
  const noun = op.kind === 'all_on' ? 'All-on' : 'All-off';
  // Strip-scoped ops carry a label (e.g. "Backline strip") prefixed onto the
  // banner so it's clear which strip is cycling. Global ops have no label →
  // empty prefix → text byte-identical to before. `html` gets the escaped form.
  const prefix = op.label ? op.label + ': ' : '';
  const prefixHtml = op.label ? escapeHtml(op.label) + ': ' : '';

  if (cancelled) {
    return {
      hidden: false, cancelled, complete, retrying: isRetrying,
      text: prefix + noun + ' cancelled — ' + op.completed.length + '/' + op.total + ' complete',
      cancelHidden: true,
    };
  }
  if (complete) {
    return {
      hidden: false, cancelled, complete, retrying: isRetrying,
      text: prefix + noun + ' complete — ' + op.completed.length + '/' + op.total
        + (op.failed.length ? ' (' + op.failed.length + ' failed)' : ''),
      cancelHidden: true,
    };
  }
  if (isRetrying) {
    const r = op.retrying;
    const target = r.machine_name ? ' ' + r.machine_name : '';
    const delay = r.delay != null ? r.delay.toFixed(1) + 's' : '…';
    return {
      hidden: false, cancelled, complete, retrying: isRetrying,
      html: prefixHtml + '<span class="retry-spinner"></span>'
        + 'Retrying' + escapeHtml(target)
        + ' (attempt ' + r.next_attempt + '): '
        + escapeHtml(r.error || 'transient failure')
        + '. Next try in ' + delay + '…',
      cancelHidden: false, cancelDisabled: !!op.cancel_requested,
    };
  }
  const verb = op.kind === 'all_on' ? 'Turning on' : 'Turning off';
  const idx = (op.index || 0) + 1;
  const target = op.current_machine ? ' ' + op.current_machine : '';
  return {
    hidden: false, cancelled, complete, retrying: isRetrying,
    text: prefix + verb + ' ' + idx + '/' + op.total + target + '…',
    cancelHidden: false, cancelDisabled: !!op.cancel_requested,
  };
}
