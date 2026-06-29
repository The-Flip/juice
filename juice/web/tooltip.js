// Shared horizontal placement for chart hover tooltips. Pure math (no DOM): the
// caller measures the rendered tooltip width and passes it in, so the clamp uses
// the real width rather than a hardcoded guess. Keeps a tooltip from vanishing
// off the right (or left) edge of the viewport near the chart's edges.

// Given the anchor x (the hovered point / cursor, in the same coordinate space as
// `viewportWidth` — i.e. viewport pixels), return the tooltip's left so it stays
// on screen: preferred to the right of the anchor, flipped to the left when that
// would overflow the right edge, then clamped into [pad, viewportWidth-width-pad].
export function placeTooltipX(anchorX, tooltipWidth, viewportWidth, offset = 14, pad = 8) {
  let left = anchorX + offset; // default: just right of the anchor
  if (left + tooltipWidth + pad > viewportWidth) {
    left = anchorX - offset - tooltipWidth; // flip to the left of the anchor
  }
  // Final clamp: never past the right edge, never before the left edge. (When the
  // tooltip is wider than the viewport the right clamp wins, then the left clamp
  // pins it to `pad` — best effort.)
  const maxLeft = viewportWidth - tooltipWidth - pad;
  if (left > maxLeft) left = maxLeft;
  if (left < pad) left = pad;
  return left;
}
