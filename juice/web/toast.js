// Transient toast notification — DOM glue that operates on the ambient document.
// Shared because it was copy-pasted byte-for-byte on four pages (dashboard,
// detail, strip, circuit). Inlined via the JS_TOAST marker.
export function showToast(msg, type) {
  const existing = document.querySelector('.toast');
  if (existing) existing.remove();
  const t = document.createElement('div');
  t.className = 'toast toast-' + type;
  t.textContent = msg;
  document.body.appendChild(t);
  setTimeout(() => { t.style.opacity = '0'; setTimeout(() => t.remove(), 300); }, 4000);
}
