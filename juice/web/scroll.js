// Honor a URL-hash deep link after async content (charts) renders. The browser
// scrolls to the hash on load, but charts above the target render asynchronously
// and grow the page afterward — pushing the target down so the initial scroll
// lands too high (often the top). Re-align to the target as the page grows (a
// ResizeObserver on <body>), bailing on the first real user scroll intent so we
// never hijack scrolling; a safety timeout stops observing once layout settles.
//
// `win` is injected (the page calls `honorHashScroll(window)`) so the DOM glue is
// unit-testable with a jsdom window + a mock ResizeObserver.
export function honorHashScroll(win) {
  const { location, document } = win;
  if (!location.hash) return;
  let hashId = location.hash.slice(1);
  // decodeURIComponent throws on a malformed fragment (e.g. "#%"); fall back to raw.
  try {
    hashId = decodeURIComponent(hashId);
  } catch (e) {
    /* keep the raw fragment */
  }

  let userScrolled = false;
  const realign = () => {
    if (userScrolled) return;
    // Re-query each time: the target may be created later (e.g. the air page
    // builds its #air-<metric> panels only after its data fetch resolves).
    const target = document.getElementById(hashId);
    if (target) target.scrollIntoView();
  };
  const ro = new win.ResizeObserver(realign);
  const stop = () => {
    userScrolled = true;
    ro.disconnect();
  };
  for (const ev of ['wheel', 'touchstart', 'keydown']) {
    win.addEventListener(ev, stop, { passive: true });
  }
  ro.observe(document.body); // fires as the async charts grow the page
  realign();
  win.setTimeout(() => ro.disconnect(), 10000);
}
