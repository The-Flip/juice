"""Error taxonomy.

The split that matters is *recoverable in place* vs *the process should die*:

- `TransientError` — a blip. Retry, count it, carry on. The device is fine.
- `DeviceAuthError` — credentials were rejected. Retrying at poll cadence is how
  you get rate-limited out of your own hardware, so this parks the device and
  asks for a human instead.
- `FatalError` — an invariant the process cannot recover from by trying again
  (unwritable buffer, corrupt database, bad config). Carries the exit code so
  the supervisor's restart is a deliberate act rather than a stack trace.

Device adapters translate their library's exceptions into these; nothing above
`tap.kasa_*` should need to know what `python-kasa` raises.
"""

from __future__ import annotations

# Exit codes. 2 is the conventional "your invocation was wrong"; 70 is
# EX_SOFTWARE from sysexits.h, "an internal software error". Docker's
# restart policy treats both as a crash, which is what we want — a daemon
# that stays up doing nothing is worse than one that visibly restarts.
EXIT_CONFIG = 2
EXIT_INTERNAL = 70


class TapError(Exception):
    """Base for every error tap raises deliberately."""


class TransientError(TapError):
    """A failure worth retrying: a timeout, a refused connection, a blip."""


class DeviceAuthError(TapError):
    """The device rejected our credentials. Needs a human, not patience."""


class DeviceExcludedError(TapError):
    """Config says not to poll this device. Stop, do not retry."""


class FatalError(TapError):
    """The process cannot continue. Exit with `code` and let the supervisor restart."""

    def __init__(self, message: str, code: int = EXIT_INTERNAL) -> None:
        super().__init__(message)
        self.code = code
