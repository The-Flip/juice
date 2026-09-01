"""Version 2 of the juice HTTP API.

Designed against domain_model.md, user_needs.md and status_vocabulary.md rather
than grown alongside a UI. Mounted into the same aiohttp application as v1, which
keeps running untouched until the new interface retires it.
"""

from juice.api.v2.routes import ROUTES, V2_PREFIX, register_v2

__all__ = ["ROUTES", "V2_PREFIX", "register_v2"]
