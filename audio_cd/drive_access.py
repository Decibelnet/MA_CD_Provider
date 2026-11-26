"""Coordinate metadata and extraction access to one optical drive."""
from __future__ import annotations

import asyncio


class DriveAccessCoordinator:
    """Event-loop-local ownership gate; callers release after physical cleanup.

    Metadata waits for other metadata queries, but skips extraction ownership.
    Extraction waits for ownership to become available. Each acquisition uses
    a unique token, preventing stale releases from affecting a newer owner.
    """

    def __init__(self) -> None:
        self._owner: object | None = None
        self._reading = False
        self._changed = asyncio.Event()

    async def acquire_query(self) -> object | None:
        """Acquire query ownership, or return None while extraction owns it."""
        while self._owner is not None:
            if self._reading:
                return None
            await self._changed.wait()
        return self._claim(reading=False)

    async def acquire_reader(self) -> object:
        """Wait for exclusive extraction ownership."""
        while self._owner is not None:
            await self._changed.wait()
        return self._claim(reading=True)

    def _claim(self, *, reading: bool) -> object:
        # No suspension between checking availability and assigning ownership.
        token = object()
        self._owner = token
        self._reading = reading
        self._changed = asyncio.Event()
        return token

    def release(self, token: object) -> None:
        """Release only the matching acquisition, after subprocess cleanup."""
        if self._owner is None or self._owner is not token:
            raise RuntimeError("Invalid drive ownership token")
        self._owner = None
        self._reading = False
        self._changed.set()
