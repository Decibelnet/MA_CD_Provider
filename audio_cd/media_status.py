"""Read Linux optical media status without starting an extraction process."""
from __future__ import annotations

import asyncio
import fcntl
import os
from dataclasses import dataclass, replace
from enum import IntEnum

CDROM_DRIVE_STATUS = 0x5326
CDROM_MEDIA_CHANGED = 0x5325
CDSL_CURRENT = 0x7FFFFFFF


class DriveStatus(IntEnum):
    UNKNOWN = 0
    NO_DISC = 1
    TRAY_OPEN = 2
    NOT_READY = 3
    DISC_PRESENT = 4


@dataclass(frozen=True)
class MediaObservation:
    """None means the change indicator could not be verified."""

    status: DriveStatus
    changed: bool | None
    error: str | None = None
    retained: bool = False


def _read_media(device: str) -> MediaObservation:
    status = DriveStatus.UNKNOWN
    changed = None
    try:
        fd = os.open(device, os.O_RDONLY | os.O_NONBLOCK)
        try:
            raw_status = fcntl.ioctl(fd, CDROM_DRIVE_STATUS, CDSL_CURRENT)
            try:
                status = DriveStatus(raw_status)
            except ValueError:
                status = DriveStatus.UNKNOWN
            raw_changed = fcntl.ioctl(fd, CDROM_MEDIA_CHANGED, CDSL_CURRENT)
            if raw_changed in (0, 1):
                changed = bool(raw_changed)
        finally:
            os.close(fd)
    except OSError as err:
        return MediaObservation(status, changed, str(err))
    return MediaObservation(status, changed)


class MediaStatus:
    """One event-loop-local observer per drive.

    Retain a pending observation across caller cancellation: the worker
    cannot be cancelled safely and may have consumed a change notification.
    The next caller receives that observation before starting another check.
    """

    def __init__(self, device: str = "/dev/sr0") -> None:
        self.device = device
        self._lock = asyncio.Lock()
        self._pending: asyncio.Task[MediaObservation] | None = None

    async def check(self) -> MediaObservation:
        async with self._lock:
            retained = self._pending is not None
            if self._pending is None:
                self._pending = asyncio.create_task(
                    asyncio.to_thread(_read_media, self.device)
                )
            task = self._pending
            try:
                result = await asyncio.shield(task)
            except asyncio.CancelledError:
                raise
            except Exception:
                self._pending = None
                raise
            self._pending = None
            return replace(result, retained=True) if retained else result
