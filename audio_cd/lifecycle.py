"""Standalone lifetime coordination for a fixed CD disc session."""
from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Sequence
from contextlib import aclosing

from .drive_access import DriveAccessCoordinator
from .reader_manager import CDReaderManager
from .sequential_reader import SequentialReaderError


class CDLifecycle:
    """Retain a manager across normal track boundaries, with bounded idle time."""

    def __init__(
        self, device: str, track_sectors: Sequence[tuple[int, int]],
        idle_timeout: float = 90,
        coordinator: DriveAccessCoordinator | None = None,
    ) -> None:
        if idle_timeout <= 0:
            raise ValueError("Idle timeout must be positive")
        self.device = device
        self.track_sectors = tuple(track_sectors)
        self.idle_timeout = idle_timeout
        self._lock = asyncio.Lock()
        self._coordinator = coordinator
        self._ownership: object | None = None
        self._acquiring: asyncio.Task[object] | None = None
        self._manager: CDReaderManager | None = None
        self._active = 0
        self._timer: asyncio.Task[None] | None = None
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None

    def _cancel_timer(self) -> None:
        if self._timer is not None:
            self._timer.cancel()
            self._timer = None

    async def _release(self) -> None:
        # Keep ownership until shutdown actually completes.
        if self._manager is not None:
            await self._manager.close()
            self._manager = None
        self._release_ownership()

    def _release_ownership(self) -> None:
        if self._ownership is not None:
            assert self._coordinator is not None
            self._coordinator.release(self._ownership)
            self._ownership = None

    async def _acquire_ownership(self) -> None:
        if self._coordinator is None:
            return
        task = asyncio.create_task(self._coordinator.acquire_reader())
        self._acquiring = task
        try:
            self._ownership = await task
        except asyncio.CancelledError:
            # A completed acquisition can race with cancellation of its caller.
            if task.done() and not task.cancelled():
                self._ownership = task.result()
                self._release_ownership()
            if self._closed:
                raise SequentialReaderError("Lifecycle is closed") from None
            raise
        finally:
            self._acquiring = None

    async def _expire(self, manager: CDReaderManager) -> None:
        try:
            await asyncio.sleep(self.idle_timeout)
            async with self._lock:
                if self._timer is not asyncio.current_task():
                    return
                self._timer = None
                if self._manager is manager and not self._active:
                    await self._release()
        finally:
            if self._timer is asyncio.current_task():
                self._timer = None

    async def _finish(self, manager: CDReaderManager, completed: bool) -> None:
        async with self._lock:
            self._active -= 1
            if self._manager is not manager or self._active or self._closed:
                return
            if completed:
                self._timer = asyncio.create_task(self._expire(manager))
            else:
                await self._release()

    async def stream_track(
        self, track_number: int, seek_position: int = 0
    ) -> AsyncGenerator[bytes]:
        """Track consumer lifetime, including cancellation and generator closure."""
        if seek_position < 0:
            raise ValueError("Seek position must not be negative")
        if not any(number == track_number for number, _ in self.track_sectors):
            raise SequentialReaderError("Track is not on this disc")
        async with self._lock:
            if self._closed:
                raise SequentialReaderError("Lifecycle is closed")
            self._cancel_timer()
            if self._manager is None:
                await self._acquire_ownership()
                try:
                    if self._closed:
                        raise SequentialReaderError("Lifecycle is closed")
                    self._manager = CDReaderManager(self.device, self.track_sectors)
                except BaseException:
                    self._release_ownership()
                    raise
            manager = self._manager
            self._active += 1
        completed = False
        try:
            async with aclosing(manager.stream_track(track_number, seek_position)) as stream:
                async for chunk in stream:
                    yield chunk
            completed = True
        finally:
            # A second cancellation must not interrupt ownership cleanup.
            task = asyncio.create_task(self._finish(manager, completed))
            await asyncio.shield(task)

    async def _close(self) -> None:
        async with self._lock:
            self._cancel_timer()
            await self._release()

    async def close(self) -> None:
        """Permanently close this lifecycle, sharing shutdown among callers."""
        self._closed = True
        if self._acquiring is not None:
            self._acquiring.cancel()
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._close())
        await asyncio.shield(self._close_task)
