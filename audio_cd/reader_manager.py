"""Manage exclusive ownership of a sequential CD reader."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Sequence
from contextlib import aclosing

from .sequential_reader import SequentialCDReader, SequentialReaderError


class CDReaderManager:
    """Manage one reader for a fixed disc TOC.

    Consecutive requests share a reader. Other requests replace it only
    after cleanup. The owner must close this manager when the disc changes
    or the playback session ends.
    """

    def __init__(self, device: str, track_sectors: Sequence[tuple[int, int]]) -> None:
        self.device = device
        self.track_sectors = tuple(track_sectors)
        self._numbers = tuple(number for number, _ in self.track_sectors)
        if not self._numbers or any(
            n < 1 or s < 1 for n, s in self.track_sectors
        ) or any(a >= b for a, b in zip(self._numbers, self._numbers[1:])):
            raise ValueError("Expected positive, strictly ordered CD tracks")
        self._lock = asyncio.Lock()
        self._reader: SequentialCDReader | None = None
        self._last_index: int | None = None
        self._closed = False

    async def _release(self) -> None:
        """Release under the ownership lock; retain ownership if cancelled."""
        if self._reader is not None:
            await self._reader.close()
            self._reader = None
        self._last_index = None

    async def close(self) -> None:
        """Permanently close this disc session."""
        async with self._lock:
            self._closed = True
            await self._release()

    async def stream_track(
        self, track_number: int, seek_position: int = 0
    ) -> AsyncGenerator[bytes]:
        """Stream a track, restarting for nonconsecutive requests or seeks."""
        if seek_position < 0:
            raise ValueError("Seek position must not be negative")
        try:
            index = self._numbers.index(track_number)
        except ValueError as err:
            raise SequentialReaderError("Track is not on this disc") from err

        reader = None
        stream = None
        first = None
        try:
            async with self._lock:
                if self._closed:
                    raise SequentialReaderError("Reader manager is closed")
                if (
                    self._reader is None
                    or self._last_index is None
                    or index != self._last_index + 1
                    or seek_position
                ):
                    # Invalidate before awaiting so a cancelled replacement
                    # cannot accidentally reuse a reader that is closing.
                    self._last_index = None
                    await self._release()
                    self._reader = SequentialCDReader(self.device, self.track_sectors)
                reader = self._reader
                self._last_index = index
                stream = reader.stream_track(track_number)
                # Schedule registration in request order, without holding the
                # lock while waiting for audio (which may need another consumer).
                first = asyncio.create_task(anext(stream))
                await asyncio.sleep(0)

            skip = seek_position * 176400
            async with aclosing(stream):
                try:
                    chunk = await first
                except StopAsyncIteration:
                    return
                while True:
                    if skip:
                        count = min(skip, len(chunk))
                        skip -= count
                        chunk = chunk[count:]
                    if chunk:
                        yield chunk
                    try:
                        chunk = await anext(stream)
                    except StopAsyncIteration:
                        break
        except SequentialReaderError:
            async with self._lock:
                if reader is not None and self._reader is reader:
                    self._last_index = None
                    await self._release()
            raise
        finally:
            if first is not None:
                if not first.done():
                    first.cancel()
                await asyncio.gather(first, return_exceptions=True)
            if stream is not None:
                await stream.aclose()
