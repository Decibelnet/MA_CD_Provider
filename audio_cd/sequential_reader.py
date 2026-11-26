"""Experimental single-process reader for sequential audio-CD tracks.

This module is deliberately not wired into the provider yet. It provides a
bounded, testable prototype for routing one continuous cdparanoia read into
per-track async streams. Track lengths are expressed in CD sectors (2352 PCM
bytes per sector), avoiding rounded-duration boundary errors.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import AsyncGenerator, Iterable, Iterator, Sequence
from dataclasses import dataclass

LOGGER = logging.getLogger(__name__)

PCM_BYTES_PER_SECTOR = 2352
DEFAULT_CHUNK_SIZE = 64 * 1024
DEFAULT_TRACK_BUFFER_BYTES = 10 * 1024 * 1024


class SequentialReaderError(RuntimeError):
    """The sequential CD reader cannot satisfy a stream request."""


@dataclass(frozen=True, slots=True)
class TrackChunk:
    """A PCM chunk and the 1-based track number it belongs to."""

    track_number: int
    pcm: bytes


def split_pcm_chunks(
    chunks: Iterable[bytes],
    track_sectors: Sequence[tuple[int, int]],
) -> Iterator[TrackChunk]:
    """Split a continuous PCM byte stream at exact CD track boundaries.

    Args:
        chunks: Arbitrarily-sized PCM chunks from one continuous drive read.
        track_sectors: Ordered (1-based track number, sector count) pairs.
    """
    if not track_sectors:
        return
    if any(number < 1 or sectors < 1 for number, sectors in track_sectors):
        raise ValueError("Track numbers and sector counts must be positive")
    if any(
        track_sectors[index][0] >= track_sectors[index + 1][0]
        for index in range(len(track_sectors) - 1)
    ):
        raise ValueError("Tracks must be supplied in strictly increasing order")

    track_index = 0
    remaining = track_sectors[0][1] * PCM_BYTES_PER_SECTOR

    for source_chunk in chunks:
        view = memoryview(source_chunk)
        offset = 0
        while offset < len(view):
            if track_index >= len(track_sectors):
                raise SequentialReaderError(
                    "cdparanoia produced more PCM than the requested track range"
                )
            take = min(remaining, len(view) - offset)
            if take:
                yield TrackChunk(
                    track_number=track_sectors[track_index][0],
                    pcm=bytes(view[offset : offset + take]),
                )
                offset += take
                remaining -= take
            if remaining == 0:
                track_index += 1
                if track_index < len(track_sectors):
                    remaining = (
                        track_sectors[track_index][1] * PCM_BYTES_PER_SECTOR
                    )

    if track_index < len(track_sectors):
        raise SequentialReaderError(
            "cdparanoia ended before all requested track sectors were read"
        )


@dataclass(frozen=True, slots=True)
class _EndOfTrack:
    pass


@dataclass(frozen=True, slots=True)
class _ReadFailure:
    error: BaseException


@dataclass(frozen=True, slots=True)
class _ReaderClosed:
    pass


_END = _EndOfTrack()
_CLOSED = _ReaderClosed()


class SequentialCDReader:
    """One cdparanoia process feeding bounded per-track queues.

    The first request starts a continuous read from that track through the
    final track in the supplied TOC. A consumer drains its track queue while
    the next track is retained in a bounded queue (10 MiB by default, about
    57 seconds of CDDA PCM). The producer naturally pauses when that queue is
    full, so memory use stays bounded.

    This first prototype intentionally supports one stream consumer per track
    and sequential, unseeked reads only. Seeking/restarts and provider
    lifecycle integration are follow-up work; the production provider still
    uses its existing drive path until those cases are tested.
    """

    def __init__(
        self,
        device: str,
        track_sectors: Sequence[tuple[int, int]],
        *,
        buffer_bytes: int = DEFAULT_TRACK_BUFFER_BYTES,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> None:
        if not track_sectors:
            raise ValueError("At least one audio track is required")
        if buffer_bytes < chunk_size or chunk_size < 1:
            raise ValueError("buffer_bytes must be >= chunk_size > 0")
        self.device = device
        self.track_sectors = tuple(track_sectors)
        self.chunk_size = chunk_size
        queue_chunks = max(1, buffer_bytes // chunk_size)
        self._queues: dict[int, asyncio.Queue[bytes | _EndOfTrack | _ReadFailure | _ReaderClosed]] = {
            number: asyncio.Queue(maxsize=queue_chunks)
            for number, _ in self.track_sectors
        }
        self._consumers: set[int] = set()
        self._requested: set[int] = set()
        self._discarded: set[int] = set()
        self._process: asyncio.subprocess.Process | None = None
        self._producer: asyncio.Task[None] | None = None
        self._start_track: int | None = None
        self._closed = False
        self._close_task: asyncio.Task[None] | None = None

    async def stream_track(self, track_number: int) -> AsyncGenerator[bytes]:
        """Yield one track from the shared sequential read."""
        if self._closed:
            raise SequentialReaderError("Reader is closed")
        if track_number not in self._queues:
            raise SequentialReaderError(f"Track {track_number} is not on this disc")
        if track_number in self._requested:
            raise SequentialReaderError(
                f"Track {track_number} can only be consumed once by this reader"
            )

        if self._producer is None:
            self._start_track = track_number
            self._producer = asyncio.create_task(
                self._read_sequentially(track_number),
                name=f"cd-sequential-reader-{track_number}",
            )
        elif self._start_track is not None and track_number < self._start_track:
            raise SequentialReaderError(
                "Cannot request a track before the active reader's start track"
            )
        elif track_number in self._discarded:
            raise SequentialReaderError(
                "This track's stream was cancelled; restart the reader before retrying"
            )

        self._consumers.add(track_number)
        self._requested.add(track_number)
        queue = self._queues[track_number]
        completed = False
        try:
            while True:
                entry = await queue.get()
                if isinstance(entry, _ReaderClosed):
                    raise SequentialReaderError("Reader is closed")
                if isinstance(entry, _EndOfTrack):
                    completed = True
                    return
                if isinstance(entry, _ReadFailure):
                    raise SequentialReaderError(str(entry.error)) from entry.error
                yield entry
        finally:
            self._consumers.discard(track_number)
            # If MA cancels a stream (for example, during a seek), discard the
            # rest of that track so the one physical reader can move forward.
            if not completed:
                self._discarded.add(track_number)
                # Release a producer blocked by this track's bounded queue.
                while not queue.empty():
                    with contextlib.suppress(asyncio.QueueEmpty):
                        queue.get_nowait()

    async def _read_sequentially(self, start_track: int) -> None:
        assert self._process is None
        start_index = next(
            index
            for index, (number, _) in enumerate(self.track_sectors)
            if number == start_track
        )
        selected_tracks = self.track_sectors[start_index:]
        last_track = selected_tracks[-1][0]
        command = (
            "cdparanoia",
            "-d",
            self.device,
            "-r",
            f"{start_track}-{last_track}",
            "-",
        )
        try:
            startup = asyncio.create_task(
                asyncio.create_subprocess_exec(
                    *command,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.DEVNULL,
                )
            )
            try:
                self._process = await asyncio.shield(startup)
            except asyncio.CancelledError:
                # Shutdown owns the producer until startup settles, so a
                # process created during cancellation is still reaped.
                self._process = await startup
                raise
            assert self._process.stdout is not None
            track_index = 0
            remaining = selected_tracks[0][1] * PCM_BYTES_PER_SECTOR
            while chunk := await self._process.stdout.read(self.chunk_size):
                view = memoryview(chunk)
                offset = 0
                while offset < len(view):
                    if track_index >= len(selected_tracks):
                        raise SequentialReaderError(
                            "cdparanoia produced PCM beyond the final track"
                        )
                    track_number = selected_tracks[track_index][0]
                    take = min(remaining, len(view) - offset)
                    piece = bytes(view[offset : offset + take])
                    offset += take
                    remaining -= take
                    if track_number not in self._discarded:
                        await self._queues[track_number].put(piece)
                    if remaining == 0:
                        if track_number not in self._discarded:
                            await self._queues[track_number].put(_END)
                        track_index += 1
                        if track_index == len(selected_tracks):
                            if offset < len(view):
                                raise SequentialReaderError(
                                    "cdparanoia produced PCM beyond the final track"
                                )
                            break
                        remaining = (
                            selected_tracks[track_index][1] * PCM_BYTES_PER_SECTOR
                        )

            return_code = await self._process.wait()
            if return_code != 0:
                raise SequentialReaderError(
                    f"cdparanoia exited with status {return_code}"
                )
            if track_index < len(selected_tracks):
                raise SequentialReaderError(
                    "cdparanoia ended before the final track boundary"
                )
        except asyncio.CancelledError:
            raise
        except BaseException as err:
            LOGGER.exception("Sequential CD read failed")
            for number, _ in selected_tracks:
                queue = self._queues[number]
                while not queue.empty():
                    with contextlib.suppress(asyncio.QueueEmpty):
                        queue.get_nowait()
                if number not in self._discarded:
                    await queue.put(_ReadFailure(err))
        finally:
            process = self._process
            self._process = None
            if process is not None and process.returncode is None:
                process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), timeout=2)
                except TimeoutError:
                    process.kill()
                    await process.wait()

    async def close(self) -> None:
        """Stop the shared process and release all reader resources."""
        if self._close_task is None:
            self._closed = True
            self._close_task = asyncio.create_task(self._close_impl())
        await asyncio.shield(self._close_task)

    async def _close_impl(self) -> None:
        """Perform cleanup once, independently of individual close callers."""
        # Cancel before draining queues so the producer cannot refill them.
        if self._producer is not None and not self._producer.done():
            self._producer.cancel()
        # Closing abandons buffered PCM and wakes every active consumer.
        # No await here: even a full queue must accept the shutdown marker.
        for number, queue in self._queues.items():
            while not queue.empty():
                queue.get_nowait()
            if number in self._consumers:
                queue.put_nowait(_CLOSED)
        if self._producer is not None and not self._producer.done():
            with contextlib.suppress(asyncio.CancelledError):
                await self._producer
        process = self._process
        if process is not None and process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=2)
            except TimeoutError:
                process.kill()
                await process.wait()
