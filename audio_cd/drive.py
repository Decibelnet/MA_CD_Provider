"""Non-blocking wrapper around cdparanoia for live audio-CD reads.

No audio is written to disk: track bytes are streamed from the child process stdout.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncGenerator


LOGGER = logging.getLogger(__name__)


class DriveError(RuntimeError):
    """Optical drive operation failed."""


class CDDrive:
    """Access one Linux optical drive through cdparanoia."""

    def __init__(self, device: str = "/dev/sr0") -> None:
        self.device = device

    async def query(self) -> str:
        """Return cdparanoia's disc/track table output."""
        try:
            proc = await asyncio.create_subprocess_exec(
                "cdparanoia",
                "-Q",
                "-d",
                self.device,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
        except FileNotFoundError as err:
            raise DriveError(
                "cdparanoia is not installed in the Music Assistant container"
            ) from err

        output, _ = await proc.communicate()
        text = output.decode("utf-8", errors="replace")
        if proc.returncode != 0:
            raise DriveError(text.strip() or f"cdparanoia exited with {proc.returncode}")
        return text

    async def stream_track(self, track_number: int) -> AsyncGenerator[bytes]:
        """Yield raw 44.1 kHz, 16-bit stereo PCM for one 1-based track."""
        if track_number < 1:
            raise ValueError("Track numbers are 1-based and must be positive")

        read_started = time.monotonic()
        try:
            proc = await asyncio.create_subprocess_exec(
                "cdparanoia",
                "-d",
                self.device,
                "-r",
                str(track_number),
                "-",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
            )
        except FileNotFoundError as err:
            raise DriveError(
                "cdparanoia is not installed in the Music Assistant container"
            ) from err

        process_started = time.monotonic()
        LOGGER.info(
            "CD read track %s: cdparanoia process started in %.3f s",
            track_number,
            process_started - read_started,
        )
        assert proc.stdout is not None
        first_chunk = True
        try:
            while chunk := await proc.stdout.read(64 * 1024):
                if first_chunk:
                    first_chunk = False
                    first_pcm_at = time.monotonic()
                    LOGGER.info(
                        "CD read track %s: first PCM chunk (%s bytes) arrived %.3f s "
                        "after process start (%.3f s total)",
                        track_number,
                        len(chunk),
                        first_pcm_at - process_started,
                        first_pcm_at - read_started,
                    )
                yield chunk
            return_code = await proc.wait()
            if return_code != 0:
                raise DriveError(
                    f"cdparanoia failed while reading track {track_number} "
                    f"(exit {return_code})"
                )
        finally:
            if proc.returncode is None:
                proc.terminate()
                try:
                    await asyncio.wait_for(proc.wait(), timeout=2)
                except TimeoutError:
                    proc.kill()
                    await proc.wait()
