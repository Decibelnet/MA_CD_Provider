"""Non-blocking wrapper around cdparanoia for live audio-CD reads.

No audio is written to disk: track bytes are streamed from the child process stdout.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncGenerator


from .drive_access import DriveAccessCoordinator


LOGGER = logging.getLogger(__name__)


class DriveError(RuntimeError):
    """Optical drive operation failed."""


class CDDrive:
    """Access one Linux optical drive through cdparanoia."""

    def __init__(
        self, device: str = "/dev/sr0",
        coordinator: DriveAccessCoordinator | None = None,
    ) -> None:
        self.device = device
        self._coordinator = coordinator

    async def query(self) -> str | None:
        """Return the TOC, or None if extraction owns the drive."""
        token = None
        if self._coordinator is not None:
            token = await self._coordinator.acquire_query()
            if token is None:
                return None
        startup = asyncio.create_task(asyncio.create_subprocess_exec(
            "cdparanoia", "-Q", "-d", self.device,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        ))
        communication = None
        try:
            proc = await asyncio.shield(startup)
            communication = asyncio.create_task(proc.communicate())
            output, _ = await asyncio.shield(communication)
            text = output.decode("utf-8", errors="replace")
            if proc.returncode != 0:
                raise DriveError(text.strip() or f"cdparanoia exited with {proc.returncode}")
            return text
        except FileNotFoundError as err:
            raise DriveError(
                "cdparanoia is not installed in the Music Assistant container"
            ) from err
        finally:
            cleanup = asyncio.create_task(
                self._finish_query(startup, communication, token)
            )
            await asyncio.shield(cleanup)

    async def _finish_query(self, startup, communication, token) -> None:
        """Keep ownership until startup and physical process cleanup finish."""
        try:
            proc = await startup
        except Exception:
            # Subprocess creation failed; there is no process to clean up.
            if token is not None:
                self._coordinator.release(token)
            return

        if communication is None:
            communication = asyncio.create_task(proc.communicate())
        if proc.returncode is None:
            try:
                proc.terminate()
            except ProcessLookupError:
                pass
            try:
                await asyncio.wait_for(asyncio.shield(communication), 2)
            except TimeoutError:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
        # Drain the pipes even after termination to avoid pipe backpressure.
        await communication
        await proc.wait()
        if token is not None:
            self._coordinator.release(token)

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
