"""Music Assistant provider for an audio CD in a local Linux optical drive."""

from __future__ import annotations

import asyncio
import hashlib
import re
import time
from collections.abc import AsyncGenerator, Sequence

from music_assistant_models.enums import (
    ContentType,
    MediaType,
    ProviderFeature,
    StreamType,
)
from music_assistant_models.media_items import (
    AudioFormat,
    BrowseFolder,
    ProviderMapping,
    Track,
)
from music_assistant_models.streamdetails import StreamDetails
from music_assistant.models.music_provider import MusicProvider

from .drive import CDDrive, DriveError

SUPPORTED_FEATURES = {ProviderFeature.BROWSE}
_TIME_RE = re.compile(r"\[(\d+):(\d{2})\.(\d{2})\]")
_DISC_CACHE_SECONDS = 5.0


def _time_to_seconds(value: str) -> int:
    minutes, remainder = value.split(":")
    seconds, frames = remainder.split(".")
    return int(minutes) * 60 + int(seconds) + round(int(frames) / 75)


def _parse_tracks(output: str) -> list[tuple[int, int]]:
    """Parse track number and duration from cdparanoia -Q output."""
    tracks: list[tuple[int, int]] = []
    for line in output.splitlines():
        match = re.match(r"^\s*(\d+)\.", line)
        if not match:
            continue
        times = _TIME_RE.findall(line)
        if len(times) < 2:
            continue
        start = _time_to_seconds(":".join(times[0][:2]) + "." + times[0][2])
        end = _time_to_seconds(":".join(times[1][:2]) + "." + times[1][2])
        duration = max(0, end - start)
        if duration:
            tracks.append((int(match.group(1)), duration))
    return tracks


async def setup(mass, manifest, config):
    """Set up the audio CD provider."""
    provider = AudioCDProvider(mass, manifest, config, SUPPORTED_FEATURES)
    # Allow setup with an empty drive; the disc may be inserted later.
    try:
        await provider._refresh_disc()
    except DriveError:
        provider._disc_id = None
        provider._tracks = []
    return provider


class AudioCDProvider(MusicProvider):
    """Expose the currently inserted audio CD as temporary browseable tracks."""

    def __init__(self, mass, manifest, config, supported_features):
        super().__init__(mass, manifest, config, supported_features)
        self.drive = CDDrive("/dev/sr0")
        self._disc_id: str | None = None
        self._tracks: list[tuple[int, int]] = []
        self._disc_checked_at = 0.0
        self._disc_refresh_lock = asyncio.Lock()

    @property
    def is_streaming_provider(self) -> bool:
        """CD tracks are transient and must never be cached as a local library."""
        return True

    async def _refresh_disc(self, *, force: bool = False) -> None:
        """Refresh disc metadata, reusing a recent successful check when safe."""
        now = time.monotonic()
        if (
            not force
            and self._disc_checked_at
            and now - self._disc_checked_at < _DISC_CACHE_SECONDS
        ):
            return

        async with self._disc_refresh_lock:
            # Another coroutine may have refreshed the disc while we waited.
            now = time.monotonic()
            if (
                not force
                and self._disc_checked_at
                and now - self._disc_checked_at < _DISC_CACHE_SECONDS
            ):
                return

            try:
                output = await self.drive.query()
                tracks = _parse_tracks(output)
                if not tracks:
                    raise DriveError("No audio tracks were found on the inserted disc")
            except DriveError:
                self._disc_id = None
                self._tracks = []
                self._disc_checked_at = 0.0
                raise

            self._disc_id = hashlib.sha256(output.encode()).hexdigest()[:12]
            self._tracks = tracks
            self._disc_checked_at = time.monotonic()

    async def get_track(self, prov_track_id: str) -> Track:
        """Return full details for a queued CD track, if its disc is still present."""
        try:
            disc_id, track_text = prov_track_id.rsplit(":", 1)
            track_number = int(track_text)
        except (ValueError, AttributeError) as err:
            raise DriveError(f"Invalid CD track id: {prov_track_id}") from err

        await self._refresh_disc()
        if disc_id != self._disc_id:
            raise DriveError("The disc has changed since this track was added to the queue")
        duration = dict(self._tracks).get(track_number)
        if duration is None:
            raise DriveError(f"Track {track_number} is not present on the current disc")

        return Track(
            item_id=prov_track_id,
            provider=self.instance_id,
            name=f"Track {track_number:02d}",
            duration=duration,
            track_number=track_number,
            provider_mappings={
                ProviderMapping(
                    item_id=prov_track_id,
                    provider_domain=self.domain,
                    provider_instance=self.instance_id,
                    available=True,
                    audio_format=AudioFormat(
                        content_type=ContentType.PCM_S16LE,
                        codec_type=ContentType.PCM_S16LE,
                        sample_rate=44100,
                        bit_depth=16,
                        channels=2,
                    ),
                )
            },
        )

    async def browse(self, path: str) -> Sequence[Track | BrowseFolder]:
        """List the tracks on the disc currently in the drive."""
        try:
            # Browsing explicitly checks the drive so inserted/removed discs show promptly.
            await self._refresh_disc(force=True)
        except DriveError:
            return []
        return [
            Track(
                item_id=f"{self._disc_id}:{number}",
                provider=self.instance_id,
                name=f"Track {number:02d}",
                duration=duration,
                track_number=number,
                provider_mappings={
                    ProviderMapping(
                        item_id=f"{self._disc_id}:{number}",
                        provider_domain=self.domain,
                        provider_instance=self.instance_id,
                        available=True,
                        audio_format=AudioFormat(
                            content_type=ContentType.PCM_S16LE,
                            codec_type=ContentType.PCM_S16LE,
                            sample_rate=44100,
                            bit_depth=16,
                            channels=2,
                        ),
                    )
                },
            )
            for number, duration in self._tracks
        ]

    async def get_stream_details(
        self, item_id: str, media_type: MediaType
    ) -> StreamDetails:
        """Describe the raw PCM stream for one CD track."""
        try:
            disc_id, track_text = item_id.rsplit(":", 1)
            track_number = int(track_text)
        except (ValueError, AttributeError) as err:
            raise DriveError(f"Invalid CD track id: {item_id}") from err

        await self._refresh_disc()
        if disc_id != self._disc_id:
            raise DriveError("The disc has changed since this track was added to the queue")
        duration = dict(self._tracks).get(track_number)
        if duration is None:
            raise DriveError(f"Track {track_number} is not present on the current disc")

        return StreamDetails(
            provider=self.instance_id,
            item_id=item_id,
            audio_format=AudioFormat(
                content_type=ContentType.PCM_S16LE,
                codec_type=ContentType.PCM_S16LE,
                sample_rate=44100,
                bit_depth=16,
                channels=2,
            ),
            media_type=MediaType.TRACK,
            stream_type=StreamType.CUSTOM,
            duration=duration,
            allow_seek=True,
            can_seek=True,
            data={"disc_id": disc_id, "track_number": track_number},
        )

    async def get_audio_stream(
        self, streamdetails: StreamDetails, seek_position: int = 0
    ) -> AsyncGenerator[bytes]:
        """Read PCM from the drive without creating an audio file."""
        data = streamdetails.data or {}
        await self._refresh_disc()
        if data.get("disc_id") != self._disc_id:
            raise DriveError("The disc has changed; refusing to stream the queued track")
        track_number = int(data["track_number"])
        bytes_to_skip = max(0, seek_position) * 44100 * 2 * 2
        async for chunk in self.drive.stream_track(track_number):
            if bytes_to_skip:
                skipped = min(bytes_to_skip, len(chunk))
                chunk = chunk[skipped:]
                bytes_to_skip -= skipped
                if not chunk:
                    continue
            yield chunk
