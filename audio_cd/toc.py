"""Parse exact audio CD track lengths from cdparanoia -Q."""

from __future__ import annotations

import re
from dataclasses import dataclass

_TRACK_RE = re.compile(r"^\s*(\d+)\.\s+(\d+)\s+\[(\d+):(\d{2})\.(\d{2})\]\s+\d+\s+\[\d+:\d{2}\.\d{2}\]")


@dataclass(frozen=True, slots=True)
class CDTrack:
    number: int
    sectors: int

    @property
    def duration(self) -> int:
        """Rounded duration for Music Assistant metadata."""
        return round(self.sectors / 75)


def parse_toc(output: str) -> list[CDTrack]:
    """Read the integer sector length, not the rounded display duration."""
    tracks: list[CDTrack] = []
    for line in output.splitlines():
        match = _TRACK_RE.match(line)
        if not match:
            continue
        number, sectors, minutes, seconds, frames = map(int, match.groups())
        if (
            number < 1
            or sectors < 1
            or seconds >= 60
            or frames >= 75
            or sectors != (minutes * 60 + seconds) * 75 + frames
            or (tracks and number <= tracks[-1].number)
        ):
            raise ValueError(f"Invalid CD track table row: {line.strip()}")
        tracks.append(CDTrack(number, sectors))
    return tracks
