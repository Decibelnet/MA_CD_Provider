# Music Assistant Audio CD Provider

A Music Assistant music provider for playing physical audio CDs directly from a locally attached optical drive.

## Goals

- Expose audio CD tracks through Music Assistant's normal music browsing and playback/queue experience.
- Stream digital PCM audio directly from the optical drive at playback time.
- Avoid permanent ripping or storing complete track files.
- Detect disc insertion and removal.
- Provide track metadata and album artwork where available.
- Support ejecting the disc.

## Development status

**Proof of concept: optical-drive access and raw PCM extraction verified. Provider implementation not yet complete.**

Test environment:

- Home Assistant OS
- Music Assistant server image: `ghcr.io/music-assistant/server:2.10.13`
- USB optical drive: Slimtype eBAU108
- Device nodes: `/dev/sr0` and `/dev/sg1`
- Linux reports 16 audio tracks on the test disc.
- `cdparanoia` successfully enumerated the disc and streamed raw CDDA PCM to stdout.

The test environment demonstrated that the drive can be accessed from a suitably configured Music Assistant container. Installing `cdparanoia` interactively in a running container is not persistent; a reproducible packaging approach is still required.

## Compatibility

The initial implementation should target the Music Assistant provider API supported by the selected server version. Compatibility with later Music Assistant releases will be assessed separately.

## Design principles

- Read audio from the physical disc on demand.
- Do not create permanent ripped audio files.
- Keep optical-drive operations isolated behind a small drive-access layer.
- Handle disc changes and read errors cleanly.
- Do not interfere with the user's existing Music Assistant installation or data.

## Planned work

1. Confirm the provider API and packaging approach for the target Music Assistant version.
2. Implement drive discovery, TOC reading, track enumeration, and eject.
3. Implement streaming of PCM audio from the selected track.
4. Add metadata and artwork lookup.
5. Handle disc insertion/removal and queue behaviour.
6. Add tests and installation documentation.

## Safety

This project is experimental. Test with a separate Music Assistant instance and back up configuration before changing a production installation.
