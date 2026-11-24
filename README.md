# Music Assistant Audio CD Provider

A Music Assistant music provider for playing physical audio CDs directly from a locally attached optical drive.

## Goals

- Expose the inserted audio CD as temporary tracks in Music Assistant.
- Stream digital PCM audio from the optical drive when requested.
- Avoid permanent ripping or storing complete track files.
- Reject queued tracks if the disc has changed.
- Add metadata, artwork, insertion/removal notifications and eject support in follow-up milestones.

## Current status

**Early provider implementation and image packaging are in progress; playback has not yet been validated through Music Assistant.**

Verified on the target Home Assistant OS system:

- Music Assistant server: `ghcr.io/music-assistant/server:2.10.13`
- USB optical drive: Slimtype eBAU108
- Device nodes: `/dev/sr0` and `/dev/sg1`
- Linux detects a 16-track audio CD.
- `cdparanoia` successfully enumerates tracks and emits raw CDDA PCM to stdout.
- The initial provider code exposes tracks and supplies raw PCM through Music Assistant's custom-stream interface.

## Build

The Dockerfile extends the official Music Assistant 2.10.13 image, installs the system package `cdparanoia`, and copies the provider into Music Assistant's installed provider directory.

GitHub Actions builds and publishes multi-architecture images to:

`ghcr.io/decibelnet/music-assistant-cd`

The `main` branch publishes the tags `2.10.13` and `latest`. GitHub Container Registry packages are private by default in some repository configurations; set the package visibility to **Public** before using the image from Home Assistant OS without registry credentials.

## Home Assistant OS app

Use the custom image below in the local Music Assistant app's `config.yaml` after the GHCR image has been published and made public:

```yaml
image: ghcr.io/decibelnet/music-assistant-cd:2.10.13
```

Keep the existing custom app slug, data directory, device mappings and permissions unchanged. In particular, preserve access to `/dev/sr0` and `/dev/sg1`. Do not replace or remove the original Music Assistant app or its data.

## Known limitations

- The provider currently labels items as Track 01, Track 02, etc.
- Album/track metadata and artwork lookup are not implemented yet.
- Disc insertion/removal notifications and eject are not implemented yet.
- Seeking currently reads from the start of a track and discards the requested initial PCM bytes; this is correct but not yet optimized.
- The provider has not yet been tested end-to-end through the Music Assistant queue and player pipeline.

## Design principles

- Read audio from the physical disc on demand.
- Do not create permanent ripped audio files.
- Keep optical-drive operations isolated behind a small drive-access layer.
- Handle disc changes and read errors cleanly.
- Test with the separate custom app and preserve the original installation and data.
