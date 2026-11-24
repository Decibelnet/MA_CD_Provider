# Architecture and implementation plan

## Scope

The provider targets physical Red Book audio CDs attached to the host running Music Assistant. Track audio is read from the disc when requested and streamed to Music Assistant. The provider must not rip tracks to a permanent audio library.

## Target compatibility

The first implementation targets the Music Assistant server provider API at tag `2.10.13`, matching the user's isolated test instance. Do not assume APIs from newer releases are available. Validate imports and model signatures against that tag before considering the implementation complete.

## Components

### 1. Drive access layer

Responsibilities:

- Locate and open the configured optical drive (initially `/dev/sr0`).
- Read the CD table of contents and audio track boundaries.
- Determine whether media is present and detect disc changes.
- Eject the disc on explicit request.
- Run blocking optical-drive operations off the asyncio event loop.
- Close file descriptors and terminate child processes on cancellation or unload.

The proof of concept established that `/dev/sr0` is accessible in the isolated container, that the Linux CD-ROM TOC ioctl reports tracks, and that `cdparanoia` can enumerate the test disc and emit raw CDDA PCM to stdout.

### 2. Music Assistant provider

The provider should expose the currently inserted disc as a browseable album/folder with tracks. Disc-derived items should be ephemeral rather than synchronised into the persistent MA library by default.

Playback should use the MA provider's custom-stream mechanism, with stream details describing the actual PCM format. The stream generator should read only the requested track/segment from the drive and yield PCM bytes. Seeking must either be implemented accurately or reported as unsupported.

### 3. Metadata and artwork

Track titles, artist, album, year, and artwork may be looked up using a disc identifier derived from the TOC (for example, a MusicBrainz Disc ID). Metadata lookup is optional and must not prevent playback when the network is unavailable or the disc is unknown. Track number and duration from the TOC are the local fallback.

### 4. Disc lifecycle

Insertion/removal should invalidate stale track metadata and make unavailable tracks unavailable to subsequent playback requests. A playback request must verify that the expected disc is still present; it must not silently play a different disc inserted after queueing.

### 5. Packaging

The interactive installation of `cdparanoia` in the running test container is temporary. The final deployment must use a reproducible way to provide the required executable/library, such as a maintained custom image or a supported provider dependency. Do not rely on manual package installation surviving a container recreation.

## Safety and reliability requirements

- Never write full track audio to permanent storage.
- Use bounded buffering; do not read an entire track into memory.
- Do not block the asyncio event loop with subprocess or device I/O.
- Handle cancellation, device busy errors, missing media, and disc swaps.
- Keep the original Music Assistant app and its data separate from the experimental instance.
- Do not claim support for multi-drive selection until it is implemented and tested.

## Validation checklist

- [x] Host detects the USB optical drive and audio CD.
- [x] Isolated container can open `/dev/sr0`.
- [x] Linux TOC ioctl succeeds and reports the test disc's tracks.
- [x] `cdparanoia` enumerates all audio tracks on the test disc.
- [x] Raw PCM can be streamed to stdout without creating a ripped file.
- [ ] Provider imports successfully on Music Assistant 2.10.13.
- [ ] Disc appears in Music Assistant browse UI.
- [ ] Track metadata and durations are correct.
- [ ] PCM playback works through a normal MA player queue.
- [ ] Pause, stop, seek behaviour is correct and accurately advertised.
- [ ] Disc removal and replacement invalidate stale queue items.
- [ ] Eject works safely.
- [ ] Deployment survives a container recreation without manual changes.
