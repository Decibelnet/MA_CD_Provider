# Extend the official Music Assistant server image with the CD reader
# and this provider. Keep the server version aligned with the HA app config.
FROM ghcr.io/music-assistant/server:2.10.13

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends cdparanoia \
    && rm -rf /var/lib/apt/lists/*

COPY audio_cd /tmp/audio_cd

RUN set -eux; \
    PROVIDERS_DIR="$(/app/venv/bin/python3 -c 'import pathlib, music_assistant; print(pathlib.Path(music_assistant.__file__).resolve().parent / "providers")')"; \
    install -d "${PROVIDERS_DIR}/audio_cd"; \
    cp -a /tmp/audio_cd/. "${PROVIDERS_DIR}/audio_cd/"; \
    rm -rf /tmp/audio_cd; \
    command -v cdparanoia
