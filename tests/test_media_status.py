"""Tests for standalone Linux media status observation."""
import asyncio
import importlib
import sys
import threading
import types
import unittest
from pathlib import Path
from unittest.mock import patch

package = types.ModuleType("cd_media_status_test")
package.__path__ = [str(Path(__file__).resolve().parents[1] / "audio_cd")]
sys.modules[package.__name__] = package
media = importlib.import_module("cd_media_status_test.media_status")


class MediaStatusTests(unittest.IsolatedAsyncioTestCase):
    async def test_present_and_changed(self):
        with patch.object(media.os, "open", return_value=42) as opened, \
             patch.object(media.os, "close") as closed, \
             patch.object(media.fcntl, "ioctl", side_effect=[4, 1]) as ioctl:
            result = await media.MediaStatus().check()
        self.assertEqual(result, media.MediaObservation(media.DriveStatus.DISC_PRESENT, True))
        opened.assert_called_once_with("/dev/sr0", media.os.O_RDONLY | media.os.O_NONBLOCK)
        closed.assert_called_once_with(42)
        self.assertEqual([call.args for call in ioctl.call_args_list], [
            (42, media.CDROM_DRIVE_STATUS, media.CDSL_CURRENT),
            (42, media.CDROM_MEDIA_CHANGED, media.CDSL_CURRENT),
        ])

    async def test_status_values(self):
        for raw, expected in [(0, media.DriveStatus.UNKNOWN),
                              (1, media.DriveStatus.NO_DISC),
                              (2, media.DriveStatus.TRAY_OPEN),
                              (3, media.DriveStatus.NOT_READY),
                              (4, media.DriveStatus.DISC_PRESENT),
                              (99, media.DriveStatus.UNKNOWN)]:
            with self.subTest(raw=raw), \
                 patch.object(media.os, "open", return_value=42), \
                 patch.object(media.os, "close"), \
                 patch.object(media.fcntl, "ioctl", side_effect=[raw, 0]):
                self.assertEqual(await media.MediaStatus().check(),
                                 media.MediaObservation(expected, False))

    async def test_unknown_change_indicator(self):
        with patch.object(media.os, "open", return_value=42), \
             patch.object(media.os, "close"), \
             patch.object(media.fcntl, "ioctl", side_effect=[4, 7]):
            result = await media.MediaStatus().check()
        self.assertIsNone(result.changed)

    async def test_open_failure(self):
        with patch.object(media.os, "open", side_effect=OSError("open failed")), \
             patch.object(media.os, "close") as closed:
            result = await media.MediaStatus().check()
        self.assertEqual(result.status, media.DriveStatus.UNKNOWN)
        self.assertIsNone(result.changed)
        self.assertEqual(result.error, "open failed")
        closed.assert_not_called()

    async def test_ioctl_failure_closes_device(self):
        with patch.object(media.os, "open", return_value=42), \
             patch.object(media.os, "close") as closed, \
             patch.object(media.fcntl, "ioctl", side_effect=[4, OSError("unsupported")]):
            result = await media.MediaStatus().check()
        self.assertEqual(result.status, media.DriveStatus.DISC_PRESENT)
        self.assertIsNone(result.changed)
        self.assertEqual(result.error, "unsupported")
        closed.assert_called_once_with(42)

    async def test_serialized_checks(self):
        observer = media.MediaStatus()
        first = media.MediaObservation(media.DriveStatus.DISC_PRESENT, True)
        second = media.MediaObservation(media.DriveStatus.DISC_PRESENT, False)
        entered = asyncio.Event()
        release = threading.Event()
        loop = asyncio.get_running_loop()
        calls = []

        def read(device):
            calls.append(device)
            if len(calls) == 1:
                loop.call_soon_threadsafe(entered.set)
                if not release.wait(5):
                    raise RuntimeError("worker timed out")
                return first
            return second

        with patch.object(media, "_read_media", side_effect=read):
            a = asyncio.create_task(observer.check())
            b = None
            try:
                await asyncio.wait_for(entered.wait(), 5)
                b = asyncio.create_task(observer.check())
                await asyncio.sleep(0)
                self.assertFalse(b.done())
                self.assertEqual(len(calls), 1)
                release.set()
                self.assertEqual(await a, first)
                self.assertEqual(await b, second)
                self.assertEqual(len(calls), 2)
            finally:
                release.set()
                await asyncio.gather(*[t for t in (a, b) if t is not None],
                                     return_exceptions=True)

    async def test_cancelled_check_preserves_notification(self):
        observer = media.MediaStatus()
        first = media.MediaObservation(media.DriveStatus.DISC_PRESENT, True)
        second = media.MediaObservation(media.DriveStatus.DISC_PRESENT, False)
        entered = asyncio.Event()
        release = threading.Event()
        loop = asyncio.get_running_loop()
        calls = []

        def read(device):
            calls.append(device)
            if len(calls) == 1:
                loop.call_soon_threadsafe(entered.set)
                if not release.wait(5):
                    raise RuntimeError("worker timed out")
                return first
            return second

        with patch.object(media, "_read_media", side_effect=read):
            a = asyncio.create_task(observer.check())
            b = None
            try:
                await asyncio.wait_for(entered.wait(), 5)
                a.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await a
                b = asyncio.create_task(observer.check())
                await asyncio.sleep(0)
                self.assertFalse(b.done())
                self.assertEqual(len(calls), 1)
                release.set()
                self.assertEqual(await b, media.MediaObservation(
                    media.DriveStatus.DISC_PRESENT, True, retained=True
                ))
                self.assertEqual(await observer.check(), second)
                self.assertEqual(len(calls), 2)
            finally:
                release.set()
                await asyncio.gather(*[t for t in (a, b) if t is not None],
                                     return_exceptions=True)


    async def test_retained_observation_after_repeated_cancellation(self):
        observer = media.MediaStatus()
        old = media.MediaObservation(media.DriveStatus.DISC_PRESENT, False)
        fresh = media.MediaObservation(media.DriveStatus.NO_DISC, True)
        entered = asyncio.Event()
        release = threading.Event()
        loop = asyncio.get_running_loop()
        calls = []

        def read(device):
            calls.append(device)
            if len(calls) == 1:
                loop.call_soon_threadsafe(entered.set)
                if not release.wait(5):
                    raise RuntimeError("worker timed out")
                return old
            return fresh

        with patch.object(media, "_read_media", side_effect=read):
            a = asyncio.create_task(observer.check())
            b = None
            try:
                await asyncio.wait_for(entered.wait(), 5)
                a.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await a
                b = asyncio.create_task(observer.check())
                await asyncio.sleep(0)
                b.cancel()
                with self.assertRaises(asyncio.CancelledError):
                    await b
                self.assertEqual(len(calls), 1)
                release.set()
                self.assertEqual(await observer.check(), media.MediaObservation(
                    media.DriveStatus.DISC_PRESENT, False, retained=True
                ))
                self.assertEqual(await observer.check(), fresh)
                self.assertEqual(len(calls), 2)
            finally:
                release.set()
                await asyncio.gather(*[t for t in (a, b) if t is not None],
                                     return_exceptions=True)
