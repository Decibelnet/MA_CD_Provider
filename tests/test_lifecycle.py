"""Tests for standalone CD lifecycle coordination."""
import asyncio
import importlib
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

package = types.ModuleType("cd_lifecycle_test")
package.__path__ = [str(Path(__file__).resolve().parents[1] / "audio_cd")]
sys.modules[package.__name__] = package
module = importlib.import_module("cd_lifecycle_test.lifecycle")
CDLifecycle = module.CDLifecycle


class FakeManager:
    def __init__(self, device, tracks):
        self.closed = False

    async def stream_track(self, number, seek_position=0):
        yield b"pcm"

    async def close(self):
        self.closed = True


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.patcher = patch.object(module, "CDReaderManager", FakeManager)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)
        self.lifecycle = CDLifecycle("fake", [(1, 1), (2, 1)], idle_timeout=0.03)
        self.addAsyncCleanup(self.lifecycle.close)

    async def consume(self, number):
        return [chunk async for chunk in self.lifecycle.stream_track(number)]

    async def test_normal_completion_retains_then_expires(self):
        await self.consume(1)
        manager = self.lifecycle._manager
        self.assertFalse(manager.closed)
        await asyncio.wait_for(self.lifecycle._timer, 1)
        self.assertTrue(manager.closed)
        self.assertIsNone(self.lifecycle._manager)

    async def test_new_request_cancels_timer(self):
        await self.consume(1)
        manager = self.lifecycle._manager
        timer = self.lifecycle._timer
        stream = self.lifecycle.stream_track(2)
        await anext(stream)
        await asyncio.gather(timer, return_exceptions=True)
        self.assertIs(self.lifecycle._manager, manager)
        self.assertFalse(manager.closed)
        await stream.aclose()
        self.assertTrue(manager.closed)

    async def test_premature_exit_releases(self):
        stream = self.lifecycle.stream_track(1)
        await anext(stream)
        manager = self.lifecycle._manager
        await stream.aclose()
        self.assertTrue(manager.closed)
        self.assertIsNone(self.lifecycle._manager)

    async def test_other_consumer_prevents_release(self):
        first = self.lifecycle.stream_track(1)
        second = self.lifecycle.stream_track(2)
        await anext(first)
        await anext(second)
        manager = self.lifecycle._manager
        await first.aclose()
        self.assertFalse(manager.closed)
        self.assertIsNone(self.lifecycle._timer)
        await second.aclose()
        self.assertTrue(manager.closed)

    async def test_close_cancels_idle_timer(self):
        await self.consume(1)
        manager = self.lifecycle._manager
        timer = self.lifecycle._timer
        await self.lifecycle.close()
        await asyncio.gather(timer, return_exceptions=True)
        self.assertTrue(manager.closed)
        self.assertIsNone(self.lifecycle._manager)

    async def test_request_waits_for_expiry_cleanup(self):
        await self.consume(1)
        manager = self.lifecycle._manager
        entered = asyncio.Event()
        release = asyncio.Event()

        async def slow_close():
            entered.set()
            await release.wait()
            manager.closed = True

        manager.close = slow_close
        await asyncio.wait_for(entered.wait(), 1)
        request = asyncio.create_task(self.consume(2))
        try:
            await asyncio.sleep(0)
            self.assertFalse(request.done())
            self.assertIs(self.lifecycle._manager, manager)
        finally:
            release.set()
            await asyncio.wait_for(request, 1)
        self.assertTrue(manager.closed)
        self.assertIsNot(self.lifecycle._manager, manager)


class OwnershipTests(LifecycleTests):
    async def asyncSetUp(self):
        await super().asyncSetUp()
        self.gate = module.DriveAccessCoordinator()
        self.lifecycle = CDLifecycle(
            "fake", [(1, 1), (2, 1)], idle_timeout=0.03,
            coordinator=self.gate,
        )
        self.addAsyncCleanup(self.lifecycle.close)

    async def assert_available(self):
        token = await self.gate.acquire_query()
        self.assertIsNotNone(token)
        self.gate.release(token)

    async def test_ownership_retained_across_tracks_and_idle(self):
        await self.consume(1)
        token = self.lifecycle._ownership
        self.assertIsNotNone(token)
        self.assertIsNone(await self.gate.acquire_query())
        await self.consume(2)
        self.assertIs(self.lifecycle._ownership, token)
        self.assertIsNone(await self.gate.acquire_query())
        await asyncio.wait_for(self.lifecycle._timer, 1)
        await self.assert_available()

    async def test_premature_close_releases_ownership(self):
        stream = self.lifecycle.stream_track(1)
        await anext(stream)
        self.assertIsNone(await self.gate.acquire_query())
        await stream.aclose()
        await self.assert_available()

    async def test_shutdown_releases_ownership(self):
        await self.consume(1)
        await self.lifecycle.close()
        await self.assert_available()

    async def wait_for_acquisition(self):
        async with asyncio.timeout(1):
            while self.lifecycle._acquiring is None:
                await asyncio.sleep(0)

    async def test_reader_waits_for_query(self):
        token = await self.gate.acquire_query()
        request = asyncio.create_task(self.consume(1))
        await self.wait_for_acquisition()
        self.assertFalse(request.done())
        self.assertIsNone(self.lifecycle._manager)
        self.gate.release(token)
        self.assertEqual(await asyncio.wait_for(request, 1), [b"pcm"])
        self.assertIsNone(await self.gate.acquire_query())

    async def test_cancelled_waiter(self):
        token = await self.gate.acquire_query()
        request = asyncio.create_task(self.consume(1))
        await self.wait_for_acquisition()
        request.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await request
        self.assertIsNone(self.lifecycle._manager)
        self.gate.release(token)
        await self.assert_available()

    async def test_manager_creation_failure(self):
        with patch.object(module, "CDReaderManager", side_effect=RuntimeError("creation")):
            with self.assertRaisesRegex(RuntimeError, "creation"):
                await self.consume(1)
        await self.assert_available()

    async def test_cleanup_failure_retains_ownership(self):
        stream = self.lifecycle.stream_track(1)
        await anext(stream)
        manager = self.lifecycle._manager
        original_close = manager.close

        async def fail():
            raise RuntimeError("cleanup")

        manager.close = fail
        try:
            with self.assertRaisesRegex(RuntimeError, "cleanup"):
                await stream.aclose()
            self.assertIs(self.lifecycle._manager, manager)
            self.assertIsNone(await self.gate.acquire_query())
        finally:
            manager.close = original_close
        await self.lifecycle.close()
        await self.assert_available()

    async def test_shutdown_while_waiting(self):
        token = await self.gate.acquire_query()
        request = asyncio.create_task(self.consume(1))
        await self.wait_for_acquisition()
        await asyncio.wait_for(self.lifecycle.close(), 1)
        with self.assertRaisesRegex(module.SequentialReaderError, "Lifecycle is closed"):
            await request
        self.assertIsNone(self.lifecycle._manager)
        self.gate.release(token)
        await self.assert_available()
