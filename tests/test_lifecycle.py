"""Tests for standalone CD lifecycle coordination."""
import asyncio
import unittest
from unittest.mock import patch

from audio_cd.lifecycle import CDLifecycle


class FakeManager:
    def __init__(self, device, tracks):
        self.closed = False

    async def stream_track(self, number, seek_position=0):
        yield b"pcm"

    async def close(self):
        self.closed = True


class LifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.patcher = patch("audio_cd.lifecycle.CDReaderManager", FakeManager)
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
