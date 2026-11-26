"""Tests for standalone optical drive ownership."""
import asyncio
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location(
    "drive_access_test",
    Path(__file__).resolve().parents[1] / "audio_cd" / "drive_access.py",
)
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)
DriveAccessCoordinator = module.DriveAccessCoordinator


class DriveAccessTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.gate = DriveAccessCoordinator()

    async def test_query_available(self):
        token = await self.gate.acquire_query()
        self.assertIsNotNone(token)
        self.gate.release(token)
        token = await self.gate.acquire_query()
        self.assertIsNotNone(token)
        self.gate.release(token)

    async def test_query_skips_reader(self):
        reader = await self.gate.acquire_reader()
        self.assertIsNone(await self.gate.acquire_query())
        self.gate.release(reader)
        query = await self.gate.acquire_query()
        self.assertIsNotNone(query)
        self.gate.release(query)

    async def check_waiter(self, owner, acquire):
        task = asyncio.create_task(acquire())
        try:
            await asyncio.sleep(0)
            self.assertFalse(task.done())
            self.gate.release(owner)
            token = await asyncio.wait_for(task, 1)
            self.assertIsNotNone(token)
            self.gate.release(token)
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def test_queries_serialize(self):
        await self.check_waiter(
            await self.gate.acquire_query(), self.gate.acquire_query
        )

    async def test_reader_waits_for_query(self):
        await self.check_waiter(
            await self.gate.acquire_query(), self.gate.acquire_reader
        )

    async def test_readers_serialize(self):
        await self.check_waiter(
            await self.gate.acquire_reader(), self.gate.acquire_reader
        )

    async def test_stale_release_rejected(self):
        old = await self.gate.acquire_reader()
        self.gate.release(old)
        current = await self.gate.acquire_reader()
        with self.assertRaises(RuntimeError):
            self.gate.release(old)
        self.assertIsNone(await self.gate.acquire_query())
        self.gate.release(current)
        with self.assertRaises(RuntimeError):
            self.gate.release(current)

    async def test_cancelled_waiters_do_not_claim(self):
        owner = await self.gate.acquire_query()
        tasks = [
            asyncio.create_task(self.gate.acquire_reader()),
            asyncio.create_task(self.gate.acquire_query()),
        ]
        await asyncio.sleep(0)
        for task in tasks:
            self.assertFalse(task.done())
            task.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        self.assertTrue(all(isinstance(r, asyncio.CancelledError) for r in results))
        self.gate.release(owner)
        token = await asyncio.wait_for(self.gate.acquire_reader(), 1)
        self.gate.release(token)

    async def test_competing_requests_never_overlap(self):
        active = 0
        completed = 0

        async def worker(acquire):
            nonlocal active, completed
            token = await acquire()
            if token is None:
                return
            try:
                active += 1
                self.assertEqual(active, 1)
                await asyncio.sleep(0)
                completed += 1
            finally:
                active -= 1
                self.gate.release(token)

        await asyncio.wait_for(asyncio.gather(*(
            worker(self.gate.acquire_reader if i % 2 else self.gate.acquire_query)
            for i in range(20)
        )), 2)
        self.assertEqual(active, 0)
        self.assertGreaterEqual(completed, 10)
