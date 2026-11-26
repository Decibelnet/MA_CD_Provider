"""Standalone metadata query ownership tests."""
import asyncio
import importlib
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import AsyncMock, patch

package = types.ModuleType("cd_drive_test")
package.__path__ = [str(Path(__file__).resolve().parents[1] / "audio_cd")]
sys.modules[package.__name__] = package
module = importlib.import_module("cd_drive_test.drive")


class FakeProcess:
    def __init__(self, code=0, blocked=False):
        self.code = code
        self.returncode = None
        self.ready = asyncio.Event()
        self.entered = asyncio.Event()
        self.terminated = asyncio.Event()
        self.defer_exit = False
        if not blocked:
            self.ready.set()

    async def communicate(self):
        self.entered.set()
        await self.ready.wait()
        if self.returncode is None:
            self.returncode = self.code
        return b"TOC", None

    def terminate(self):
        self.terminated.set()
        if not self.defer_exit:
            self.returncode = -15
            self.ready.set()

    def kill(self):
        self.returncode = -9
        self.ready.set()

    async def wait(self):
        await self.ready.wait()
        return self.returncode


class DriveTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.gate = module.DriveAccessCoordinator()
        self.drive = module.CDDrive("fake", coordinator=self.gate)
        self.spawn = AsyncMock()
        patcher = patch.object(module.asyncio, "create_subprocess_exec", self.spawn)
        patcher.start()
        self.addCleanup(patcher.stop)

    async def available(self):
        token = await asyncio.wait_for(self.gate.acquire_reader(), 1)
        self.gate.release(token)

    async def test_success(self):
        self.spawn.return_value = FakeProcess()
        self.assertEqual(await self.drive.query(), "TOC")
        await self.available()

    async def test_skip_during_extraction(self):
        token = await self.gate.acquire_reader()
        self.assertIsNone(await self.drive.query())
        self.spawn.assert_not_called()
        self.gate.release(token)

    async def test_queries_serialize(self):
        first = FakeProcess(blocked=True)
        second = FakeProcess()
        self.spawn.side_effect = [first, second]
        a = asyncio.create_task(self.drive.query())
        await asyncio.wait_for(first.entered.wait(), 1)
        b = asyncio.create_task(self.drive.query())
        await asyncio.sleep(0)
        self.assertEqual(self.spawn.call_count, 1)
        first.ready.set()
        self.assertEqual(await asyncio.wait_for(asyncio.gather(a, b), 1), ["TOC", "TOC"])
        await self.available()

    async def test_reader_waits(self):
        proc = FakeProcess(blocked=True)
        self.spawn.return_value = proc
        query = asyncio.create_task(self.drive.query())
        await asyncio.wait_for(proc.entered.wait(), 1)
        reader = asyncio.create_task(self.gate.acquire_reader())
        await asyncio.sleep(0)
        self.assertFalse(reader.done())
        proc.ready.set()
        await query
        token = await asyncio.wait_for(reader, 1)
        self.assertEqual(proc.returncode, 0)
        self.gate.release(token)

    async def test_nonzero_exit(self):
        self.spawn.return_value = FakeProcess(code=1)
        with self.assertRaises(module.DriveError):
            await self.drive.query()
        await self.available()

    async def test_missing_executable(self):
        self.spawn.side_effect = FileNotFoundError()
        with self.assertRaises(module.DriveError):
            await self.drive.query()
        await self.available()

    async def test_cancel_query(self):
        proc = FakeProcess(blocked=True)
        self.spawn.return_value = proc
        query = asyncio.create_task(self.drive.query())
        await asyncio.wait_for(proc.entered.wait(), 1)
        query.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await query
        self.assertEqual(proc.returncode, -15)
        await self.available()

    async def test_cancel_startup(self):
        proc = FakeProcess(blocked=True)
        entered = asyncio.Event()
        proceed = asyncio.Event()

        async def spawn(*args, **kwargs):
            entered.set()
            await proceed.wait()
            return proc

        self.spawn.side_effect = spawn
        query = asyncio.create_task(self.drive.query())
        await asyncio.wait_for(entered.wait(), 1)
        query.cancel()
        await asyncio.sleep(0)
        self.assertFalse(query.done())
        proceed.set()
        with self.assertRaises(asyncio.CancelledError):
            await query
        self.assertEqual(proc.returncode, -15)
        await self.available()

    async def test_repeated_cancellation(self):
        proc = FakeProcess(blocked=True)
        proc.defer_exit = True
        self.spawn.return_value = proc
        query = asyncio.create_task(self.drive.query())
        await asyncio.wait_for(proc.entered.wait(), 1)
        query.cancel()
        await asyncio.wait_for(proc.terminated.wait(), 1)
        query.cancel()
        with self.assertRaises(asyncio.CancelledError):
            await query
        reader = asyncio.create_task(self.gate.acquire_reader())
        await asyncio.sleep(0)
        self.assertFalse(reader.done())
        proc.returncode = -15
        proc.ready.set()
        token = await asyncio.wait_for(reader, 1)
        self.gate.release(token)

    async def test_cleanup_failure_retains_ownership(self):
        proc = FakeProcess(blocked=True)
        self.spawn.return_value = proc

        def fail():
            raise RuntimeError("cleanup failed")

        proc.terminate = fail
        query = asyncio.create_task(self.drive.query())
        await asyncio.wait_for(proc.entered.wait(), 1)
        query.cancel()
        with self.assertRaisesRegex(RuntimeError, "cleanup failed"):
            await query
        reader = asyncio.create_task(self.gate.acquire_reader())
        await asyncio.sleep(0)
        self.assertFalse(reader.done())
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)
        proc.ready.set()
