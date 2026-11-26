"""Standalone manager tests with a controlled reader."""

import asyncio
import importlib
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

package = types.ModuleType("cd_manager_test")
package.__path__ = [str(Path(__file__).resolve().parents[1] / "audio_cd")]
sys.modules[package.__name__] = package
module = importlib.import_module("cd_manager_test.reader_manager")


class FakeReader:
    instances = []

    def __init__(self, device, tracks):
        assert all(r.closed for r in self.instances)
        self.instances.append(self)
        self.closed = False
        self.fail = False

    async def stream_track(self, number):
        if self.fail:
            raise module.SequentialReaderError("read failed")
        yield bytes([number]) * 176404

    async def close(self):
        await asyncio.sleep(0)
        self.closed = True


class ManagerTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        FakeReader.instances = []
        self.patcher = patch.object(module, "SequentialCDReader", FakeReader)
        self.patcher.start()
        self.addCleanup(self.patcher.stop)
        self.manager = module.CDReaderManager("/dev/test", [(n, 100) for n in range(1, 5)])
        self.addAsyncCleanup(self.manager.close)

    async def read(self, number, seek=0):
        return b"".join([c async for c in self.manager.stream_track(number, seek)])

    async def test_consecutive_reuse(self):
        await self.read(1)
        await self.read(2)
        await self.read(3)
        self.assertEqual(len(FakeReader.instances), 1)

    async def test_skip_backward_repeat(self):
        for number in (1, 3, 2, 2):
            await self.read(number)
        self.assertEqual(len(FakeReader.instances), 4)
        self.assertTrue(all(r.closed for r in FakeReader.instances[:-1]))

    async def test_seek_restarts_and_skips_pcm(self):
        await self.read(1)
        self.assertEqual(await self.read(2, 1), bytes([2]) * 4)
        self.assertEqual(len(FakeReader.instances), 2)

    async def test_failure_invalidates(self):
        await self.read(1)
        FakeReader.instances[0].fail = True
        with self.assertRaisesRegex(module.SequentialReaderError, "read failed"):
            await self.read(2)
        self.assertTrue(FakeReader.instances[0].closed)
        await self.read(3)
        self.assertEqual(len(FakeReader.instances), 2)

    async def test_concurrent_consecutive_requests(self):
        first, second = await asyncio.gather(self.read(1), self.read(2))
        self.assertEqual(first, bytes([1]) * 176404)
        self.assertEqual(second, bytes([2]) * 176404)
        self.assertEqual(len(FakeReader.instances), 1)

    async def test_close_prevents_new_requests(self):
        await self.read(1)
        await self.manager.close()
        self.assertTrue(FakeReader.instances[0].closed)
        with self.assertRaisesRegex(module.SequentialReaderError, "manager is closed"):
            await self.read(2)
