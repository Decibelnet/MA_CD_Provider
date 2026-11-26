"""Unit tests for the experimental sequential CD reader."""

import asyncio
import importlib.util
import sys
import unittest
from pathlib import Path
from unittest.mock import patch

# Load the standalone module directly. Importing audio_cd.sequential_reader
# would first execute audio_cd/__init__.py, which depends on the full Music
# Assistant runtime. These tests intentionally exercise only the reader module.
MODULE_PATH = Path(__file__).resolve().parents[1] / "audio_cd" / "sequential_reader.py"
SPEC = importlib.util.spec_from_file_location("audio_cd_sequential_reader_test", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise ImportError(f"Cannot load sequential reader module from {MODULE_PATH}")
sequential_reader = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = sequential_reader
SPEC.loader.exec_module(sequential_reader)

PCM_BYTES_PER_SECTOR = sequential_reader.PCM_BYTES_PER_SECTOR
SequentialReaderError = sequential_reader.SequentialReaderError
SequentialCDReader = sequential_reader.SequentialCDReader
split_pcm_chunks = sequential_reader.split_pcm_chunks


class SplitPCMChunksTests(unittest.TestCase):
    def test_splits_at_track_boundary_across_arbitrary_chunks(self) -> None:
        first = b"a" * PCM_BYTES_PER_SECTOR
        second = b"b" * PCM_BYTES_PER_SECTOR
        chunks = [first[:1000], first[1000:] + second[:700], second[700:]]
        result = list(split_pcm_chunks(chunks, [(1, 1), (2, 1)]))
        self.assertEqual(
            [(chunk.track_number, len(chunk.pcm)) for chunk in result],
            [(1, 1000), (1, PCM_BYTES_PER_SECTOR - 1000), (2, 700),
             (2, PCM_BYTES_PER_SECTOR - 700)],
        )
        self.assertEqual(b"".join(c.pcm for c in result if c.track_number == 1), first)
        self.assertEqual(b"".join(c.pcm for c in result if c.track_number == 2), second)

    def test_rejects_short_read(self) -> None:
        with self.assertRaisesRegex(SequentialReaderError, "ended before"):
            list(split_pcm_chunks([b"x" * 100], [(1, 1)]))

    def test_rejects_extra_audio(self) -> None:
        with self.assertRaisesRegex(SequentialReaderError, "more PCM"):
            list(split_pcm_chunks([b"x" * (PCM_BYTES_PER_SECTOR + 1)], [(1, 1)]))

    def test_rejects_non_increasing_track_numbers(self) -> None:
        with self.assertRaisesRegex(ValueError, "strictly increasing"):
            list(split_pcm_chunks([], [(2, 1), (2, 1)]))


class _FakeStdout:
    def __init__(self, pcm: bytes, chunk_size: int) -> None:
        self._pcm = pcm
        self._chunk_size = chunk_size
        self._offset = 0

    async def read(self, requested: int) -> bytes:
        size = min(requested, self._chunk_size, len(self._pcm) - self._offset)
        if size <= 0:
            return b""
        chunk = self._pcm[self._offset : self._offset + size]
        self._offset += size
        return chunk


class _FakeProcess:
    def __init__(self, pcm: bytes, chunk_size: int) -> None:
        self.stdout = _FakeStdout(pcm, chunk_size)
        self.returncode = None
        self.terminated = False

    async def wait(self) -> int:
        self.returncode = 0
        return 0

    def terminate(self) -> None:
        self.terminated = True
        self.returncode = 0

    def kill(self) -> None:
        self.terminated = True
        self.returncode = -9


class SequentialCDReaderTests(unittest.IsolatedAsyncioTestCase):
    async def test_reads_tracks_from_one_process_and_routes_exact_pcm(self) -> None:
        track1 = b"1" * PCM_BYTES_PER_SECTOR
        track2 = b"2" * PCM_BYTES_PER_SECTOR
        process = _FakeProcess(track1 + track2, chunk_size=777)
        command_calls = []

        async def fake_create_subprocess_exec(*args, **kwargs):
            command_calls.append((args, kwargs))
            return process

        reader = SequentialCDReader(
            "/dev/test", [(1, 1), (2, 1)], buffer_bytes=2048, chunk_size=1024
        )
        try:
            with patch.object(asyncio, "create_subprocess_exec", fake_create_subprocess_exec):
                actual1 = b"".join([chunk async for chunk in reader.stream_track(1)])
                actual2 = b"".join([chunk async for chunk in reader.stream_track(2)])
            self.assertEqual(actual1, track1)
            self.assertEqual(actual2, track2)
            self.assertEqual(len(command_calls), 1)
            self.assertIn("1-2", command_calls[0][0])
        finally:
            await reader.close()

    async def test_can_start_read_at_requested_track(self) -> None:
        track2 = b"2" * PCM_BYTES_PER_SECTOR
        process = _FakeProcess(track2, chunk_size=1024)
        command_calls = []

        async def fake_create_subprocess_exec(*args, **kwargs):
            command_calls.append(args)
            return process

        reader = SequentialCDReader(
            "/dev/test", [(1, 1), (2, 1)], buffer_bytes=2048, chunk_size=1024
        )
        try:
            with patch.object(asyncio, "create_subprocess_exec", fake_create_subprocess_exec):
                actual = b"".join([chunk async for chunk in reader.stream_track(2)])
            self.assertEqual(actual, track2)
            self.assertIn("2-2", command_calls[0])
        finally:
            await reader.close()


    async def test_close_wakes_blocked_consumers(self) -> None:
        reading = asyncio.Event()

        class BlockingStdout:
            async def read(self, requested: int) -> bytes:
                reading.set()
                await asyncio.Future()
                return b""

        process = _FakeProcess(b"", chunk_size=1024)
        process.stdout = BlockingStdout()
        reader = SequentialCDReader("/dev/test", [(1, 1), (2, 1)])
        streams = [reader.stream_track(number) for number in (1, 2)]
        tasks = []
        try:
            with patch.object(asyncio, "create_subprocess_exec", return_value=process):
                tasks = [asyncio.create_task(anext(stream)) for stream in streams]
                await asyncio.wait_for(reading.wait(), timeout=2)
                self.assertEqual(reader._consumers, {1, 2})
                await asyncio.wait_for(reader.close(), timeout=2)
                for task in tasks:
                    with self.assertRaisesRegex(SequentialReaderError, "Reader is closed"):
                        await asyncio.wait_for(task, timeout=2)
                self.assertTrue(process.terminated)
        finally:
            await reader.close()
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            for stream in streams:
                await stream.aclose()

    async def test_close_with_full_queue(self) -> None:
        process = _FakeProcess(b"x" * PCM_BYTES_PER_SECTOR, chunk_size=512)
        reader = SequentialCDReader(
            "/dev/test", [(1, 1)], buffer_bytes=512, chunk_size=512
        )
        stream = reader.stream_track(1)
        try:
            with patch.object(asyncio, "create_subprocess_exec", return_value=process):
                self.assertEqual(await asyncio.wait_for(anext(stream), timeout=2), b"x" * 512)
                # The producer needs a turn to refill the bounded queue.
                for _ in range(100):
                    if reader._queues[1].full():
                        break
                    await asyncio.sleep(0.001)
                self.assertTrue(reader._queues[1].full())
                await asyncio.wait_for(reader.close(), timeout=2)
                with self.assertRaisesRegex(SequentialReaderError, "Reader is closed"):
                    await asyncio.wait_for(anext(stream), timeout=2)
                self.assertTrue(process.terminated)
                self.assertTrue(reader._producer.done())
        finally:
            await reader.close()
            await stream.aclose()


if __name__ == "__main__":
    unittest.main()
