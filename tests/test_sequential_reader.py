"""Unit tests for the experimental sequential CD reader."""

import unittest

from audio_cd.sequential_reader import (
    PCM_BYTES_PER_SECTOR,
    SequentialReaderError,
    split_pcm_chunks,
)


class SplitPCMChunksTests(unittest.TestCase):
    def test_splits_at_track_boundary_across_arbitrary_chunks(self) -> None:
        first = b"a" * PCM_BYTES_PER_SECTOR
        second = b"b" * PCM_BYTES_PER_SECTOR
        # Deliberately split source chunks in the middle of sectors.
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
            list(split_pcm_chunks(
                [b"x" * (PCM_BYTES_PER_SECTOR + 1)], [(1, 1)]
            ))

    def test_rejects_non_increasing_track_numbers(self) -> None:
        with self.assertRaisesRegex(ValueError, "strictly increasing"):
            list(split_pcm_chunks([], [(2, 1), (2, 1)]))


if __name__ == "__main__":
    unittest.main()
