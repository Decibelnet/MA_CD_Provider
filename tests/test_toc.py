"""Standalone tests for the CD TOC parser."""

import importlib.util
import sys
import unittest
from pathlib import Path

MODULE_PATH = Path(__file__).resolve().parents[1] / "audio_cd" / "toc.py"
SPEC = importlib.util.spec_from_file_location("audio_cd_toc_test", MODULE_PATH)
if SPEC is None or SPEC.loader is None:
    raise ImportError(f"Cannot load TOC parser from {MODULE_PATH}")
toc = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = toc
SPEC.loader.exec_module(toc)


class TOCTests(unittest.TestCase):
    def test_exact_sectors_and_rounded_durations(self):
        output = """
Table of contents (audio tracks only):
  1.     150 [00:02.00]        0 [00:00.00]    no   no  2
  2.     151 [00:02.01]      150 [00:02.00]    no   no  2
 10.    4504 [01:00.04]      301 [00:04.01]    no   no  2
TOTAL   4805 [01:04.05]
"""
        tracks = toc.parse_toc(output)
        self.assertEqual([(t.number, t.sectors) for t in tracks],
                         [(1, 150), (2, 151), (10, 4504)])
        self.assertEqual([t.duration for t in tracks], [2, 2, 60])

    def test_rounding_preserves_subsecond_precision(self):
        tracks = toc.parse_toc(
            "1. 37 [00:00.37] 0 [00:00.00]\n"
            "2. 38 [00:00.38] 37 [00:00.37]"
        )
        self.assertEqual([t.sectors for t in tracks], [37, 38])
        self.assertEqual([t.duration for t in tracks], [0, 1])

    def test_empty_output(self):
        self.assertEqual(toc.parse_toc("No audio tracks"), [])

    def test_inconsistent_length_rejected(self):
        with self.assertRaises(ValueError):
            toc.parse_toc("1. 151 [00:02.00] 0 [00:00.00]")

    def test_duplicate_track_rejected(self):
        row = "1. 150 [00:02.00] 0 [00:00.00]"
        with self.assertRaises(ValueError):
            toc.parse_toc(row + "\n" + row)


if __name__ == "__main__":
    unittest.main()
