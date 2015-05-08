#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""This is the test suite for ``bxrec``.

"""

import sys
import unittest
import bxrec.bin


class TestETL(unittest.TestCase):
    """Test the etl.py script.

    """

    def setUp(self):
        pass

    def tearDown(self):
        pass

    def test_clean_text(self):
        from bxrec.bin.etl import clean_text

        # Check the parsing and translation of HTML entities.
        self.assertEquals(clean_test(" &amp;   ", "&"))

        return


if __name__ == '__main__':
    unittest.main()
