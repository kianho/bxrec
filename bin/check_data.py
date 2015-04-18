#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""

Description:
    Verify the consistency of the contents of a CSV file according to the
    expected data types of each column.

Usage:
    check_data.py -i CSV -c COLS [options]

Options:
    -i, --input CSV         Path to the input csv file.
    -c, --cols COLS         Comma-separated list of <name>:<dtype> pairs for
                            each column.
    -d, --delim DELIM       CSV column delimiter [default: ,].
    -s, --skip-first        Skip the first line of the input csv file.

"""

import __builtin__
import os
import sys
import re
import traceback
import pandas

from StringIO import StringIO
from collections import OrderedDict
from docopt import docopt
from schema import Schema, Or, And, Use



def get_col_dtypes(s):
    """
    """

    pairs = map((lambda x : x.split(":")), s.split(","))
    return OrderedDict(((p[0], getattr(__builtin__, p[1])) for p in pairs))


if __name__ == '__main__':
    opts = docopt(__doc__)

    # Command-line options schema.
    opts = Schema(
            {
                "--input" : os.path.exists,
                "--cols" : Use(get_col_dtypes),
                "--delim" : Use(str),
                "--skip-first" : Use(bool)
            }).validate(opts)

    skip_first = opts["--skip-first"]
    bad_lines = 0
    with open(opts["--input"], "rb") as f:
        for row_i, line in enumerate(f):
            if skip_first:
                skip_first = False
                continue

            row_num = row_i + 1
    
            # Let pandas handle the character encoding/decoding for each line
            # separately.
            #
            # Display a stack trace for mis-interpreted lines to stderr.
            try:
                row = pandas.read_csv(StringIO(line), sep=opts["--delim"],
                        names=opts["--cols"].keys(), dtype=opts["--cols"],
                        header=None)
                log_msg = "OK" + os.linesep
            except Exception, e:
                exc_str = traceback.format_exc(sys.exc_info())
                log_msg = "BAD" + os.linesep + exec_str 
                bad_lines += 1

            log_msg = ("ROW %d -> " % row_num) + log_msg
            sys.stderr.write(log_msg)
