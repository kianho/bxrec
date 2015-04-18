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
import chardet
import pandas

from csv import QUOTE_MINIMAL
from StringIO import StringIO
from collections import OrderedDict
from docopt import docopt
from schema import Schema, Or, And, Use
from ftfy import fix_text, guess_bytes


class CharDetReader:
    """A custom encoding-agnostic CSV reader.

    """

    def __init__(self, f, cols, skip_first=True, **kwargs):
        """Constructor.

        """

        self.f = f
        self.cols = cols
        self.skip_first = skip_first
        self.kwargs = kwargs

        return

    def __iter__(self):
        """Return a generator over each parsed csv line. The character encodings
        and data types in each line are inferred.

        """

        skip_first = self.skip_first
        sep = self.kwargs.get("delimiter", ",")
        names, dtype = self.cols.keys(), self.cols

        for line in self.f:
            if skip_first:
                skip_first = False
                continue

            try:
                row = pandas.read_csv(StringIO(line), sep=sep,
                        names=names, dtype=dtype, header=None)
                yield row.to_records(index=None)[0], None
            except Exception, e:
                # Generate error message then continue checking other lines.
                # TODO: add pretty indents to make error messages more legible.
                exception_str = traceback.format_exc(sys.exc_info())
                yield None, exception_str


        return


def recoder(f):
    """Auto-detect the character set for each line in `f` and re-encode it as
    a utf-8 string.

    """

    for line in f:
        line = guess_bytes(line)
        encoding = chardet.detect(line)["encoding"]
        yield encoding, line.decode(encoding).encode("utf-8")

    return


def get_col_dtypes(s):
    """
    """

    pairs = map((lambda x : x.split(":")), s.split(","))
    return OrderedDict(((p[0], getattr(__builtin__, p[1])) for p in pairs))



if __name__ == '__main__':
    opts = docopt(__doc__)

    # command-line option schema
    opts = Schema(
            {
                "--input" : os.path.exists,
                "--cols" : Use(get_col_dtypes),
                "--delim" : Use(str),
                "--skip-first" : Use(bool)
            }).validate(opts)

    with open(opts["--input"], "rb") as f:
        reader = CharDetReader(f, skip_first=opts["--skip-first"],
                    cols=opts["--cols"],
                    delimiter=opts["--delim"])

        for row_i, (row, exception_str) in enumerate(reader):
            # Display the condition of each row.
            line_num = row_i + 1
            if row:
                log_msg = "ROW %d -> OK" % line_num + os.linesep
            else:
                log_msg = "ROW %d -> BAD" % line_num + os.linesep
                log_msg += exception_str
            sys.stderr.write(log_msg)
