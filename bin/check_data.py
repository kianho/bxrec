#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""

Description:
    ...

Usage:
    check_data.py -i CSV [-F NAME ...]

Options:
    -i CSV, --input CSV
    -F NAME

"""

import os
import sys
import re
import chardet
import pandas

from csv import QUOTE_MINIMAL
from StringIO import StringIO
from docopt import docopt


class CharDetReader:
    """A custom encoding-agnostic CSV reader.

    """

    def __init__(self, f, skip_first=True,
            field_names=None, **kwargs):
        """Constructor.

        """

        self.f = f
        self.field_names = field_names
        self.skip_first = skip_first
        self.kwargs = kwargs

        return

    def __iter__(self):
        """Return a generator over each parsed csv line. The character encodings
        and data types in each line are inferred.

        """

        skip = self.skip_first
        sep = self.kwargs.get("delimiter", ",")
        columns = self.field_names

        for encoding, line in recoder(self.f):
            if skip:
                skip = False
                continue

            # Let pandas infer the data types of each field.
            row = pandas.read_csv(StringIO(line), sep=sep,
                    names=columns, header=None)
            yield row.to_records(index=None)[0]

        return


def recoder(f):
    """Auto-detect the character set for each line in `f` and re-encode it as
    a utf-8 string.

    """

    for line in f:
        encoding = chardet.detect(line)["encoding"]
        yield encoding, line.decode(encoding).encode("utf-8")

    return


if __name__ == '__main__':
    opts = docopt(__doc__)

    with open(opts["--input"], "rb") as f:
        reader = CharDetReader(f, skip_first=True, field_names=opts["-F"],
                    delimiter=";", quoting=QUOTE_MINIMAL)

        for row in reader:
            print row
