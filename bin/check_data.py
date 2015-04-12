#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""

Description:
    ...

Usage:
    check_data.py -f CSV

Options:
    -f CSV
    -o OUTPUT

"""

import os
import sys
import re
import codecs
import csvkit
import chardet

from csv import QUOTE_MINIMAL
from StringIO import StringIO
from docopt import docopt

#from pyspark import SparkConf, SparkContext
#from pyspark.sql import SQLContext, Row

FLOAT_RE = re.compile(r"[0-9]+\.[0-9]+")


class CharDetReader:
    def __init__(self, f, **kwargs):
        """Constructor.

        """

        self.f = f
        self.kwargs = kwargs

        return

    def __iter__(self):
        for encoding, line in recoder(self.f):
            yield encoding, csvkit.reader(StringIO(line), **self.kwargs).next()

        return


def recoder(f):
    """Auto-detect the character set for each line in `f` and re-encode it as
    a utf-8 string.

    """

    for line in f:
        encoding = chardet.detect(line)["encoding"]
        yield encoding, line.decode(encoding).encode("utf-8")

    return


def parse_csv_line(line, sep):
    """
    """
    f = StringIO(line)
    vals = csvkit.reader(f, delimiter=";", quoting=0).next()

    age_str = vals[2]

    # Parse the age if it was specified, ages are assumed to be integers.
    try:
        age = int(age_str[2])
    except ValueError:
        age = None

    return


#def load_data(sc, fn, output_fn):
#    """
#    """
#    rdd = sc.textFile(fn)
#
#    # Skip the header line (the first line).
#    header = rdd.first()
#    csv_lines = rdd.filter(lambda ln : ln != header)
#    csv_rows = csv_lines.map(lambda ln : parse_line(ln, sep=";"))
#    csv_rows.saveAsTextFile(output_fn)
#
#    return


if __name__ == '__main__':
    opts = docopt(__doc__)
    
#    # Initialise the spark context.
#    conf = SparkConf().setMaster("local").setAppName("etl")
#    sc = SparkContext(conf=conf)
#

    with open(opts["-f"], "rb") as f:
        reader = CharDetReader(f, delimiter=";", quoting=QUOTE_MINIMAL)
        for encoding, row in reader:
            print row

