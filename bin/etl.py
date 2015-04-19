#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""

Description:
    ...

Usage:
    etl.py check <bx-users-csv>

Options:
    check


"""

import os
import sys
import re
import sqlite3
import textwrap
import tabulate
import petl as etl

from csv import QUOTE_MINIMAL
from collections import OrderedDict, defaultdict
from docopt import docopt
from unidecode import unidecode # transliterate unicode to ascii

PWD = os.path.dirname(os.path.abspath(__file__))

# detect the char. encoding of a file:
#   $ uchardet <FILE>
ENCODING = "windows-1252"
DELIMITER = ";"

ERROR_VALUE = None
WS_PAT = re.compile(r"\s+")

# Expected field patterns.
ISBN_PAT = re.compile(r"^((\d{12}[\dA-Za-z])|(\d{9}[\dA-Za-z]))$")
USER_ID_PAT = re.compile(r"^(\d+)$")
AGE_PAT = re.compile(r"^(\d+)$")


def translit(s):
    return unicode(unidecode(unicode(s.encode("utf-8"), "utf-8")))


def convert_isbn(s):
    """

    ISBN values are 13 digits if assigned after Jan. 1, 2007, and 10 digits otherwise.
    - last digit is a "check digit", which can be a letter or number.

    """

    # Remove leading/trailing/inner whitespace.
    # Make the raw ISBN upper case.
    s = WS_PAT.sub("", s).upper()

    if ISBN_PAT.match(s):
        return unicode(s)

    return ERROR_VALUE


def check_bx_users(fn):
    """
    """

    tab = ( etl.fromcsv(fn, delimiter=DELIMITER,
                quoting=QUOTE_MINIMAL, encoding=ENCODING) )

    # Compute a summary of "bad" rows.
    summary = defaultdict(int)

    for row_num, r in enumerate(tab.records(), 1):
        # Check user-id format.
        if r["User-ID"] == "NULL":
            summary["User-ID, NULL"] += 1
        elif not USER_ID_PAT.match(r["User-ID"]):
            sys.stderr.write("WARNING: invalid `User-ID` format @ row %d"
                    % row_num + os.linesep)
            summary["User-ID, invalid"] += 1
        else:
            pass

        # Check location format:
        if r["Location"] == "NULL":
            summary["Location, NULL"] += 1

        # Check age format.
        if r["Age"] == "NULL":
            summary["(Age, NULL)"] += 1
        elif not AGE_PAT.match(r["Age"]):
            sys.stderr.write("WARNING: invalid `Age` format @ row %d"
                    % row_num + os.linesep)
            summary["(Age, invalid)"] += 1
        else:
            pass

    # Print a summary of the bad rows.
    print("SUMMARY for %s" % fn)
    print(tabulate.tabulate(summary.items()))

    return


def main():
    conn = sqlite3.connect("bx.db")

    #
    # Transform BX-Users.csv
    #

    tab = etl.fromcsv("data/csv/BX-Users.csv", delimiter=DELIMITER,
            quoting=QUOTE_MINIMAL, encoding=ENCODING)

    tab = tab.rename({
        "User-ID" : "user_id",
        "Location" : "location",
        "Age" : "age"
        })

    tab = ( tab.convert("user_id", translit, errorvalue=ERROR_VALUE)
               .convert("location", translit, errorvalue=ERROR_VALUE)
               .convert("age", lambda x : float(x), errorvalue=ERROR_VALUE) )

    #
    # Transform BX-Books.csv
    #
    tab = etl.fromcsv("data/csv/BX-Books.csv", delimiter=DELIMITER,
                quoting=QUOTE_MINIMAL, encoding=ENCODING)
    tab = tab.rename({
                    "ISBN" : "isbn",
                    "Book-Title" : "title",
                    "Book-Author" : "author",
                    "Year-Of-Publication" : "year",
                    "Publisher" : "publisher",
                    "Image-URL-S" : "img_url_s",
                    "Image-URL-M" : "img_url_m",
                    "Image-URL-L" : "img_url_l"
                    })
    tab = ( tab.convert("isbn", convert_isbn, errorvalue=ERROR_VALUE)
            )

    print tab.stringpatterns("isbn")

    sys.exit()

    #          .convert("isbn", translit, errorvalue=ERROR_VALUE)
    #          .convert("title", translit, errorvalue=ERROR_VALUE)
    #          .convert("author", lambda x : ERROR_VALUE if x == "NULL" translit, 


    print tab.look(50)

    sys.exit()

    #
    # Transform BX-Book-Ratings.csv
    #

    #
    # Load each table into an sqlite db.
    #
    tab.todb(conn, "users", create=True)

    conn.close()

    return

if __name__ == '__main__':
    opts = docopt(__doc__)

    if opts["check"]:
        check_bx_users(opts["<bx-users-csv>"])
