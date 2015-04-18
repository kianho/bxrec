#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""

Description:
    ...

Usage:
    etl.py


"""

import os
import sys
import re
import sqlite3
import ftfy
import petl as etl

from csv import QUOTE_MINIMAL
from collections import OrderedDict
from unidecode import unidecode # transliterate unicode to ascii

# detect the char. encoding of a file:
#   $ uchardet <FILE>
ENCODING = "windows-1252"
DELIMITER = ";"

ERROR_VALUE = None
ISBN_PAT = re.compile(r"[0-9]+")


def translit(s):
    return unicode(unidecode(unicode(s.encode("utf-8"), "utf-8")))


def convert_isbn(s):
    if ISBN_PAT.match(s):
        return unicode(s)
    return ERROR_VALUE


def main():
    conn = sqlite3.connect("bx.db")

    tab = etl.fromcsv("BX-Users.csv", delimiter=DELIMITER,
            quoting=QUOTE_MINIMAL, encoding=ENCODING)

    tab = tab.rename({
        "User-ID" : "user_id",
        "Location" : "location",
        "Age" : "age"
        })

#   transforms = OrderedDict([
#       ("age", lambda r : (ERROR_VALUE if r["age"] == "NULL" else float(r["age"])))
#   ])
#   tab = tab.fieldmap(transforms, failonerror=True)

    tab = ( tab.convert("user_id", translit, errorvalue=ERROR_VALUE)
               .convert("location", translit, errorvalue=ERROR_VALUE)
               .convert("age", lambda x : float(x), errorvalue=ERROR_VALUE) )

    # Transform the books.
    tab = etl.fromcsv("BX-Books.csv", delimiter=DELIMITER,
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

    # write the `users` table to a sqlite db.
    tab.todb(conn, "users", create=True)

    conn.close()

    return

if __name__ == '__main__':
    main()
