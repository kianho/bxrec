#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""

Description:
    This script performs an ETL of the BX dataset into a sqlite via a series of
    intermediate data validation and cleaning operations.

Usage:
    etl.py etl <bx-users-csv> <bx-books-csv> <bx-book-ratings-csv> --db DB
    etl.py check <bx-users-csv> <bx-books-csv> <bx-book-ratings-csv>

Options:
    etl         Run ETL on the BX data and load into an sqlite db.
    check       Log the rows in the BX data that look mangled/unusual.
    --db DB     The sqlite db in which to load the BX data. 

"""

import os
import sys
import re
import json
import sqlite3
import petl as etl

from HTMLParser import HTMLParser
from csv import QUOTE_ALL
from docopt import docopt
from cerberus import Validator
from unicodedata import normalize

PWD = os.path.dirname(os.path.abspath(__file__))

# detect the char. encoding of a file:
#   $ uchardet <FILE>
ENCODING = "windows-1252"
DELIMITER = ";"
ESCAPECHAR = "\\"

ERROR_VALUE = None
NULL_VALUE = None

# Expected field patterns.
USER_ID_PAT = re.compile(r"^(\d+)$")
AGE_PAT = re.compile(r"^(\d+)$")
YEAR_PAT = re.compile(r"^(\d{4})$") # TODO: check for improbably old/new books.
URL_PAT = re.compile(r"^(https?:\/\/)?([\da-z\.-]+)\.([a-z\.]{2,6})([\/\w \.-]*)*\/?$")
WS_PAT = re.compile(r"\s+")

# Use the html parser from the `HTMLParser` library to unescape HTML
# entities.
HTML_PARSER = HTMLParser()



def validate_age(field, value, error):
    # Allow "NULL" string values, these will eventually represent `None`
    # values.
    if value != "NULL" and not AGE_PAT.match(value):
        error(field, "must be an integer")
    return


def validate_not_null_str(field, value, error):
    if value == "NULL":
        error(field, "must not be 'NULL'")


def validate_rating(field, value, error):
    try:
        rating = int(value)
    except ValueError:
        rating = -1

    if 0 <= rating <= 10:
        error(field, "must be 0 <= x <= 10")


def check_bx_users(fn):
    """Check the consistentcy of the BX-Users.csv file.

    """

    tab = etl.fromcsv(fn, delimiter=DELIMITER,
            quoting=QUOTE_ALL, encoding=ENCODING)

    v = Validator({
            "User-ID" : {
                "type" : "string",
                "regex" : USER_ID_PAT,
                "required" : True
            },
            "Location" : {
                "type" : "string",
                "required" : True
            },
            "Age" : {
                "type" : "string",
                "validator" : validate_age,
                "required" : True
            }
        })

    for row_num, r in enumerate(tab.dicts(), 1):
        is_valid = v.validate(r)
        if not is_valid:
            print "row %d -> %r, %r" % (row_num, v.errors, r)

    return


def check_bx_books(fn):
    """Check the consistency of the BX-Books.csv file.

    """

    tab = etl.fromcsv(fn, delimiter=DELIMITER,
            quoting=QUOTE_ALL, encoding=ENCODING)

    v = Validator({
            "ISBN" : {
                "type" : "string",
                "validator" : validate_not_null_str,
                "required" : True,
            },
            "Book-Title" : {
                "type" : "string",
                "validator" : validate_not_null_str,
                "required" : True
            },
            "Book-Author" : {
                "type" : "string",
                "validator" : validate_not_null_str,
                "required" : True
            },
            "Year-Of-Publication" : {
                "type" : "string",
                "regex" : YEAR_PAT,
                "required" : True
            },
            "Publisher" : {
                "type" : "string",
                "validator" : validate_not_null_str,
                "required" : True
            },
            "Image-URL-S" : {
                "type" : "string",
                "regex" : URL_PAT,
                "required" : True
            },
            "Image-URL-M" : {
                "type" : "string",
                "regex" : URL_PAT,
                "required" : True
            },
            "Image-URL-L" : {
                "type" : "string",
                "regex" : URL_PAT,
                "required" : True
            }
        })

    for row_num, r in enumerate(tab.dicts(), 1):
        is_valid = v.validate(r)
        if not is_valid:
            print "row %d -> %r, %r" % (row_num, v.errors, r)

    return


def check_bx_book_ratings(fn):
    """Check the consistency of the BX-Book-Ratings.csv file.

    """

    tab = etl.fromcsv(fn, delimiter=DELIMITER,
            quoting=QUOTE_ALL, encoding=ENCODING)

    v = Validator({
            "User-ID" : {
                "type" : "string",
                "required" : True
            },
            "ISBN" : {
                "type" : "string",
                "required" : True
            },
            "Book-Rating" : {
                "type" : "string",
                "validator" : validate_rating,
                "required" : True
            }
        })

    for row_num, r in enumerate(tab.dicts(), 1):
        is_valid = v.validate(r)
        if not is_valid:
            print "row %d -> %r, %r" % (row_num, v.errors, r)

    return


def clean_isbn(s):
    return WS_PAT.sub("", s)


def clean_text(s):
    """Generic string cleaning function for standard string columns (e.g. those
    which aren't intended to solely represent integers or floating-point
    numbers.

    The string cleaning operations include:

    1. stripping flanking whitespace.
    2. unescaping HTML entities.
    3. normalise unicode using "NFKD" form.

    Assume that any tranliteration to ASCII occurs at the time of querying. 

    Note: This function enforces utf-8 encoding.

    """

    s = normalize("NFKD", s)

    # ALTERNATIVE:
    # Transliterate to ASCII (128 code-points)
    #
    #   from unidecode import unidecode
    #   .
    #   .
    #   .
    #   try:
    #       s.decode("utf-8")
    #   except UnicodeEncodeError:
    #       s = unicode(unidecode(s))

    s = HTML_PARSER.unescape(s.strip())

    if s == "NULL" or not s:
        return NULL_VALUE

    return s


def clean_year(s):
    """
    """

    year = s.strip()

    try:
        year = int(year)
    except ValueError:
        return NULL_VALUE

    if year <= 0:
        return NULL_VALUE
    return year


def clean_rating(s):
    """
    """

    rating = s.strip()

    try:
        rating = int(rating)
    except ValueError:
        return NULL_VALUE

    return rating


def do_etl(bx_users_fn, bx_books_fn, bx_book_ratings_fn, db_fn):
    """Run the ETL procedures over all the BX csv files, populating a single
    sqlite .db file.

    Arguments:
        bx_users_fn -- path to the BX-Users.csv file.
        bx_books_fn -- path to the BX-Books.csv file.
        bx_book_ratings_fn -- path to the BX-Book_Ratings.csv file.
        db_fn -- path to the output sqlite .db file.

    Returns:
        None

    """

    errargs = { "errorvalue" : ERROR_VALUE }

    # Transform BX-Users.csv
    tab = etl.fromcsv(bx_users_fn, delimiter=DELIMITER, escapechar=ESCAPECHAR,
            quoting=QUOTE_ALL, encoding=ENCODING)

    tab = tab.rename({
        "User-ID" : "user_id",
        "Location" : "location",
        "Age" : "age"
        })

    users_tab = (
            tab.convert("user_id",
                    (lambda s : s.strip()), **errargs)
               .convert("location", clean_text, **errargs)
               .convert("age",
                    (lambda s : int(s)), **errargs) )

    # Transform BX-Books.csv
    tab = etl.fromcsv(bx_books_fn, delimiter=DELIMITER, escapechar=ESCAPECHAR,
                quoting=QUOTE_ALL, encoding=ENCODING)

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

    books_tab = (
            tab.convert("isbn", clean_isbn, **errargs)
               .convert("title", clean_text, **errargs)
               .convert("author", clean_text, **errargs)
               .convert("year", clean_year, **errargs)
               .convert("publisher", clean_text, **errargs)
               .convert("img_url_s", (lambda s : s.strip()), **errargs)
               .convert("img_url_m", (lambda s : s.strip()), **errargs)
               .convert("img_url_l", (lambda s : s.strip()), **errargs) )

    # Transform BX-Book-Ratings.csv
    tab = etl.fromcsv(bx_book_ratings_fn, delimiter=DELIMITER,
                escapechar=ESCAPECHAR, quoting=QUOTE_ALL, encoding=ENCODING)

    tab = tab.rename({
        "User-ID" : "user_id",
        "ISBN" : "isbn",
        "Book-Rating" : "rating"
        })

    ratings_tab = (
            tab.convert("user_id", (lambda s : s.strip()), **errargs)
               .convert("isbn", clean_isbn, **errargs)
               .convert("rating", clean_rating, **errargs) )

    with sqlite3.connect(db_fn) as conn:
        # Load each table into an sqlite db.
        users_tab.todb(conn, "users", create=True)
        books_tab.todb(conn, "books", create=True, drop=True)
        ratings_tab.todb(conn, "ratings", create=True)
        conn.commit()

    return


if __name__ == '__main__':
    opts = docopt(__doc__)

    if opts["etl"]:
        do_etl(opts["<bx-users-csv>"], opts["<bx-books-csv>"],
                opts["<bx-book-ratings-csv>"], opts["--db"])
    elif opts["check"]:
        print opts["<bx-books-csv>"]
        check_bx_books(opts["<bx-books-csv>"])

        print opts["<bx-users-csv>"]
        check_bx_users(opts["<bx-users-csv>"])

        print opts["<bx-book-ratings-csv>"]
        check_bx_book_ratings(opts["<bx-book-ratings-csv>"])
