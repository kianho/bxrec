#
# Author:
#   Kian Ho <hui.kian.ho@gmail.com>
#
# Description:
# 	This Makefile controls the data wrangling tasks associated with the BX
# 	dataset.
#
# Usage:
#
#   Make the primary BX dataset:
#
#   $ make data/bx.db
# 
export SHELL
SHELL:=/bin/bash

BASE_DIR=$(shell pwd)
BIN=$(BASE_DIR)/bin
DATA=$(BASE_DIR)/data
CSV=$(DATA)/csv
LOG=$(BASE_DIR)/log

CSV_ZIP=$(DATA)/BX-CSV-Dump.zip
CSV_FILES=$(addprefix $(DATA)/BX, -Book-Ratings.csv -Books.csv -Users.csv)

ETL_PY=$(BIN)/etl.py
ETL_DB=$(DATA)/bx.db


# ETL the csv files into a single sqlite db.
$(ETL_DB): $(DATA)/.ds
	time $(ETL_PY) etl \
	   	$(CSV)/BX-Users.csv $(CSV)/BX-Books.csv $(CSV)/BX-Book-Ratings.csv --db $@

# Inspect the csv files.
$(LOG)/BX.log: $(LOG)/.ds
	( time $(ETL_PY) check \
	   	$(CSV)/BX-Users.csv $(CSV)/BX-Books.csv $(CSV)/BX-Book-Ratings.csv ) > $@

$(LOG)/.ds: 
	mkdir -p $(@D) && touch $@

dataset: $(DATA)/csv/.ds

# Generate/unzip the raw csv files.
$(DATA)/csv/.ds: $(CSV_ZIP) $(DATA)/.ds
	mkdir -p $(@D) && touch $@
	unzip -d $(@D) $<

$(DATA)/.ds:
	touch $@
